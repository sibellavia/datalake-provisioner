package ceph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type RGWAdminAPIAdapter struct {
	Endpoint           string
	AdminPath          string
	Region             string
	AccessKeyID        string
	SecretAccessKey    string
	InsecureSkipVerify bool

	httpClient *http.Client
	signer     *v4.Signer
	creds      aws.Credentials
}

type rgwUserResponse struct {
	UserID string `json:"user_id"`
	Keys   []struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	} `json:"keys"`
}

type adminAPIError struct {
	StatusCode int
	Body       string
}

func (e *adminAPIError) Error() string {
	return fmt.Sprintf("rgw admin api error: status=%d body=%s", e.StatusCode, e.Body)
}

func NewRGWAdminAPIAdapter(endpoint, adminPath, region, accessKeyID, secretAccessKey string, insecureSkipVerify bool) (*RGWAdminAPIAdapter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("rgw endpoint is required")
	}
	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("rgw admin credentials are required")
	}
	if adminPath == "" {
		adminPath = "/admin"
	}
	if !strings.HasPrefix(adminPath, "/") {
		adminPath = "/" + adminPath
	}
	if region == "" {
		region = "us-east-1"
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify} //nolint:gosec
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: transport}

	staticCreds := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")
	creds, err := staticCreds.Retrieve(context.Background())
	if err != nil {
		return nil, fmt.Errorf("retrieve static credentials: %w", err)
	}

	adapter := &RGWAdminAPIAdapter{
		Endpoint:           strings.TrimRight(endpoint, "/"),
		AdminPath:          strings.TrimRight(adminPath, "/"),
		Region:             region,
		AccessKeyID:        accessKeyID,
		SecretAccessKey:    secretAccessKey,
		InsecureSkipVerify: insecureSkipVerify,
		httpClient:         httpClient,
		signer:             v4.NewSigner(),
		creds:              creds,
	}

	return adapter, nil
}

func (a *RGWAdminAPIAdapter) Provision(ctx context.Context, in ProvisionInput) (ProvisionOutput, error) {
	uid := buildUID(in.LakeID)
	displayName := fmt.Sprintf("lake-%s", in.LakeID)

	user, err := a.getOrCreateUser(ctx, uid, displayName)
	if err != nil {
		return ProvisionOutput{}, err
	}

	if len(user.Keys) == 0 {
		if err := a.createS3Key(ctx, uid); err != nil {
			return ProvisionOutput{}, err
		}
		user, err = a.getUser(ctx, uid)
		if err != nil {
			return ProvisionOutput{}, err
		}
	}
	if len(user.Keys) == 0 {
		return ProvisionOutput{}, fmt.Errorf("no s3 key available for user %s", uid)
	}

	if err := a.setUserQuota(ctx, uid, in.SizeGiB); err != nil {
		return ProvisionOutput{}, err
	}

	return ProvisionOutput{RGWUser: uid}, nil
}

func (a *RGWAdminAPIAdapter) Resize(ctx context.Context, lakeID string, sizeGiB int64) error {
	uid := buildUID(lakeID)
	return a.setUserQuota(ctx, uid, sizeGiB)
}

func (a *RGWAdminAPIAdapter) Deprovision(ctx context.Context, lakeID string) error {
	uid := buildUID(lakeID)

	params := url.Values{}
	params.Set("uid", uid)
	params.Set("purge-data", "true")
	params.Set("purge-keys", "true")
	_, err := a.adminRequest(ctx, http.MethodDelete, "/user", params, nil)
	if err != nil {
		if isAdminAPINotFound(err) {
			return nil
		}
		return fmt.Errorf("delete rgw user %s: %w", uid, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) getOrCreateUser(ctx context.Context, uid, displayName string) (rgwUserResponse, error) {
	user, err := a.getUser(ctx, uid)
	if err == nil {
		return user, nil
	}
	if !isAdminAPINotFound(err) {
		return rgwUserResponse{}, err
	}

	params := url.Values{}
	params.Set("uid", uid)
	params.Set("display-name", displayName)

	body, err := a.adminRequest(ctx, http.MethodPut, "/user", params, nil)
	if err != nil {
		return rgwUserResponse{}, fmt.Errorf("create rgw user %s: %w", uid, err)
	}

	var out rgwUserResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return rgwUserResponse{}, fmt.Errorf("parse create user response: %w", err)
	}
	return out, nil
}

func (a *RGWAdminAPIAdapter) getUser(ctx context.Context, uid string) (rgwUserResponse, error) {
	params := url.Values{}
	params.Set("uid", uid)
	body, err := a.adminRequest(ctx, http.MethodGet, "/user", params, nil)
	if err != nil {
		return rgwUserResponse{}, err
	}
	var out rgwUserResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return rgwUserResponse{}, fmt.Errorf("parse get user response: %w", err)
	}
	return out, nil
}

func (a *RGWAdminAPIAdapter) createS3Key(ctx context.Context, uid string) error {
	params := url.Values{}
	params.Set("uid", uid)
	params.Set("key", "true")
	params.Set("key-type", "s3")
	params.Set("generate-key", "true")
	_, err := a.adminRequest(ctx, http.MethodPut, "/user", params, nil)
	if err != nil {
		return fmt.Errorf("create s3 key for user %s: %w", uid, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) ensureBucket(ctx context.Context, s3Client *s3.Client, bucketName string) error {
	_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName})
	if err == nil {
		return nil
	}

	_, createErr := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketName})
	if createErr != nil {
		msg := strings.ToLower(createErr.Error())
		if strings.Contains(msg, "bucketalreadyownedbyyou") || strings.Contains(msg, "bucket already exists") {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", bucketName, createErr)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) deleteBucketIfEmpty(ctx context.Context, s3Client *s3.Client, bucketName string) error {
	objects, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucketName, MaxKeys: aws.Int32(1)})
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "nosuchbucket") {
			return nil
		}
		return fmt.Errorf("list bucket %s: %w", bucketName, err)
	}
	if objects.KeyCount != nil && *objects.KeyCount > 0 {
		return fmt.Errorf("bucket %s is not empty", bucketName)
	}

	_, err = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucketName})
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "nosuchbucket") {
			return nil
		}
		return fmt.Errorf("delete bucket %s: %w", bucketName, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) newS3Client(ctx context.Context, accessKeyID, secretAccessKey string) (*s3.Client, error) {
	staticCreds := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(a.Region),
		config.WithCredentialsProvider(staticCreds),
		config.WithHTTPClient(a.httpClient),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, _ string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: a.Endpoint, SigningRegion: a.Region, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}

func isAdminAPINotFound(err error) bool {
	apiErr, ok := err.(*adminAPIError)
	return ok && apiErr.StatusCode == http.StatusNotFound
}

func (a *RGWAdminAPIAdapter) setUserQuota(ctx context.Context, uid string, sizeGiB int64) error {
	if sizeGiB <= 0 {
		return fmt.Errorf("invalid sizeGiB %d", sizeGiB)
	}
	maxSizeKB := sizeGiB * 1024 * 1024

	params := url.Values{}
	params.Set("uid", uid)
	params.Set("quota", "true")
	params.Set("quota-type", "user")
	params.Set("enabled", "true")
	params.Set("max-size-kb", fmt.Sprintf("%d", maxSizeKB))

	_, err := a.adminRequest(ctx, http.MethodPut, "/user", params, nil)
	if err != nil {
		return fmt.Errorf("set user quota %s: %w", uid, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) adminRequest(ctx context.Context, method, resourcePath string, params url.Values, body []byte) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}

	u, err := url.Parse(a.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid rgw endpoint: %w", err)
	}
	u.Path = a.AdminPath + resourcePath
	u.RawQuery = params.Encode()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	payloadHash := hashPayload(body)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if err := a.signer.SignHTTP(ctx, a.creds, req, payloadHash, "s3", a.Region, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("admin request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, &adminAPIError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	return respBody, nil
}

func hashPayload(body []byte) string {
	h := sha256.New()
	if len(body) > 0 {
		h.Write(body)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func buildUID(lakeID string) string {
	id := strings.ToLower(strings.ReplaceAll(lakeID, "-", ""))
	if len(id) > 20 {
		id = id[:20]
	}
	return "lake-" + id
}

var _ Adapter = (*RGWAdminAPIAdapter)(nil)
