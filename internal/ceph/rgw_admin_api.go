package ceph

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	rgwadmin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/movincloud/datalake-provisioner/internal/observability"
)

type RGWAdminAPIAdapter struct {
	Endpoint           string
	AdminPath          string
	Region             string
	InsecureSkipVerify bool

	httpClient  *http.Client
	adminClient *rgwadmin.API
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
	adminPath = strings.TrimRight(adminPath, "/")
	if adminPath != "/admin" {
		return nil, fmt.Errorf("rgw admin path %q unsupported: go-ceph/rgw/admin expects /admin", adminPath)
	}
	if region == "" {
		region = "us-east-1"
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify} //nolint:gosec
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: transport}

	adminClient, err := rgwadmin.New(strings.TrimRight(endpoint, "/"), accessKeyID, secretAccessKey, httpClient)
	if err != nil {
		return nil, fmt.Errorf("init rgw admin client: %w", err)
	}

	return &RGWAdminAPIAdapter{
		Endpoint:           strings.TrimRight(endpoint, "/"),
		AdminPath:          adminPath,
		Region:             region,
		InsecureSkipVerify: insecureSkipVerify,
		httpClient:         httpClient,
		adminClient:        adminClient,
	}, nil
}

func (a *RGWAdminAPIAdapter) CheckReady(ctx context.Context) (err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("check_ready", time.Since(startedAt), err)
	}()

	if _, err = a.adminClient.GetUsers(ctx); err != nil {
		return fmt.Errorf("rgw admin readiness check failed: %w", err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) EnsureLake(ctx context.Context, lakeID string) (lakeAccess LakeAccess, err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("ensure_lake", time.Since(startedAt), err)
	}()

	user, err := a.ensureLakeUserWithKey(ctx, lakeID)
	if err != nil {
		return LakeAccess{}, err
	}
	return LakeAccess{RGWUser: user.ID}, nil
}

func (a *RGWAdminAPIAdapter) SetLakeQuota(ctx context.Context, lakeID string, sizeGiB int64) (err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("set_lake_quota", time.Since(startedAt), err)
	}()

	if sizeGiB <= 0 {
		return fmt.Errorf("invalid sizeGiB %d", sizeGiB)
	}

	maxSizeKB := int(sizeGiB * 1024 * 1024)
	enabled := true
	uid := buildUID(lakeID)
	err = a.adminClient.SetUserQuota(ctx, rgwadmin.QuotaSpec{
		UID:       uid,
		Enabled:   &enabled,
		MaxSizeKb: &maxSizeKB,
	})
	if err != nil {
		return fmt.Errorf("set user quota %s: %w", uid, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) DeleteLake(ctx context.Context, lakeID string) (err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("delete_lake", time.Since(startedAt), err)
	}()

	uid := buildUID(lakeID)

	buckets, err := a.adminClient.ListUsersBuckets(ctx, uid)
	if err != nil {
		if errors.Is(err, rgwadmin.ErrNoSuchUser) {
			return nil
		}
		return fmt.Errorf("list rgw buckets for user %s: %w", uid, err)
	}
	if len(buckets) > 0 {
		return fmt.Errorf("cannot delete lake %s: rgw user %s still owns %d bucket(s)", lakeID, uid, len(buckets))
	}

	err = a.adminClient.RemoveUser(ctx, rgwadmin.User{ID: uid})
	if err != nil {
		if errors.Is(err, rgwadmin.ErrNoSuchUser) {
			return nil
		}
		return fmt.Errorf("delete rgw user %s: %w", uid, err)
	}
	return nil
}

func (a *RGWAdminAPIAdapter) CreateBucket(ctx context.Context, lakeID, bucketName string) (err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("create_bucket", time.Since(startedAt), err)
	}()

	user, err := a.getLakeUserWithKey(ctx, lakeID)
	if err != nil {
		return err
	}

	s3Client, err := a.newS3Client(ctx, user.Keys[0].AccessKey, user.Keys[0].SecretKey)
	if err != nil {
		return fmt.Errorf("init s3 client for user %s: %w", user.ID, err)
	}
	return a.ensureBucket(ctx, s3Client, bucketName)
}

func (a *RGWAdminAPIAdapter) DeleteBucketIfEmpty(ctx context.Context, lakeID, bucketName string) (err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("delete_bucket_if_empty", time.Since(startedAt), err)
	}()

	user, err := a.getLakeUserWithKey(ctx, lakeID)
	if err != nil {
		return err
	}

	s3Client, err := a.newS3Client(ctx, user.Keys[0].AccessKey, user.Keys[0].SecretKey)
	if err != nil {
		return fmt.Errorf("init s3 client for user %s: %w", user.ID, err)
	}
	return a.deleteBucketIfEmpty(ctx, s3Client, bucketName)
}

func (a *RGWAdminAPIAdapter) GetLakeUsage(ctx context.Context, lakeID string) (lakeUsage LakeUsage, err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("get_lake_usage", time.Since(startedAt), err)
	}()

	user, err := a.getLakeUser(ctx, lakeID, true)
	if err != nil {
		return LakeUsage{}, err
	}

	return LakeUsage{
		UsedBytes:   uint64PtrToInt64(user.Stat.Size),
		ObjectCount: uint64PtrToInt64(user.Stat.NumObjects),
	}, nil
}

func (a *RGWAdminAPIAdapter) GetBucketUsage(ctx context.Context, bucketName string) (bucketUsage BucketUsage, err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("get_bucket_usage", time.Since(startedAt), err)
	}()

	bucket, err := a.adminClient.GetBucketInfo(ctx, rgwadmin.Bucket{Bucket: bucketName})
	if err != nil {
		return BucketUsage{}, fmt.Errorf("get bucket info %s: %w", bucketName, err)
	}

	return BucketUsage{
		UsedBytes:   uint64PtrToInt64(bucket.Usage.RgwMain.Size),
		ObjectCount: uint64PtrToInt64(bucket.Usage.RgwMain.NumObjects),
	}, nil
}

func (a *RGWAdminAPIAdapter) ListLakeBucketUsage(ctx context.Context, lakeID string) (usageByBucket map[string]BucketUsage, err error) {
	startedAt := time.Now()
	defer func() {
		observability.ObserveCephRequest("list_lake_bucket_usage", time.Since(startedAt), err)
	}()

	uid := buildUID(lakeID)
	buckets, err := a.adminClient.ListUsersBucketsWithStat(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("list buckets with stats for user %s: %w", uid, err)
	}

	usageByBucket = make(map[string]BucketUsage, len(buckets))
	for _, bucket := range buckets {
		usageByBucket[bucket.Bucket] = BucketUsage{
			UsedBytes:   uint64PtrToInt64(bucket.Usage.RgwMain.Size),
			ObjectCount: uint64PtrToInt64(bucket.Usage.RgwMain.NumObjects),
		}
	}
	return usageByBucket, nil
}

func (a *RGWAdminAPIAdapter) ensureLakeUserWithKey(ctx context.Context, lakeID string) (rgwadmin.User, error) {
	user, err := a.getLakeUser(ctx, lakeID, false)
	if err != nil {
		if !errors.Is(err, rgwadmin.ErrNoSuchUser) {
			return rgwadmin.User{}, err
		}

		generateKey := true
		user, err = a.adminClient.CreateUser(ctx, rgwadmin.User{
			ID:          buildUID(lakeID),
			DisplayName: fmt.Sprintf("lake-%s", lakeID),
			GenerateKey: &generateKey,
		})
		if err != nil {
			if !errors.Is(err, rgwadmin.ErrUserExists) {
				return rgwadmin.User{}, fmt.Errorf("create rgw user %s: %w", buildUID(lakeID), err)
			}
			user, err = a.getLakeUser(ctx, lakeID, false)
			if err != nil {
				return rgwadmin.User{}, err
			}
		}
	}

	return a.ensureUserHasKey(ctx, user)
}

func (a *RGWAdminAPIAdapter) getLakeUserWithKey(ctx context.Context, lakeID string) (rgwadmin.User, error) {
	user, err := a.getLakeUser(ctx, lakeID, false)
	if err != nil {
		return rgwadmin.User{}, err
	}
	return a.ensureUserHasKey(ctx, user)
}

func (a *RGWAdminAPIAdapter) getLakeUser(ctx context.Context, lakeID string, withStats bool) (rgwadmin.User, error) {
	request := rgwadmin.User{ID: buildUID(lakeID)}
	if withStats {
		generateStat := true
		request.GenerateStat = &generateStat
	}

	user, err := a.adminClient.GetUser(ctx, request)
	if err != nil {
		return rgwadmin.User{}, fmt.Errorf("get rgw user %s: %w", buildUID(lakeID), err)
	}
	return user, nil
}

func (a *RGWAdminAPIAdapter) ensureUserHasKey(ctx context.Context, user rgwadmin.User) (rgwadmin.User, error) {
	if len(user.Keys) > 0 {
		return user, nil
	}

	generateKey := true
	_, err := a.adminClient.CreateKey(ctx, rgwadmin.UserKeySpec{
		UID:         user.ID,
		KeyType:     "s3",
		GenerateKey: &generateKey,
	})
	if err != nil {
		return rgwadmin.User{}, fmt.Errorf("create s3 key for user %s: %w", user.ID, err)
	}

	refreshedUser, err := a.adminClient.GetUser(ctx, rgwadmin.User{ID: user.ID})
	if err != nil {
		return rgwadmin.User{}, fmt.Errorf("get rgw user %s after key creation: %w", user.ID, err)
	}
	if len(refreshedUser.Keys) == 0 {
		return rgwadmin.User{}, fmt.Errorf("no s3 key available for user %s", user.ID)
	}
	return refreshedUser, nil
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

func buildUID(lakeID string) string {
	id := strings.ToLower(strings.ReplaceAll(lakeID, "-", ""))
	if len(id) > 20 {
		id = id[:20]
	}
	return "lake-" + id
}

func uint64PtrToInt64(v *uint64) int64 {
	if v == nil {
		return 0
	}
	return int64(*v)
}

var _ Adapter = (*RGWAdminAPIAdapter)(nil)
