package service

import (
	"fmt"
	"strings"
)

func buildPhysicalBucketName(lakeID, bucketID, name string) string {
	lakePart := strings.ToLower(strings.ReplaceAll(lakeID, "-", ""))
	if len(lakePart) > 8 {
		lakePart = lakePart[:8]
	}

	bucketPart := strings.ToLower(strings.ReplaceAll(bucketID, "-", ""))
	if len(bucketPart) > 8 {
		bucketPart = bucketPart[:8]
	}

	namePart := strings.ToLower(name)
	if len(namePart) > 20 {
		namePart = namePart[:20]
	}

	return fmt.Sprintf("dl-%s-%s-%s", lakePart, namePart, bucketPart)
}
