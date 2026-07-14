package objectstore

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrChecksumMismatch = errors.New("object checksum mismatch")

type Config struct {
	Region          string
	InternalURL     string
	PublicURL       string
	AccessKeyID     string
	SecretAccessKey string
	TLSConfig       *tls.Config
}

type CompletedPart struct {
	PartNumber     int32  `json:"part_number"`
	ETag           string `json:"etag"`
	ChecksumSHA256 string `json:"checksum_sha256"`
}

type AttemptPrefix struct {
	Prefix         string
	AttemptID      string
	LatestModified time.Time
}

type Client struct {
	internal *s3.Client
	presign  *s3.PresignClient
}

func New(ctx context.Context, config Config) (*Client, error) {
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	if config.InternalURL == "" || config.PublicURL == "" || config.AccessKeyID == "" || config.SecretAccessKey == "" {
		return nil, errors.New("S3 internal/public endpoints and credentials are required")
	}
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, "")),
	}
	if config.TLSConfig != nil {
		loadOptions = append(loadOptions, awsconfig.WithHTTPClient(&http.Client{
			Transport: &http.Transport{TLSClientConfig: config.TLSConfig.Clone(), ForceAttemptHTTP2: true},
			Timeout:   4 * time.Hour,
		}))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration: %w", err)
	}
	internal := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(strings.TrimRight(config.InternalURL, "/"))
		options.UsePathStyle = true
	})
	public := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(strings.TrimRight(config.PublicURL, "/"))
		options.UsePathStyle = true
	})
	return &Client{internal: internal, presign: s3.NewPresignClient(public)}, nil
}

func (c *Client) Ready(ctx context.Context, bucket string) error {
	_, err := c.internal.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	return err
}

func (c *Client) EnsureBucketLifecycle(ctx context.Context, bucket string, retentionDays int32) error {
	_, err := c.internal.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			return err
		}
	}
	_, err = c.internal.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
			ID: aws.String("platform-retention"), Status: types.ExpirationStatusEnabled,
			Filter:                         &types.LifecycleRuleFilter{Prefix: aws.String("")},
			Expiration:                     &types.LifecycleExpiration{Days: aws.Int32(retentionDays)},
			AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)},
		}}},
	})
	return err
}

func (c *Client) CreateMultipart(ctx context.Context, bucket, key string) (string, error) {
	out, err := c.internal.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return "", err
	}
	if out.UploadId == nil || *out.UploadId == "" {
		return "", errors.New("S3 gateway returned an empty multipart upload ID")
	}
	return *out.UploadId, nil
}

func (c *Client) PresignPart(ctx context.Context, bucket, key, uploadID string, partNumber int32, checksumBase64 string, expires time.Duration) (string, error) {
	if partNumber < 1 || partNumber > 10000 {
		return "", errors.New("part number outside S3 range")
	}
	request, err := c.presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		PartNumber: aws.Int32(partNumber), ChecksumSHA256: aws.String(checksumBase64),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return "", err
	}
	return request.URL, nil
}

func (c *Client) PresignDownload(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	request, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return "", err
	}
	return request.URL, nil
}

func (c *Client) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) error {
	if len(parts) == 0 || len(parts) > 10000 {
		return errors.New("invalid multipart part count")
	}
	completed := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		completed = append(completed, types.CompletedPart{
			PartNumber: aws.Int32(part.PartNumber), ETag: aws.String(part.ETag),
			ChecksumSHA256: aws.String(part.ChecksumSHA256),
		})
	}
	_, err := c.internal.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	return err
}

func (c *Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	_, err := c.internal.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	return err
}

func (c *Client) VerifyObject(ctx context.Context, bucket, key string, expectedBytes int64, expectedHex string) error {
	out, err := c.internal.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
	if err != nil {
		return err
	}
	if out.ContentLength == nil || *out.ContentLength != expectedBytes {
		return fmt.Errorf("%w: object size got %v, expected %d", ErrChecksumMismatch, out.ContentLength, expectedBytes)
	}
	expectedRaw, err := hex.DecodeString(expectedHex)
	if err != nil {
		return fmt.Errorf("decode expected checksum: %w", err)
	}
	response, err := c.internal.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(response.Body, expectedBytes+1))
	if err != nil || written != expectedBytes {
		if err != nil {
			return fmt.Errorf("whole-object checksum stream failed after %d of %d bytes: %w", written, expectedBytes, err)
		}
		return fmt.Errorf("%w: whole-object stream wrote %d of %d bytes", ErrChecksumMismatch, written, expectedBytes)
	}
	actualRaw := hash.Sum(nil)
	if len(expectedRaw) != len(actualRaw) || subtle.ConstantTimeCompare(expectedRaw, actualRaw) != 1 {
		return ErrChecksumMismatch
	}
	return nil
}

func (c *Client) DownloadFile(ctx context.Context, bucket, key, destination, expectedHex string, expectedBytes int64) error {
	response, err := c.internal.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	temporary := destination + ".partial"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, expectedBytes+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || written != expectedBytes {
		_ = os.Remove(temporary)
		return fmt.Errorf("download size or stream mismatch: wrote %d of %d bytes: %v %v", written, expectedBytes, copyErr, closeErr)
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(expectedHex)), []byte(actual)) != 1 {
		_ = os.Remove(temporary)
		return errors.New("downloaded object checksum mismatch")
	}
	return os.Rename(temporary, destination)
}

func (c *Client) UploadFile(ctx context.Context, bucket, key, source string) (int64, string, error) {
	file, err := os.Open(source)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return 0, "", err
	}
	digest := hash.Sum(nil)
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	uploader := transfermanager.New(c.internal, func(options *transfermanager.Options) {
		options.PartSizeBytes = 64 << 20
		options.MultipartUploadThreshold = 64 << 20
		options.Concurrency = 1
		options.MaxUploadParts = 10_000
		options.FailTimeout = 30 * time.Second
		options.ChecksumAlgorithm = transfertypes.ChecksumAlgorithmSha256
	})
	_, err = uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: file,
		ContentLength: aws.Int64(info.Size()), MpuObjectSize: aws.Int64(info.Size()),
		IfNoneMatch: aws.String("*"), ChecksumAlgorithm: transfertypes.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return 0, "", err
	}
	expected := hex.EncodeToString(digest)
	if err = c.VerifyObject(ctx, bucket, key, info.Size(), expected); err != nil {
		return 0, "", fmt.Errorf("verify uploaded artifact: %w", err)
	}
	return info.Size(), expected, nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.internal.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (c *Client) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(c.internal, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}
		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			if object.Key != nil {
				objects = append(objects, types.ObjectIdentifier{Key: object.Key})
			}
		}
		if len(objects) > 0 {
			result, deleteErr := c.internal.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
			})
			if deleteErr != nil {
				return deleteErr
			}
			if len(result.Errors) > 0 {
				return fmt.Errorf("S3 prefix deletion returned %d object errors", len(result.Errors))
			}
		}
	}
	return nil
}

func (c *Client) ListAttemptPrefixes(ctx context.Context, bucket string) ([]AttemptPrefix, error) {
	byPrefix := make(map[string]AttemptPrefix)
	paginator := s3.NewListObjectsV2Paginator(c.internal, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String("jobs/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			segments := strings.Split(*object.Key, "/")
			if len(segments) < 7 || segments[0] != "jobs" || segments[2] != "tasks" || segments[4] != "attempts" || segments[5] == "" {
				continue
			}
			prefix := strings.Join(segments[:6], "/") + "/"
			candidate := byPrefix[prefix]
			candidate.Prefix = prefix
			candidate.AttemptID = segments[5]
			if object.LastModified != nil && object.LastModified.After(candidate.LatestModified) {
				candidate.LatestModified = *object.LastModified
			}
			byPrefix[prefix] = candidate
		}
	}
	prefixes := make([]AttemptPrefix, 0, len(byPrefix))
	for _, prefix := range byPrefix {
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}
