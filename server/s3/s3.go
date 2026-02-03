package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	client   *s3.Client
	presign  *s3.PresignClient
	bucket   string
	prefix   string
	endpoint string
}

type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	Prefix          string
	Region          string
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	// Parse endpoint for region
	region := cfg.Region
	if region == "" {
		region = "auto" // R2 default
	}

	// Build AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})

	return &Client{
		client:   client,
		presign:  s3.NewPresignClient(client),
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		endpoint: cfg.Endpoint,
	}, nil
}

func (c *Client) fullKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + "/" + key
}

// PresignedUploadURL generates a presigned URL for uploading a file
func (c *Client) PresignedUploadURL(ctx context.Context, key string, contentType string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(c.fullKey(key)),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("failed to presign upload URL: %w", err)
	}
	return req.URL, nil
}

// PresignedDownloadURL generates a presigned URL for downloading a file
func (c *Client) PresignedDownloadURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("failed to presign download URL: %w", err)
	}
	return req.URL, nil
}

// PresignedDownloadURLWithFilename generates a presigned URL with Content-Disposition header
func (c *Client) PresignedDownloadURLWithFilename(ctx context.Context, key string, filename string, expiry time.Duration) (string, error) {
	contentDisposition := fmt.Sprintf("attachment; filename=\"%s\"", url.PathEscape(filename))
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(c.bucket),
		Key:                        aws.String(c.fullKey(key)),
		ResponseContentDisposition: aws.String(contentDisposition),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("failed to presign download URL: %w", err)
	}
	return req.URL, nil
}

// Upload uploads data directly to S3
func (c *Client) Upload(ctx context.Context, key string, data io.Reader, contentType string, size int64) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(c.fullKey(key)),
		Body:          data,
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}
	return nil
}

// Download downloads data from S3
func (c *Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download from S3: %w", err)
	}
	return resp.Body, nil
}

// Delete deletes a file from S3
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}
	return nil
}

// HeadObject checks if an object exists and returns its metadata
func (c *Client) HeadObject(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		return nil, err
	}
	return &ObjectInfo{
		Size:         aws.ToInt64(resp.ContentLength),
		ContentType:  aws.ToString(resp.ContentType),
		ETag:         aws.ToString(resp.ETag),
		LastModified: aws.ToTime(resp.LastModified),
	}, nil
}

type ObjectInfo struct {
	Size         int64
	ContentType  string
	ETag         string
	LastModified time.Time
}

// List lists objects with a given prefix
func (c *Client) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(c.fullKey(prefix)),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, ObjectInfo{
				Size:         aws.ToInt64(obj.Size),
				ETag:         aws.ToString(obj.ETag),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return objects, nil
}

// GetBucket returns the bucket name
func (c *Client) GetBucket() string {
	return c.bucket
}

// GetPrefix returns the prefix
func (c *Client) GetPrefix() string {
	return c.prefix
}
