package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

var (
	blobClient    *azblob.Client
	containerName string
	accountName   string
	accountKey    string
)

// InitBlobService initializes the Azure Blob client using a connection string
func InitBlobService(connectionString string, cName string) error {
	var err error
	blobClient, err = azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return fmt.Errorf("failed to create blob client: %w", err)
	}

	containerName = cName

	// Manually parse connection string if azblob.ParseConnectionString is missing
	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "AccountName=") {
			accountName = strings.TrimPrefix(part, "AccountName=")
		} else if strings.HasPrefix(part, "AccountKey=") {
			accountKey = strings.TrimPrefix(part, "AccountKey=")
		}
	}

	if accountName == "" || accountKey == "" {
		return fmt.Errorf("failed to extract account name or key from connection string")
	}

	log.Printf("[storage] Azure Blob Service initialized. Container: %s", containerName)

	if err := SetCORS(); err != nil {
		log.Printf("[storage] Warning: Failed to set CORS rules: %v", err)
	} else {
		log.Printf("[storage] CORS rules updated successfully")
	}

	return nil
}

// SetCORS configures the storage account to allow downloads from the frontend
func SetCORS() error {
	if blobClient == nil {
		return fmt.Errorf("blob service not initialized")
	}

	// Helper to get pointers
	toPtr := func(s string) *string { return &s }

	corsRules := []*service.CORSRule{
		{
			AllowedOrigins:  toPtr("*"), // Allow all in dev, or specify "http://localhost:3000"
			AllowedMethods:  toPtr("GET,OPTIONS"),
			AllowedHeaders:  toPtr("*"),
			ExposedHeaders:  toPtr("*"),
			MaxAgeInSeconds: func(i int32) *int32 { return &i }(3600),
		},
	}

	_, err := blobClient.ServiceClient().SetProperties(context.TODO(), &service.SetPropertiesOptions{
		CORS: corsRules,
	})
	return err
}

// GenerateWriteSAS generates a URL with a write-only SAS for a specific blob name
func GenerateWriteSAS(blobName string) (string, error) {
	// Set the SAS permissions and expiry
	permissions := sas.BlobPermissions{
		Create: true,
		Write:  true,
		Add:    true,
	}
	expiry := time.Now().Add(2 * time.Hour)

	return generateSAS(blobName, permissions, expiry)
}

// GenerateReadSAS generates a URL with a read-only SAS token for a specific blob
func GenerateReadSAS(blobName string) (string, error) {

	permissions := sas.BlobPermissions{
		Read: true,
	}
	expiry := time.Now().Add(15 * time.Minute)

	return generateSAS(blobName, permissions, expiry)
}

func generateSAS(blobName string, permissions sas.BlobPermissions, expiry time.Time) (string, error) {
	if blobClient == nil {
		return "", fmt.Errorf("blob service not initialized")
	}

	// Create service SAS values
	sasValues := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-5 * time.Minute), // Buffer for clock skew
		ExpiryTime:    expiry.UTC(),
		Permissions:   permissions.String(),
		ContainerName: containerName,
		BlobName:      blobName,
	}

	// Sign the SAS values
	skc, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return "", fmt.Errorf("failed to create credentials: %w", err)
	}

	queryParams, err := sasValues.SignWithSharedKey(skc)
	if err != nil {
		return "", fmt.Errorf("failed to sign SAS: %w", err)
	}

	// Construct the full URL
	sasURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s",
		accountName, containerName, blobName, queryParams.Encode())

	return sasURL, nil
}

func DownloadBlob(ctx context.Context, blobURL string) ([]byte, error) {
	// ROBUST FALLBACK: Use standard HTTP for SAS URLs.
	// This avoids "v1 vs v2" SDK version conflicts entirely.
	req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed with status: %s", resp.Status)
	}

	// Read the entire stream into memory
	return io.ReadAll(resp.Body)
}

// UploadBlob uploads data to a blob and returns a secure read-only SAS URL
func UploadBlob(ctx context.Context, blobName string, data []byte) (string, error) {
	if blobClient == nil {
		return "", fmt.Errorf("blob service not initialized")
	}

	_, err := blobClient.UploadBuffer(ctx, containerName, blobName, data, nil)
	if err != nil {
		return "", fmt.Errorf("failed to upload blob %s: %w", blobName, err)
	}

	log.Printf("[storage] Uploaded blob: %s (%d bytes)", blobName, len(data))

	// Return a 24-hour read-only SAS URL
	permissions := sas.BlobPermissions{Read: true}
	expiry := time.Now().Add(24 * time.Hour)
	return generateSAS(blobName, permissions, expiry)
}
