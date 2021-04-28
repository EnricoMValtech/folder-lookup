package requester

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/api/idtoken"
)

// makeGetRequest makes a request to the provided targetURL with an authenticated client.
func makeGetRequest(w io.Writer, targetURL string) error {
	// functionURL := "https://TARGET_URL"
	ctx := context.Background()

	// client is a http.Client that automatically adds an "Authorization" header
	// to any requests made.
	client, err := idtoken.NewClient(ctx, targetURL)
	if err != nil {
		return fmt.Errorf("idtoken.NewClient: %v", err)
	}

	resp, err := client.Get(targetURL)
	if err != nil {
		return fmt.Errorf("client.Get: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	return nil
}
