package folders

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	datatransfer "cloud.google.com/go/bigquery/datatransfer/apiv1"
	"github.com/pmenglund/gcp-folders/fetcher"
	"github.com/pmenglund/gcp-folders/saver"
	"github.com/pmenglund/gcp-folders/tree"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	datatransferpb "google.golang.org/genproto/googleapis/cloud/bigquery/datatransfer/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Message is the message sent to the cloud function
type Message struct {
	Data []byte `json:"data"`
}

// Dump is a Cloud Function that walks the GCP folder structure and saves it
// to a BigQuery table which can be used to lookup folder id to folder name
// for use in DataStudio.
// It can be configured via environment variables
// ROOT
// MAX_DEPTH
// DATASET
// TABLE
// PROJECT
// PARENT
func Dump(ctx context.Context, msg Message) error {
	id := os.Getenv("ROOT")
	if id == "" {
		return errors.New("ROOT environment variable required")
	}
	log.Printf("ROOT is %s", id)

	md := os.Getenv("MAX_DEPTH")
	if md == "" {
		md = "4"
	}
	max, err := strconv.Atoi(md)
	if err != nil {
		return fmt.Errorf("failed to convert MAX_DEPTH %s to int: %v", md, err)
	}
	log.Printf("MAX_DEPTH is %d", max)

	dataset := os.Getenv("DATASET")
	if dataset == "" {
		return errors.New("DATASET environment variable required")
	}
	log.Printf("DATASET is %s", dataset)

	project := os.Getenv("PROJECT")
	if project == "" {
		return errors.New("PROJECT environment variable required")
	}
	log.Printf("PROJECT is %s", project)

	table := os.Getenv("TABLE")
	if table == "" {
		table = "folders"
	}
	log.Printf("TABLE is %s", table)

	conf := fetcher.Config{
		Verbose:  true,
		MaxDepth: max,
	}
	f, err := fetcher.New(ctx, conf)
	if err != nil {
		return err
	}

	root, err := f.Fetch(id)
	if err != nil {
		return err
	}

	folders := tree.Flatten(root)

	creds, err := google.FindDefaultCredentials(ctx, bigquery.Scope)
	if err != nil {
		return err
	}
	bq, err := bigquery.NewClient(ctx, project, option.WithCredentials(creds))
	if err != nil {
		return err
	}

	s := saver.New(ctx, bq, dataset, table)
	_, err = s.Save(folders)
	if err != nil {
		return err
	}

	parent := os.Getenv("PARENT")
	if parent == "" {
		return errors.New("PARENT environment variable required")
	}
	log.Printf("PARENT is %s", parent)

	err = StartManualTransfer(parent, ctx, creds)
	if err != nil {
		return err
	}
	return nil
}

func StartManualTransfer(parent string, ctx context.Context, creds *google.Credentials) error {
	// Creates a client.
	client, err := datatransfer.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	ts := timestamppb.New(time.Now())
	tmp := datatransferpb.StartManualTransferRunsRequest_RequestedRunTime{RequestedRunTime: ts}

	req := &datatransferpb.StartManualTransferRunsRequest{
		Parent: parent,
		Time:   &tmp,
	}
	resp, err := client.StartManualTransferRuns(ctx, req)
	if err != nil {
		return err
	}
	log.Printf(resp.String())

	return nil
}
