package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/mytecor/llm-gateway/driverproto/batch"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type vertexBatchClient struct {
	client *http.Client
	parent string
	api    string
}

type vertexJob struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Output struct {
		Directory string `json:"gcsOutputDirectory"`
	} `json:"outputInfo"`
	Counts struct {
		Successful string `json:"successfulCount"`
		Failed     string `json:"failedCount"`
	} `json:"completionStats"`
}

func (j vertexJob) result() batch.Result {
	return batch.Result{Job: j.Name, State: j.State, Successful: j.Counts.Successful, Failed: j.Counts.Failed}
}

func vertexBatchError(status int, code string) *CallError {
	return &CallError{Class: "invalid_response", Status: status, Code: code}
}

func (e *Executor) vertexBatchClient(ctx context.Context, provider ProviderConfig) (*vertexBatchClient, error) {
	if cached, ok := e.batchClients.Load(provider.ID); ok {
		return cached.(*vertexBatchClient), nil
	}
	// The token source outlives individual HTTP requests.
	ctx = context.WithoutCancel(ctx)
	var tokens oauth2.TokenSource
	if secret := strings.TrimSpace(provider.VertexAuthCredentials); secret != "" {
		data := []byte(secret)
		if !strings.HasPrefix(secret, "{") {
			var err error
			data, err = os.ReadFile(secret)
			if err != nil {
				return nil, err
			}
		}
		credentials, err := google.CredentialsFromJSONWithParams(ctx, data, google.CredentialsParams{
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
		})
		if err != nil {
			return nil, err
		}
		tokens = credentials.TokenSource
	} else if provider.APIKey != "" {
		tokens = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: provider.APIKey})
	} else {
		return nil, errors.New("missing Vertex credentials")
	}
	host := "aiplatform.googleapis.com"
	if provider.VertexRegion != "global" {
		host = provider.VertexRegion + "-" + host
	}
	client := oauth2.NewClient(ctx, tokens)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	result := &vertexBatchClient{client: client, api: "https://" + host + "/v1/",
		parent: "projects/" + url.PathEscape(provider.VertexProjectID) + "/locations/" + url.PathEscape(provider.VertexRegion)}
	cached, _ := e.batchClients.LoadOrStore(provider.ID, result)
	return cached.(*vertexBatchClient), nil
}

func (e *Executor) doBatch(ctx context.Context, providerID, nativeModel string, body []byte) ([]byte, *CallError) {
	provider := e.providers[providerID]
	var command batch.Command
	if !supportsProviderBatch(provider) || json.Unmarshal(body, &command) != nil || command.Prefix == "" {
		return nil, vertexBatchError(400, "invalid_batch_operation")
	}
	var result batch.Result
	var callErr *CallError
	switch {
	case command.Operation == batch.Prepare:
		callErr = prepareOpenRouterBatch(ctx, provider, command)
	case isOpenRouter(provider):
		result, callErr = runOpenRouterBatch(ctx, provider, command)
	default:
		if command.Bucket == "" {
			return nil, vertexBatchError(400, "batch_bucket_missing")
		}
		client, err := e.vertexBatchClient(ctx, provider)
		if err != nil {
			return nil, vertexBatchError(502, "batch_credentials_unavailable")
		}
		result, callErr = client.run(ctx, nativeModel, command)
	}
	if callErr != nil {
		return nil, callErr
	}
	data, _ := json.Marshal(result)
	return data, nil
}

func (c *vertexBatchClient) run(ctx context.Context, native string, cmd batch.Command) (batch.Result, *CallError) {
	jobs := c.api + c.parent + "/batchPredictionJobs"
	prefix := "gs://" + cmd.Bucket + "/" + cmd.Prefix
	switch cmd.Operation {
	case batch.Upload:
		return batch.Result{}, c.upload(ctx, cmd)
	case batch.Submit:
		payload, _ := json.Marshal(map[string]any{
			"displayName": cmd.Name, "model": "publishers/google/models/" + native,
			"inputConfig": map[string]any{
				"instancesFormat": "jsonl", "gcsSource": map[string][]string{"uris": {prefix + "/requests.jsonl"}},
			},
			"outputConfig": map[string]any{
				"predictionsFormat": "jsonl", "gcsDestination": map[string]string{"outputUriPrefix": prefix + "/output/"},
			},
		})
		var job vertexJob
		err := c.json(ctx, http.MethodPost, jobs, payload, &job)
		if err == nil && job.Name == "" {
			err = vertexBatchError(502, "batch_job_name_missing")
		}
		return job.result(), err
	case batch.Find:
		var listed struct {
			Jobs []vertexJob `json:"batchPredictionJobs"`
		}
		query := url.Values{"filter": {`display_name="` + cmd.Name + `"`}, "pageSize": {"2"}}
		if err := c.json(ctx, http.MethodGet, jobs+"?"+query.Encode(), nil, &listed); err != nil {
			return batch.Result{}, err
		}
		if len(listed.Jobs) > 1 {
			return batch.Result{}, vertexBatchError(409, "batch_submission_ambiguous")
		}
		if len(listed.Jobs) == 1 {
			return listed.Jobs[0].result(), nil
		}
		return batch.Result{}, nil
	case batch.Status, batch.Download:
		// Vertex can return a project number even if the request used a project ID.
		// Poll through the configured project and location using only the returned numeric ID.
		_, id, ok := strings.Cut(cmd.Job, "/batchPredictionJobs/")
		if !ok || id == "" || strings.Trim(id, "0123456789") != "" {
			return batch.Result{}, vertexBatchError(400, "invalid_batch_job")
		}
		var job vertexJob
		if err := c.json(ctx, http.MethodGet, jobs+"/"+id, nil, &job); err != nil {
			return batch.Result{}, err
		}
		if cmd.Operation == batch.Download {
			if err := c.download(ctx, cmd, job, prefix+"/output/"); err != nil {
				return batch.Result{}, err
			}
		}
		return job.result(), nil
	default:
		return batch.Result{}, vertexBatchError(400, "invalid_batch_operation")
	}
}

func (c *vertexBatchClient) request(ctx context.Context, method, endpoint, mime string, body io.Reader, size int64) (*http.Response, *CallError) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, vertexBatchError(502, "batch_upstream_request_failed")
	}
	if body != nil {
		req.Header.Set("Content-Type", mime)
		req.ContentLength = size
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, vertexBatchError(502, "batch_upstream_unavailable")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, vertexBatchError(resp.StatusCode, "batch_upstream_rejected")
	}
	return resp, nil
}

func (c *vertexBatchClient) json(ctx context.Context, method, endpoint string, body []byte, dst any) *CallError {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	resp, err := c.request(ctx, method, endpoint, "application/json", reader, int64(len(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst) != nil {
		return vertexBatchError(502, "invalid_batch_upstream_response")
	}
	return nil
}

const batchStorageAPI = "https://storage.googleapis.com/storage/v1/b/"

func (c *vertexBatchClient) upload(ctx context.Context, cmd batch.Command) *CallError {
	f, err := os.Open(cmd.File)
	if err != nil {
		return vertexBatchError(500, "batch_spool_open_failed")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return vertexBatchError(500, "batch_spool_read_failed")
	}
	object := cmd.Prefix + "/" + cmd.Object
	query := url.Values{"uploadType": {"media"}, "name": {object}, "ifGenerationMatch": {"0"}}
	endpoint := "https://storage.googleapis.com/upload/storage/v1/b/" + url.PathEscape(cmd.Bucket) + "/o?" + query.Encode()
	mime := "application/jsonl"
	switch {
	case strings.HasSuffix(cmd.Object, ".jpg"):
		mime = "image/jpeg"
	case strings.HasSuffix(cmd.Object, ".png"):
		mime = "image/png"
	}
	// net/http closes its request body even on a rejected insert. Keep the
	// spool file open for the checksum comparison after a duplicate upload.
	body := io.NewSectionReader(f, 0, info.Size())
	resp, callErr := c.request(ctx, http.MethodPost, endpoint, mime, body, info.Size())
	if callErr == nil {
		resp.Body.Close()
		return nil
	}
	if callErr.Status != http.StatusPreconditionFailed {
		return callErr
	}
	var old struct {
		MD5 string `json:"md5Hash"`
	}
	endpoint = batchStorageAPI + url.PathEscape(cmd.Bucket) + "/o/" + url.PathEscape(object) + "?fields=md5Hash"
	if callErr = c.json(ctx, http.MethodGet, endpoint, nil, &old); callErr != nil {
		return callErr
	}
	digest := md5.New() // GCS exposes this checksum for simple uploads.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return vertexBatchError(500, "batch_spool_read_failed")
	}
	if _, err := io.Copy(digest, f); err != nil {
		return vertexBatchError(500, "batch_spool_read_failed")
	}
	if base64.StdEncoding.EncodeToString(digest.Sum(nil)) != old.MD5 {
		return vertexBatchError(409, "batch_file_already_exists")
	}
	return nil
}

func (c *vertexBatchClient) download(ctx context.Context, cmd batch.Command, job vertexJob, allowed string) *CallError {
	switch job.State {
	case "JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED":
	default:
		return vertexBatchError(409, "batch_results_pending")
	}
	if job.Output.Directory == "" {
		return vertexBatchError(404, "batch_results_unavailable")
	}
	dir := strings.TrimRight(job.Output.Directory, "/") + "/"
	if !strings.HasPrefix(dir, allowed) {
		return vertexBatchError(502, "batch_output_outside_namespace")
	}
	prefix := strings.TrimPrefix(dir, "gs://"+cmd.Bucket+"/")
	f, err := os.OpenFile(cmd.File, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return vertexBatchError(500, "batch_spool_open_failed")
	}
	defer f.Close()
	endpoint := batchStorageAPI + url.PathEscape(cmd.Bucket) + "/o"
	query := url.Values{"prefix": {prefix}, "fields": {"items(name),nextPageToken"}}
	count := 0
	remaining := int64(2_000_000_000)
	for {
		var page struct {
			Objects []struct{ Name string } `json:"items"`
			Next    string                  `json:"nextPageToken"`
		}
		if err := c.json(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil, &page); err != nil {
			return err
		}
		for _, obj := range page.Objects {
			if !strings.HasPrefix(obj.Name, prefix) || !strings.HasSuffix(obj.Name, ".jsonl") {
				continue
			}
			resp, err := c.request(ctx, http.MethodGet, endpoint+"/"+url.PathEscape(obj.Name)+"?alt=media", "", nil, 0)
			if err != nil {
				return err
			}
			n, copyErr := io.Copy(f, io.LimitReader(resp.Body, remaining+1))
			resp.Body.Close()
			remaining -= n + 1
			if copyErr != nil || remaining < 0 {
				return vertexBatchError(502, "batch_result_download_failed")
			}
			if _, err := f.WriteString("\n"); err != nil {
				return vertexBatchError(500, "batch_spool_write_failed")
			}
			count++
		}
		if page.Next == "" {
			break
		}
		query.Set("pageToken", page.Next)
	}
	if count == 0 {
		return vertexBatchError(404, "batch_results_unavailable")
	}
	if err := f.Close(); err != nil {
		return vertexBatchError(500, "batch_spool_write_failed")
	}
	return nil
}
