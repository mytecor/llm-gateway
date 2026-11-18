// Package batch defines the internal batch operations between the gateway and its driver.
package batch

const (
	Upload   = "upload"
	Submit   = "submit"
	Find     = "find"
	Status   = "status"
	Download = "download"
	Prepare  = "prepare"
)

// File is a private, gateway-created spool path. It is never accepted from an HTTP client.
type Command struct {
	Operation      string   `json:"operation"`
	Bucket         string   `json:"bucket"`
	Prefix         string   `json:"prefix"`
	Object         string   `json:"object,omitempty"`
	File           string   `json:"file,omitempty"`
	Name           string   `json:"name,omitempty"`
	Job            string   `json:"job,omitempty"`
	Input          string   `json:"input,omitempty"`
	Model          string   `json:"model,omitempty"`
	ImageProviders []string `json:"image_providers,omitempty"`
}

type Result struct {
	Job        string `json:"job,omitempty"`
	State      string `json:"state,omitempty"`
	Successful string `json:"successful,omitempty"`
	Failed     string `json:"failed,omitempty"`
}
