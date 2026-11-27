package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func newBatchID() (string, error) {
	var random [16]byte
	_, err := rand.Read(random[:])
	return hex.EncodeToString(random[:]), err
}

func (b *batchServer) workspacePath(id string) string {
	return filepath.Join(b.settings.StateDir, "workspace-"+b.jobKey(id)+".json")
}
func (b *batchServer) workspace(id string) (record batchRecord, err error) {
	data, err := os.ReadFile(b.workspacePath(id))
	if err == nil {
		err = json.Unmarshal(data, &record)
	}
	return record, err
}

func (b *batchServer) prepareWorkspace(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Model string `json:"model"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	d.DisallowUnknownFields()
	if d.Decode(&request) != nil || d.Decode(new(any)) != io.EOF {
		writeAPIError(w, 400, "invalid_batch_workspace")
		return
	}
	id, err := newBatchID()
	if err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	record, ok := b.newRecord(id, request.Model)
	if !ok {
		writeAPIError(w, 400, "unknown_batch_model")
		return
	}
	if err := writeImmutable(b.workspacePath(id), marshalBatch(record)); err != nil {
		writeAPIError(w, 500, "batch_state_unavailable")
		return
	}
	writeJSON(w, 201, map[string]string{"id": id, "model": record.Model})
}

func writeImmutable(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".metadata-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	return os.Link(f.Name(), path)
}
func (b *batchServer) schemaPath(hash string) string {
	return filepath.Join(b.settings.StateDir, "schema-"+hash+".json")
}
func (b *batchServer) storeSchema(hash string, raw []byte) error {
	err := writeImmutable(b.schemaPath(hash), raw)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	return err
}

func (b *batchServer) claimWorkspaceInput(w http.ResponseWriter, id string, sum []byte) bool {
	// Claim the exact source bytes before installing provider input. A retry can
	// complete an interrupted upload, but a different custom_id map cannot win.
	intent := b.workspacePath(id) + ".input"
	if err := writeImmutable(intent, sum); errors.Is(err, os.ErrExist) {
		previous, err := os.ReadFile(intent)
		if err != nil {
			batchReadError(w, err)
			return false
		}
		if !bytes.Equal(sum, previous) {
			writeAPIError(w, 409, "batch_file_already_exists")
			return false
		}
		if meta, err := b.readOpenAIFile(id); err == nil {
			writeJSON(w, 200, openAIFileResponse("file-"+id, meta))
			return false
		} else if !errors.Is(err, os.ErrNotExist) {
			batchReadError(w, err)
			return false
		}
	} else if err != nil {
		batchReadError(w, err)
		return false
	}
	return true
}
