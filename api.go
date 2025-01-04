package bucketclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type RequestMethod string

const (
	METHOD_GET    RequestMethod = "GET"
	METHOD_POST   RequestMethod = "POST"
	METHOD_PUT    RequestMethod = "PUT"
	METHOD_DELETE RequestMethod = "DELETE"
)

func (db BucketDB) apiV1RequestGeneric(
	method RequestMethod,
	endpoint string,
	headers url.Values,
	body io.Reader) (data []byte, err error) {

	var res APIV1Response
	client := &http.Client{}

	// Request
	req, err := http.NewRequest(string(method), db.host+endpoint, body)
	if err != nil {
		return data, err
	}

	// Headers
	if headers != nil {
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	} else if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Overwriting headers for token
	if db.session.Token != "" {
		req.Header.Set("Authorization", "Bearer "+db.session.Token)
	}

	// Response
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return data, err
	}

	// Data
	err = json.Unmarshal(respBody, &res)
	if err != nil {
		return data, err
	}

	data, _ = json.Marshal(res.Data)

	// APIv1Response
	if !res.Success {
		return data, fmt.Errorf("failed: %s (%s)", res.Msg, res.Err)
	}
	if res.Err != "" {
		return data, fmt.Errorf("%s (err: %s)", res.Msg, res.Err)
	}

	// OK
	return data, nil
}

func (db BucketDB) apiV1Request(
	method RequestMethod,
	endpoint string,
	headers url.Values,
	body io.Reader) (data []byte, err error) {
	// Session checks
	if !db.IsValidSession() {
		if err := db.UpdateSession(); err != nil {
			return data, err
		}
	}
	return db.apiV1RequestGeneric(method, endpoint, headers, body)
}
