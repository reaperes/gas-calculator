package dune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"golang.org/x/time/rate"
	"log"
	"net/http"
	"os"
	"reflect"
	"time"
)

// TODO: RPM depends on Dune subscription level. 15 is free.
const DUNE_CLIENT_RPM = 15
const DUNE_API_KEY = "Secret" // TODO
const DUNE_API_TIMEOUT = time.Duration(300) * time.Second

const REQUEST_CONTENT_TYPE = "application/json"

type DuneClient struct {
	httpClient  *fasthttp.Client
	rpm         int
	rateLimiter *rate.Limiter
}

type httpRequest struct {
	method  string
	url     string
	headers map[string]string
	body    *interface{}
}

// TODO: temporary timeout config
func NewDuneClient() *DuneClient {
	readTimeout, _ := time.ParseDuration("300s")
	writeTimeout, _ := time.ParseDuration("3s")
	maxIdleConnDuration, _ := time.ParseDuration("1h")
	httpClient := &fasthttp.Client{
		ReadTimeout:                   readTimeout,
		WriteTimeout:                  writeTimeout,
		MaxIdleConnDuration:           maxIdleConnDuration,
		NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
		DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this
		DisablePathNormalizing:        true,
		// increase DNS cache time to an hour instead of default minute
		Dial: (&fasthttp.TCPDialer{
			Concurrency:      4096,
			DNSCacheDuration: time.Hour,
		}).Dial,
	}

	dc := DuneClient{
		httpClient:  httpClient,
		rpm:         DUNE_CLIENT_RPM,
		rateLimiter: rate.NewLimiter(rate.Every(1*time.Minute/DUNE_CLIENT_RPM), 1),
	}

	return &dc
}

type DuneApiError struct {
	Message string `json:"message"`
}

type DuneExecutionResult struct {
	Rows []map[string]any `json:"rows"`
}

type GetExecutionResultResponse struct {
	ExecutionId        string               `json:"execution_id"`
	Error              *DuneApiError        `json:"error"`
	ExecutionStartedAt string               `json:"execution_started_at"`
	ExecutionEndedAt   string               `json:"execution_ended_at"`
	NextOffset         int                  `json:"next_offset"`
	Result             *DuneExecutionResult `json:"result"`
	State              string               `json:"state"`
}

func (dc *DuneClient) GetExecutionResult(executionId int, offset int, limit int) (*GetExecutionResultResponse, error) {
	url := fmt.Sprintf("https://api.dune.com/api/v1/query/%d/results?offset=%d&limit=%d&allow_partial_results=true", executionId, offset, limit)
	httpRequest := &httpRequest{
		method: fasthttp.MethodGet,
		headers: map[string]string{
			"X-Dune-API-Key": DUNE_API_KEY,
		},
		url: url,
	}

	resBytes, err := dc.throttleSendRequest(httpRequest)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("%v", resBytes)

	var res GetExecutionResultResponse
	err = json.Unmarshal(resBytes, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func (dc *DuneClient) throttleSendRequest(httpRequest *httpRequest) ([]byte, error) {
	err := dc.rateLimiter.Wait(context.TODO())
	if err != nil {
		return nil, err
	}
	return dc.sendRequest(httpRequest)
}

func (dc *DuneClient) sendRequest(httpRequest *httpRequest) ([]byte, error) {
	bodyBytes, err := json.Marshal(httpRequest.body)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(httpRequest.url)
	req.Header.SetMethod(httpRequest.method)
	req.Header.SetContentTypeBytes([]byte(REQUEST_CONTENT_TYPE))
	for k, v := range httpRequest.headers {
		req.Header.Set(k, v)
	}
	req.SetBodyRaw(bodyBytes)

	res := fasthttp.AcquireResponse()

	err = dc.httpClient.DoTimeout(req, res, DUNE_API_TIMEOUT)
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	if err != nil {
		errName, known := httpConnError(err)
		if known {
			fmt.Fprintf(os.Stderr, "WARN conn error: %v\n", errName)
		} else {
			fmt.Fprintf(os.Stderr, "ERR conn failure: %v %v\n", errName, err)
		}
		return nil, err
	}

	statusCode := res.StatusCode()
	resBody := res.Body()
	//fmt.Printf("DEBUG Response: %s\n", resBody)

	if statusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "ERR invalid HTTP response code: %d\n", statusCode)
		return nil, err
	}

	return resBody, nil
}

func httpConnError(err error) (string, bool) {
	var (
		errName string
		known   = true
	)

	switch {
	case errors.Is(err, fasthttp.ErrTimeout):
		errName = "timeout"
	case errors.Is(err, fasthttp.ErrNoFreeConns):
		errName = "conn_limit"
	case errors.Is(err, fasthttp.ErrConnectionClosed):
		errName = "conn_close"
	case reflect.TypeOf(err).String() == "*net.OpError":
		errName = "timeout"
	default:
		known = false
	}

	return errName, known
}
