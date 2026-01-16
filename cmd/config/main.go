package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

// Config maps the identifiers to endpoints.
type Config map[string]*meow.Endpoint

// ConcurrentConfig wraps the config together with a mutex.
type ConcurrentConfig struct {
	mu     sync.RWMutex
	config Config
}

var cfg ConcurrentConfig

func main() {
	addr := flag.String("addr", "0.0.0.0", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	file := flag.String("file", "config.csv", "CSV file to store the configuration")
	flag.Parse()

	log.SetOutput(os.Stderr)

	// Read Valkey address from environment
	valkeyURL, ok := os.LookupEnv("VALKEY_URL")
	if !ok || valkeyURL == "" {
		log.Fatal("VALKEY_URL environment variable is not set")
	}

	ctx := context.Background()
	options := valkey.ClientOption{
		InitAddress: []string{valkeyURL},
		SelectDB:    44, // DB number
	}

	vk, err := valkey.NewClient(options)
	if err != nil {
		log.Fatalf("failed to create valkey client: %v", err)
	}

	if err := vk.Do(ctx, vk.B().Ping().Build()).Error(); err != nil {
		log.Fatalf("valkey ping failed: %v", err)
	}
	log.Printf("connected to valkey at %s", valkeyURL)

	configFromValkey, err := fetchAllEndpoints(ctx, vk)
	if err != nil {
		log.Fatalf("load config from valkey: %v", err)
	}
	if len(configFromValkey) == 0 {
		configFromValkey = mustReadConfig(*file)
		if err := persistConfigToValkey(ctx, vk, configFromValkey); err != nil {
			log.Fatalf("persist config to valkey: %v", err)
		}
	}
	cfg.config = configFromValkey

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(w, r, vk)
		case http.MethodPost:
			postEndpoint(w, r, *file, vk)
		case http.MethodDelete:
			deleteEndpoint(w, r, *file, vk)
		default:
			log.Printf("request from %s rejected: method %s not allowed",
				r.RemoteAddr, r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(w, r, vk)
	})

	listenTo := fmt.Sprintf("%s:%d", *addr, *port)
	log.Printf("listen to %s", listenTo)
	http.ListenAndServe(listenTo, nil)
}

func getEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	endpoint, err := fetchEndpoint(r.Context(), vk, identifier)
	if err != nil {
		log.Printf("fetch endpoint %s from valkey: %v", identifier, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if endpoint == nil {
		log.Printf(`no such endpoint "%s"`, identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	payload, err := endpoint.JSON()
	if err != nil {
		log.Printf("convert %v to JSON: %v", endpoint, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(payload)
}

func postEndpoint(w http.ResponseWriter, r *http.Request, file string, vk valkey.Client) {
	log.Printf("POST %s from %s", r.URL, r.RemoteAddr)
	buf := bytes.NewBufferString("")
	io.Copy(buf, r.Body)
	defer r.Body.Close()
	endpoint, err := meow.EndpointFromJSON(buf.String())
	if err != nil {
		log.Printf("parse JSON body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	existing, err := fetchEndpoint(r.Context(), vk, endpoint.Identifier)
	if err != nil {
		log.Printf("fetch endpoint %s from valkey: %v", endpoint.Identifier, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var status int
	if existing != nil {
		// updating existing endpoint
		identifierPathParam, err := extractEndpointIdentifier(r.URL.String())
		if err != nil {
			log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if identifierPathParam != endpoint.Identifier {
			log.Printf("identifier mismatch: (ressource: %s, body: %s)",
				identifierPathParam, endpoint.Identifier)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status = http.StatusNoContent
	} else {
		status = http.StatusCreated
	}

	if err := storeEndpoint(r.Context(), vk, endpoint); err != nil {
		log.Printf("store endpoint in valkey: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	cfg.mu.Lock()
	if cfg.config == nil {
		cfg.config = make(Config)
	}
	cfg.config[endpoint.Identifier] = endpoint
	if err := writeConfig(cfg.config, file); err != nil {
		status = http.StatusInternalServerError
	}
	cfg.mu.Unlock()
	w.WriteHeader(status)
}

func deleteEndpoint(w http.ResponseWriter, r *http.Request, file string, vk valkey.Client) {
	log.Printf("DELETE %s from %s", r.URL, r.RemoteAddr)

	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Check existence first
	existing, err := fetchEndpoint(r.Context(), vk, identifier)
	if err != nil {
		log.Printf("fetch endpoint %s from valkey: %v", identifier, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if existing == nil {
		log.Printf("no such endpoint %q", identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := deleteEndpointKey(r.Context(), vk, identifier); err != nil {
		log.Printf("delete endpoint %s from valkey: %v", identifier, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Delete and persist
	cfg.mu.Lock()
	delete(cfg.config, identifier)
	status := http.StatusNoContent
	if err := writeConfig(cfg.config, file); err != nil {
		log.Printf("write config: %v", err)
		status = http.StatusInternalServerError
	}
	cfg.mu.Unlock()

	w.WriteHeader(status)
}

func getEndpoints(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	if r.Method != http.MethodGet {
		log.Printf("request from %s rejected: method %s not allowed",
			r.RemoteAddr, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)

	config, err := fetchAllEndpoints(r.Context(), vk)
	if err != nil {
		log.Printf("fetch endpoints from valkey: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payloads := make([]meow.EndpointPayload, 0)
	for _, endpoint := range config {
		payload := meow.EndpointPayload{
			Identifier:   endpoint.Identifier,
			URL:          endpoint.URL.String(),
			Method:       endpoint.Method,
			StatusOnline: endpoint.StatusOnline,
			Frequency:    endpoint.Frequency.String(),
			FailAfter:    endpoint.FailAfter,
		}
		payloads = append(payloads, payload)
	}
	cfg.mu.Lock()
	cfg.config = config
	cfg.mu.Unlock()
	data, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("serialize payloads: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

const endpointIdentifierPatternRaw = "^/endpoints/([a-z][-a-z0-9]+)$"

var endpointIdentifierPattern = regexp.MustCompile(endpointIdentifierPatternRaw)

func extractEndpointIdentifier(endpoint string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(endpoint)
	if len(matches) == 0 {
		return "", fmt.Errorf(`endpoint "%s" does not match pattern "%s"`,
			endpoint, endpointIdentifierPatternRaw)
	}
	return matches[1], nil
}

func writeConfig(config Config, configPath string) error {
	file, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf(`open "%s" for write: %v`, configPath, err)
	}

	writer := csv.NewWriter(file)
	defer file.Close()
	for _, endpoint := range config {
		record := []string{
			endpoint.Identifier,
			endpoint.URL.String(),
			endpoint.Method,
			strconv.Itoa(int(endpoint.StatusOnline)),
			endpoint.Frequency.String(),
			strconv.Itoa(int(endpoint.FailAfter)),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf(`write endpoint "%s": %v`, endpoint, err)
		}
	}
	writer.Flush()
	return nil
}

func mustReadConfig(configPath string) Config {
	file, err := os.Open(configPath)
	if os.IsNotExist(err) {
		// just start with an empty config
		log.Printf(`the config file "%s" does not exist`, configPath)
		return Config{}
	}

	config := make(Config, 0)
	reader := csv.NewReader(file)
	defer file.Close()
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("the config file '%s' is malformed: %v", configPath, err)
	}
	for i, line := range records {
		endpoint, err := meow.EndpointFromRecord(line)
		if err != nil {
			log.Fatalf(`line %d: "%s": %v`, i, line, err)
		}
		config[endpoint.Identifier] = endpoint
	}
	return config
}

const endpointKeyPrefix = "endpoints:"

func endpointKey(identifier string) string {
	return fmt.Sprintf("%s%s", endpointKeyPrefix, identifier)
}

func fetchEndpoint(ctx context.Context, vk valkey.Client, identifier string) (*meow.Endpoint, error) {
	kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(endpointKey(identifier)).Build()).AsStrMap()
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, nil
	}
	return endpointFromHash(kvs)
}

func fetchAllEndpoints(ctx context.Context, vk valkey.Client) (Config, error) {
	keys, err := vk.Do(ctx, vk.B().Keys().Pattern(endpointKeyPrefix+"*").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list endpoint keys: %w", err)
	}
	config := make(Config)
	for _, key := range keys {
		kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			return nil, fmt.Errorf("hgetall %s: %w", key, err)
		}
		if len(kvs) == 0 {
			continue
		}
		endpoint, err := endpointFromHash(kvs)
		if err != nil {
			return nil, fmt.Errorf("parse endpoint %s: %w", key, err)
		}
		config[endpoint.Identifier] = endpoint
	}
	return config, nil
}

func storeEndpoint(ctx context.Context, vk valkey.Client, endpoint *meow.Endpoint) error {
	cmd := vk.B().Hset().
		Key(endpointKey(endpoint.Identifier)).
		FieldValue().
		FieldValue("identifier", endpoint.Identifier).
		FieldValue("url", endpoint.URL.String()).
		FieldValue("method", endpoint.Method).
		FieldValue("status_online", strconv.Itoa(int(endpoint.StatusOnline))).
		FieldValue("frequency", endpoint.Frequency.String()).
		FieldValue("fail_after", strconv.Itoa(int(endpoint.FailAfter)))

	if err := vk.Do(ctx, cmd.Build()).Error(); err != nil {
		return fmt.Errorf("write endpoint %s to valkey: %w", endpoint.Identifier, err)
	}
	return nil
}

func deleteEndpointKey(ctx context.Context, vk valkey.Client, identifier string) error {
	if err := vk.Do(ctx, vk.B().Del().Key(endpointKey(identifier)).Build()).Error(); err != nil {
		return fmt.Errorf("delete endpoint %s: %w", identifier, err)
	}
	return nil
}

func persistConfigToValkey(ctx context.Context, vk valkey.Client, config Config) error {
	for _, endpoint := range config {
		if err := storeEndpoint(ctx, vk, endpoint); err != nil {
			return err
		}
	}
	return nil
}

func endpointFromHash(kvs map[string]string) (*meow.Endpoint, error) {
	statusOnline, err := strconv.Atoi(kvs["status_online"])
	if err != nil {
		return nil, fmt.Errorf("parse status_online: %w", err)
	}
	failAfter, err := strconv.Atoi(kvs["fail_after"])
	if err != nil {
		return nil, fmt.Errorf("parse fail_after: %w", err)
	}

	payload := meow.EndpointPayload{
		Identifier:   kvs["identifier"],
		URL:          kvs["url"],
		Method:       kvs["method"],
		StatusOnline: uint16(statusOnline),
		Frequency:    kvs["frequency"],
		FailAfter:    uint8(failAfter),
	}
	return meow.EndpointFromPayload(payload)
}
