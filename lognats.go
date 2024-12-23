package lognats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	krakendgin "github.com/luraproject/lura/v2/router/gin"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const Namespace = "github_com/anshulgoel27/krakend-lognats"
const authHeader = "Authorization"
const DefaultCorrelationIdHeader = "X-Correlation-Id"

type LogNatsConfig struct {
	LogNatsTopic        string `json:"log_nats_topic"`
	CorrelationIdHeader string `json:"correlation_id_header"`
	//LogRequest   bool   `json:"log_request"`
	//LogResponse  bool   `json:"log_response"`
}

var ErrNoConfig = errors.New("no config defined for the module")
var ErrInvalidConfig = errors.New("invalid config defined for the module")

// Payload message payload
type Payload struct {
	Method      string      `json:"method,omitempty" mapstructure:"method"`
	Path        string      `json:"path,omitempty" mapstructure:"path"`
	URL         string      `json:"url,omitempty" mapstructure:"url"`
	Data        interface{} `json:"data,omitempty" mapstructure:"data"`
	Headers     interface{} `json:"headers,omitempty" mapstructure:"headers"`
	StatusCode  int         `json:"status_code,omitempty" mapstructure:"status_code"`
	RemoteAddr  string      `json:"ip_address,omitempty" mapstructure:"ip_address"`
	ForwardedIP string      `json:"ip_forwarded,omitempty" mapstructure:"ip_forwarded"`
}

func NewHandlerFactory(ctx context.Context, hf krakendgin.HandlerFactory, l logging.Logger) krakendgin.HandlerFactory {
	return func(cfg *config.EndpointConfig, p proxy.Proxy) gin.HandlerFunc {
		next := hf(cfg, p)
		logPrefix := "[ENDPOINT: " + cfg.Endpoint + "][lognats]"
		detectorCfg, err := ParseEndpointConfig(cfg.ExtraConfig)
		if err != nil {
			l.Warning(logPrefix, err.Error())
			return next
		}
		return handler(ctx, logPrefix, next, l, detectorCfg)
	}
}

func handler(ctx context.Context, logPrefix string, next gin.HandlerFunc, l logging.Logger, cfg LogNatsConfig) gin.HandlerFunc {
	url := os.Getenv("NATS_SERVER_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Connect to NATS server
	nc, err := nats.Connect(url)
	if err != nil {
		l.Error(fmt.Sprintf("%s Error connecting to NATS: %s", logPrefix, err.Error()))
		return next
	}

	// Create a JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		l.Error(fmt.Sprintf("%s Error initializing JetStream: %s", logPrefix, err.Error()))
		return next
	}

	l.Debug(fmt.Sprintf("%s Publisher initialized successfully", logPrefix))

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		nc.Drain()
	}()

	return func(c *gin.Context) {

		if c.Request.Header.Get(cfg.CorrelationIdHeader) == "" {
			id := uuid.New()
			c.Request.Header.Set(cfg.CorrelationIdHeader, id.String())
		}

		next(c)

		go func() {
			payload := Payload{
				Method:      c.Request.Method,
				URL:         c.Request.URL.RequestURI(),
				Path:        c.Request.URL.Path,
				RemoteAddr:  c.Request.RemoteAddr,
				ForwardedIP: c.ClientIP(),
				StatusCode:  c.Writer.Status(),
			}

			headers := c.Request.Header.Clone()
			// Remove Authorization header
			headers.Del(authHeader)
			payload.Headers = headers

			raw, _ := io.ReadAll(c.Request.Body)
			if len(raw) > 0 {
				c.Request.Body = io.NopCloser(bytes.NewReader(raw))
				var pl map[string]interface{}
				if err := json.Unmarshal(raw, &pl); err == nil {
					payload.Data = pl
				}
			}

			b, err := json.Marshal(payload)
			if err == nil {
				// Publish the message to the topic
				msg := &nats.Msg{
					Subject: cfg.LogNatsTopic,
					Data:    b,
				}

				if _, err := js.PublishMsgAsync(msg); err != nil {
					l.Error(fmt.Sprintf("%s Error publishing message: %s", logPrefix, err.Error()))
				}
			}
		}()
	}
}

func ParseEndpointConfig(cfg config.ExtraConfig) (LogNatsConfig, error) {
	res := LogNatsConfig{}
	e, ok := cfg[Namespace]
	if !ok {
		return res, ErrNoConfig
	}
	b, err := json.Marshal(e)
	if err != nil {
		return res, err
	}
	err = json.Unmarshal(b, &res)

	// Set defaults if not provided
	if res.LogNatsTopic == "" {
		return res, ErrInvalidConfig
	}

	if res.CorrelationIdHeader == "" {
		res.CorrelationIdHeader = DefaultCorrelationIdHeader
	}

	// if !res.LogRequest && !res.LogResponse {
	// 	return res, ErrInvalidConfig
	// }

	return res, err
}
