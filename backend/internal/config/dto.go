package config

type document struct {
	Version   int          `yaml:"version"`
	Mode      string       `yaml:"mode"`
	Source    string       `yaml:"source"`
	Listeners listenersDTO `yaml:"listeners"`
	UI        struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"ui"`
	ClientAuth    clientAuthDTO             `yaml:"client_auth"`
	AccessGroups  map[string]accessGroupDTO `yaml:"access_groups"`
	Providers     map[string]providerDTO    `yaml:"providers"`
	Routes        map[string]routeDTO       `yaml:"routes"`
	Execution     *executionDTO             `yaml:"execution_defaults"`
	Health        *healthDTO                `yaml:"health_defaults"`
	Limits        limitsDTO                 `yaml:"limits"`
	Observability *observabilityDTO         `yaml:"observability"`
}
type accessGroupDTO struct {
	Name string `yaml:"name"`
}
type listenersDTO struct {
	Gateway string `yaml:"gateway"`
	Admin   string `yaml:"admin"`
}
type clientAuthDTO struct {
	Mode                string                  `yaml:"mode"`
	AllowGeminiQueryKey bool                    `yaml:"allow_gemini_query_key"`
	Keys                map[string]clientKeyDTO `yaml:"keys"`
}
type clientKeyDTO struct {
	Name                string          `yaml:"name"`
	Enabled             *bool           `yaml:"enabled"`
	Secret              *string         `yaml:"secret"`
	SecretRef           *string         `yaml:"secret_ref"`
	SecretVersion       string          `yaml:"secret_version"`
	ExpiresAt           *string         `yaml:"expires_at"`
	AllowedRoutes       []string        `yaml:"allowed_routes"`
	AllowedAccessGroups []string        `yaml:"allowed_access_groups"`
	DeniedAccessGroups  []string        `yaml:"denied_access_groups"`
	Limits              *localLimitsDTO `yaml:"limits"`
	Retiring            *retiringDTO    `yaml:"retiring_secret"`
}
type retiringDTO struct {
	Secret        *string `yaml:"secret"`
	SecretRef     *string `yaml:"secret_ref"`
	SecretVersion string  `yaml:"secret_version"`
	ValidUntil    string  `yaml:"valid_until"`
}
type providerDTO struct {
	Name           string                      `yaml:"name"`
	Enabled        *bool                       `yaml:"enabled"`
	Origin         string                      `yaml:"origin"`
	Execution      *executionDTO               `yaml:"execution"`
	Health         *providerHealthDTO          `yaml:"health"`
	Credentials    map[string]credentialDTO    `yaml:"credentials"`
	Endpoints      map[string]endpointDTO      `yaml:"endpoints"`
	Models         map[string]modelDTO         `yaml:"models"`
	ResourceScopes map[string]resourceScopeDTO `yaml:"resource_scopes"`
	QuotaScopes    map[string]quotaScopeDTO    `yaml:"quota_scopes"`
}
type credentialDTO struct {
	AccessGroups  []string `yaml:"access_groups"`
	Name          string   `yaml:"name"`
	Enabled       *bool    `yaml:"enabled"`
	Secret        *string  `yaml:"secret"`
	SecretRef     *string  `yaml:"secret_ref"`
	SecretVersion string   `yaml:"secret_version"`
	Identity      struct {
		Attributes map[string]string `yaml:"attributes"`
		Selectors  map[string]string `yaml:"selectors"`
	} `yaml:"identity"`
	ResourceScope string        `yaml:"resource_scope"`
	QuotaScopes   []string      `yaml:"quota_scopes"`
	Execution     *executionDTO `yaml:"execution"`
}
type endpointDTO struct {
	Protocol     string            `yaml:"protocol"`
	Operation    string            `yaml:"operation"`
	PathTemplate string            `yaml:"path_template"`
	RequestModes []string          `yaml:"request_modes"`
	Headers      map[string]string `yaml:"headers"`
	Query        map[string]string `yaml:"query"`
	Auth         struct {
		Type string `yaml:"type"`
		Name string `yaml:"name"`
	} `yaml:"auth"`
}
type modelDTO struct {
	Name         string                `yaml:"name"`
	UpstreamName string                `yaml:"upstream_name"`
	Bindings     map[string]bindingDTO `yaml:"bindings"`
}
type bindingDTO struct {
	Enabled      *bool           `yaml:"enabled"`
	Credentials  []string        `yaml:"credentials"`
	RequestModes []string        `yaml:"request_modes"`
	Capabilities []string        `yaml:"capabilities"`
	QuotaScopes  []string        `yaml:"quota_scopes"`
	Limits       *localLimitsDTO `yaml:"limits"`
}
type resourceScopeDTO struct {
	Name          string            `yaml:"name"`
	Owner         map[string]string `yaml:"owner"`
	ResourceTypes []string          `yaml:"resource_types"`
	Sharing       string            `yaml:"sharing"`
	Verification  *struct {
		CheckedAt          string            `yaml:"checked_at"`
		Evidence           string            `yaml:"evidence"`
		CredentialVersions map[string]string `yaml:"credential_versions"`
	} `yaml:"verification"`
}
type quotaScopeDTO struct {
	Name    string            `yaml:"name"`
	Owner   map[string]string `yaml:"owner"`
	Targets []string          `yaml:"targets"`
	Limits  *localLimitsDTO   `yaml:"limits"`
}
type routeDTO struct {
	Name        string `yaml:"name"`
	Enabled     *bool  `yaml:"enabled"`
	Protocol    string `yaml:"protocol"`
	Operation   string `yaml:"operation"`
	PublicModel string `yaml:"public_model"`
	Selection   *struct {
		Strategy string      `yaml:"strategy"`
		Latency  *latencyDTO `yaml:"latency"`
	} `yaml:"selection"`
	Affinity *struct {
		Enabled bool    `yaml:"enabled"`
		Header  string  `yaml:"header"`
		TTL     *string `yaml:"ttl"`
	} `yaml:"session_affinity"`
	ResourcePolicy struct {
		Mode  string `yaml:"mode"`
		Scope string `yaml:"scope"`
	} `yaml:"resource_policy"`
	Targets []struct {
		Target   string  `yaml:"target"`
		Priority int     `yaml:"priority"`
		Weight   float64 `yaml:"weight"`
	} `yaml:"targets"`
}
type latencyDTO struct {
	Stream           *thresholdDTO `yaml:"stream"`
	NonStream        *thresholdDTO `yaml:"non_stream"`
	Window           *string       `yaml:"window"`
	MaxSamples       *int          `yaml:"max_samples"`
	MinSamples       *int          `yaml:"min_samples"`
	DegradeSamples   *int          `yaml:"degrade_samples"`
	RecoverySamples  *int          `yaml:"recovery_samples"`
	SlowWeightFactor *float64      `yaml:"slow_weight_factor"`
}
type thresholdDTO struct {
	Slow     *string `yaml:"slow_threshold"`
	Recovery *string `yaml:"recovery_threshold"`
}
type localLimitsDTO struct {
	MaxInflight *int `yaml:"max_inflight"`
	RequestRate *struct {
		Requests int    `yaml:"requests"`
		Interval string `yaml:"interval"`
		Burst    int    `yaml:"burst"`
	} `yaml:"request_rate"`
}
type limitsDTO struct {
	MaxInflight                 int     `yaml:"max_inflight"`
	QueueCapacity               int     `yaml:"queue_capacity"`
	QueueWaitTimeout            *string `yaml:"queue_wait_timeout"`
	MaxRequestBodyBytes         int64   `yaml:"max_request_body_bytes"`
	MaxDecompressedBodyBytes    int64   `yaml:"max_decompressed_body_bytes"`
	MaxTotalBodyBufferBytes     int64   `yaml:"max_total_body_buffer_bytes"`
	MaxTotalResponseBufferBytes int64   `yaml:"max_total_response_buffer_bytes"`
	MaxRuntimeStateEntries      int     `yaml:"max_runtime_state_entries"`
	MaxSessionBindings          int     `yaml:"max_session_bindings"`
	MaxSessionBindingsPerClient int     `yaml:"max_session_bindings_per_client"`
	MaxSessionIDBytes           int     `yaml:"max_session_id_bytes"`
}
type executionDTO struct {
	MaxRetries              *int    `yaml:"max_retries"`
	MaxRetriesPerProvider   *int    `yaml:"max_retries_per_provider"`
	RetryInterval           *string `yaml:"retry_interval"`
	RetryMaxInterval        *string `yaml:"retry_max_interval"`
	RetryWaitTimeout        *string `yaml:"retry_wait_timeout"`
	ConnectTimeout          *string `yaml:"connect_timeout"`
	TLSHandshakeTimeout     *string `yaml:"tls_handshake_timeout"`
	ResponseHeaderTimeout   *string `yaml:"response_header_timeout"`
	StreamFirstEventTimeout *string `yaml:"stream_first_event_timeout"`
	StreamFirstContent      *struct {
		Enabled *bool   `yaml:"enabled"`
		Timeout *string `yaml:"timeout"`
	} `yaml:"stream_first_content_timeout"`
	NonStreamResponseTimeout   *string `yaml:"non_stream_response_timeout"`
	StreamIdleTimeout          *string `yaml:"stream_idle_timeout"`
	DownstreamWriteIdleTimeout *string `yaml:"downstream_write_idle_timeout"`
	RequestBodyReadTimeout     *string `yaml:"request_body_read_timeout"`
	EmptyResponse              *struct {
		Enabled        *bool   `yaml:"enabled"`
		MaxBufferBytes *int64  `yaml:"max_buffer_bytes"`
		MaxWait        *string `yaml:"max_wait"`
	} `yaml:"empty_response"`
	ConditionalRetry *struct {
		Enabled           *bool          `yaml:"enabled"`
		Rules             []retryRuleDTO `yaml:"rules"`
		MaxErrorBodyBytes *int64         `yaml:"max_error_body_bytes"`
		MaxErrorBodyWait  *string        `yaml:"max_error_body_wait"`
	} `yaml:"conditional_retry"`
}
type matchDTO struct {
	Protocols       []string `yaml:"protocols"`
	Operations      []string `yaml:"operations"`
	StatusCodes     []int    `yaml:"status_codes"`
	ErrorCodes      []string `yaml:"error_codes"`
	ErrorTypes      []string `yaml:"error_types"`
	MessageContains []string `yaml:"message_contains"`
	MessageEquals   []string `yaml:"message_equals"`
	RetryAfter      string   `yaml:"retry_after"`
	FailureKinds    []string `yaml:"failure_kinds"`
}
type retryRuleDTO struct {
	ID     string   `yaml:"id"`
	Match  matchDTO `yaml:"match"`
	Action string   `yaml:"action"`
}
type healthRuleDTO struct {
	ID      string   `yaml:"id"`
	Match   matchDTO `yaml:"match"`
	Trigger *struct {
		Type       string  `yaml:"type"`
		Count      int     `yaml:"count"`
		Window     string  `yaml:"window"`
		MinSamples int     `yaml:"min_samples"`
		Rate       float64 `yaml:"ratio"`
	} `yaml:"trigger"`
	Action struct {
		Type     string `yaml:"type"`
		Duration string `yaml:"duration"`
		Until    string `yaml:"until"`
	} `yaml:"action"`
}
type breakerDTO struct {
	Enabled           *bool           `yaml:"enabled"`
	ExemptUntil       *string         `yaml:"exempt_until"`
	Rules             []healthRuleDTO `yaml:"rules"`
	NoAvailableTarget *struct {
		Action        string `yaml:"action"`
		ProbeInterval string `yaml:"probe_interval"`
	} `yaml:"no_available_target"`
}
type providerHealthDTO struct {
	CircuitBreaker *breakerDTO `yaml:"circuit_breaker"`
}
type healthDTO struct {
	CircuitBreaker              *breakerDTO `yaml:"circuit_breaker"`
	ActiveProbe                 bool        `yaml:"active_probe"`
	ConsecutiveFailureThreshold *int        `yaml:"consecutive_failure_threshold"`
	FailureWindow               *string     `yaml:"failure_window"`
	FailureRateMinSamples       *int        `yaml:"failure_rate_min_samples"`
	FailureRateThreshold        *float64    `yaml:"failure_rate_threshold"`
	MaxSamples                  *int        `yaml:"max_samples"`
	Cooldown                    *string     `yaml:"cooldown"`
	MaxCooldown                 *string     `yaml:"max_cooldown"`
	QuotaCooldown               *string     `yaml:"quota_cooldown"`
	UnknownConcurrency          *int        `yaml:"unknown_concurrency"`
	HalfOpenConcurrency         *int        `yaml:"half_open_concurrency"`
	HalfOpenSuccessThreshold    *int        `yaml:"half_open_success_threshold"`
	HealthyTTL                  *string     `yaml:"healthy_ttl"`
}
type observabilityDTO struct {
	RecentRequestCapacity    int  `yaml:"recent_request_capacity"`
	MaxObservedJSONBytes     int  `yaml:"max_observed_json_bytes"`
	MaxObservedSSEEventBytes int  `yaml:"max_observed_sse_event_bytes"`
	MaxObservedStateBytes    int  `yaml:"max_observed_state_bytes"`
	LogQueueCapacity         int  `yaml:"log_queue_capacity"`
	CaptureBodies            bool `yaml:"capture_bodies"`
	JSONL                    *struct {
		Enabled       bool   `yaml:"enabled"`
		Directory     string `yaml:"directory"`
		RetentionDays int    `yaml:"retention_days"`
		MaxTotalBytes int64  `yaml:"max_total_bytes"`
	} `yaml:"jsonl"`
}
