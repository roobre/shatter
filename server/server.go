package server

import (
	"context"
	"fmt"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"roob.re/refractor/pool"
	"roob.re/refractor/provider/providers"
	"roob.re/refractor/provider/types"
	"roob.re/refractor/refractor"
	"roob.re/refractor/stats"
)

type Config struct {
	Stats     stats.Config     `yaml:",inline"`
	Refractor refractor.Config `yaml:",inline"`

	// Provider contains the name of the chosen provider, and provider-specific config.
	Provider map[string]yaml.Node
}

type Server struct {
	pool      *pool.Pool
	refractor *refractor.Refractor
}

func New(configFile io.Reader) (*Server, error) {
	config := Config{}
	err := yaml.NewDecoder(configFile).Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	var provider types.Provider
	for pName, yamlConfig := range config.Provider {
		pBuilder, found := providers.Map[pName]
		if !found {
			return nil, fmt.Errorf("unknown provider %q", pName)
		}

		pConfig := pBuilder.DefaultConfig()
		err := yamlConfig.Decode(pConfig)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling config for provider %q: %w", pName, err)
		}

		provider, err = pBuilder.New(pConfig)
		if err != nil {
			return nil, fmt.Errorf("creating provider %q: %w", pName, err)
		}

		log.Infof("Using provider %q", pName)

		break
	}

	p := pool.New(
		provider,
		stats.New(config.Stats),
	)

	return &Server{
		pool: p,
		refractor: refractor.New(
			config.Refractor,
			p,
		),
	}, nil
}

func (s *Server) Run(address string) error {
	ctx := context.Background()

	go s.pool.Start(ctx)

	log.Infof("Listening on %s", address)
	return http.ListenAndServe(address, s.refractor)
}
