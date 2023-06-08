package tprod

import (
	"errors"
	"github.com/rs/zerolog/log"
	"io"
	"time"
)

type Server interface {
	Close()
	Start()
	TransformerProducerTerminated(err error)
}

type ServerConfig struct {
	Exit              ConfigExitPolicy `yaml:"exit" mapstructure:"exit" json:"exit"`
	EnabledProcessors string           `yaml:"enabled-processors,omitempty" mapstructure:"enabled-processors,omitempty" json:"enabled-processors,omitempty"`
}

type server struct {
	cfg                     *ServerConfig
	transformerProducer     []TransformerProducer
	numberOfActiveProducers int
	quitc                   chan error
}

func NewServer(cfg *ServerConfig, tps []TransformerProducer, c chan error) (Server, error) {
	s := &server{
		cfg:   cfg,
		quitc: c,
	}

	for _, tp := range tps {
		s.Add(tp)
		tp.SetParent(s)
	}

	s.numberOfActiveProducers = len(s.transformerProducer)
	return s, nil
}

func (s *server) Close() {
	const semLogContext = "t-prod-server::close"
	if len(s.transformerProducer) > 0 {
		log.Info().Msg(semLogContext + " closing transformer producer")
		for _, tp := range s.transformerProducer {
			tp.Close()
		}

		// This is a wait to allow the producers to exit from the loop.
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *server) Add(tp TransformerProducer) {
	s.transformerProducer = append(s.transformerProducer, tp)
	s.numberOfActiveProducers++
}

func (s *server) Start() {
	const semLogContext = "t-prod-server::start"
	log.Info().Msg(semLogContext)
	for _, tp := range s.transformerProducer {
		tp1 := tp
		go tp1.Start()
	}
}

func (s *server) TransformerProducerTerminated(err error) {
	const semLogContext = "t-prod-server::producer-terminated"
	log.Info().Msg(semLogContext)
	if err == nil {
		return
	}

	s.numberOfActiveProducers--

	if s.numberOfActiveProducers == 0 {
		log.Info().Msg(semLogContext + " no more producers.... server not operative")
		s.quitc <- errors.New("kafka consumer server not operative")
	} else {
		if err != io.EOF {
			if s.cfg.Exit.OnFail {
				log.Error().Err(err).Msg(semLogContext + " on first transform producer error.... server shutting down")
				s.quitc <- errors.New("kafka consumer server fatal error")
			}
		} else if s.cfg.Exit.OnEof {
			log.Info().Msg(semLogContext + " on first transform producer EOF.... server shutting down")
			s.quitc <- errors.New("kafka consumer server eof")
		}
	}
}
