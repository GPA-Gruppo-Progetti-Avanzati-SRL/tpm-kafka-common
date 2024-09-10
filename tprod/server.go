package tprod

import (
	"errors"
	"github.com/rs/zerolog/log"
	"time"
)

type Server interface {
	Close()
	Start()
	TransformerProducerTerminated(err error)
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

	var startDelay time.Duration
	if s.cfg.StartDelay > 0 {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay)
	}

	log.Info().Msg(semLogContext)
	for i, tp := range s.transformerProducer {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay*(i+1))
		log.Info().Dur("delay", startDelay).Str("processor", tp.Name()).Msg(semLogContext + " - starting processor...")
		if startDelay > 0 {
			time.Sleep(startDelay)
		}

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
		if s.cfg.OnWorkerTerminated == "exit" {
			log.Error().Err(err).Msg(semLogContext + " on worker terminated.... server shutting down")
			s.quitc <- errors.New("tprod-server shutting down")
		}
		/*
			if err != io.EOF {
				if s.cfg.OnError == OnErrorExit {
					log.Error().Err(err).Msg(semLogContext + " on first transform producer error.... server shutting down")
					s.quitc <- errors.New("kafka consumer server fatal error")
				}
			} else if s.cfg.OnEof == OnEofExit {
				log.Info().Msg(semLogContext + " on first transform producer EOF.... server shutting down")
				s.quitc <- errors.New("kafka consumer server eof")
			}
		*/
	}
}
