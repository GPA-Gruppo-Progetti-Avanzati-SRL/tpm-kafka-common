package reworkdlt

import (
	"github.com/rs/zerolog/log"
	"io"
	"time"
	"tpm-kafka-common/tprod"
	"tpm-kafka-common/tprod/echo"
)

type Config struct {
	WorkerConfig *echo.Config  `yaml:"worker,omitempty" mapstructure:"worker,omitempty" json:"worker,omitempty"`
	TickInterval time.Duration `yaml:"tick-interval" mapstructure:"tick-interval" json:"tick-interval"`
	CollectionId string        `yaml:"collection-id" mapstructure:"collection-id" json:"collection-id"`
}

type reworkDltImpl struct {
	cfg    *Config
	worker tprod.TransformerProducer
	parent tprod.Server
	quitc  chan struct{}
}

func NewReworkDlt(cfg *Config) (tprod.TransformerProducer, error) {
	var err error
	b := reworkDltImpl{cfg: cfg, quitc: make(chan struct{})}
	return &b, err
}

func (rwd *reworkDltImpl) SetParent(s tprod.Server) {
	rwd.parent = s
}

func (rwd *reworkDltImpl) Start() {

	const semLogContext = "rework dlt queue - start"
	log.Info().Msg("starting rework loop")
	ticker := time.NewTicker(rwd.cfg.TickInterval)

	for {
		select {
		case <-ticker.C:
			err := rwd.runWorker()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				ticker.Stop()
				rwd.parent.TransofmerProducerTerminated(err)
				return
			}

		case <-rwd.quitc:
			log.Info().Msg("terminating worker")
			if rwd.worker != nil {
				rwd.worker.Close()
			}
			ticker.Stop()
			// Should I do...?
			rwd.parent.TransofmerProducerTerminated(nil)
			return
		}
	}
}

func (rwd *reworkDltImpl) TransofmerProducerTerminated(err error) {
	const semLogContext = "rework dlt queue - worker terminated"
	rwd.worker = nil
	if err != nil {
		if err == io.EOF {
			log.Info().Msg(semLogContext + " eof condition reached")
		} else {
			log.Error().Err(err).Msg(semLogContext)
		}
	}
}

func (rwd *reworkDltImpl) Close() {
	log.Info().Msg("signalling shutdown transformer producer")

	close(rwd.quitc)
	if rwd.worker != nil {
		rwd.worker.Close()
	}
}

func (rwd *reworkDltImpl) runWorker() error {
	const semLogContext = "rework dlt queue:run worker - "

	if rwd.worker != nil {
		log.Info().Msg(semLogContext + " worker is still running")
		return nil
	}

	ok, err := rwd.checkCosmosDb()
	if err != nil {
		return err
	}

	if !ok {
		log.Info().Msg(semLogContext + " no work to be done")
		return nil
	}

	rwd.worker, err = echo.NewEcho(rwd.cfg.WorkerConfig, nil)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " worker initialization error")
		return err
	}

	rwd.worker.SetParent(rwd)
	rwd.worker.Start()
	return nil
}

func (rwd *reworkDltImpl) checkCosmosDb() (bool, error) {

	return false, nil
}
