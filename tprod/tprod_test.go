package tprod_test

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestDuration(t *testing.T) {
	s := 600000
	var startDelay time.Duration
	if s > 0 {
		startDelay = time.Millisecond * time.Duration(s)
	}

	for i := 0; i < 5; i++ {
		startDelay = time.Millisecond * time.Duration(s*(i+1))
		if startDelay > 0 {
			time.Sleep(startDelay)
		}

		log.Info().Dur("delay", startDelay).Int64("start-delay", int64(startDelay)).Int("processor", i).Msg(" - starting processor...")
	}
}
