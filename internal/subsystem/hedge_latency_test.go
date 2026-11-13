package subsystem

import (
	"bytes"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/llm-gateway/internal/telemetry"
)

func warmLatency(s *HedgeLatencyStore, latency time.Duration) {
	for range 20 {
		s.Begin("a", "native", true).Success(latency)
	}
}

func TestReadyThresholdRequiresFreshWarmProfile(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewHedgeLatencyStore(func() time.Time { return now }, nil)
	if s.ReadyThreshold("a", "native", true) != 0 || len(s.profiles) != 0 {
		t.Fatal("read created a cold profile")
	}
	for range 19 {
		s.Begin("a", "native", true).Success(time.Second)
	}
	if s.ReadyThreshold("a", "native", true) != 0 {
		t.Fatal("read used an untrained threshold")
	}
	s.Begin("a", "native", true).Success(time.Second)
	if s.ReadyThreshold("a", "native", true) <= 0 {
		t.Fatal("trained threshold unavailable")
	}
	if s.ReadyThreshold("a", "other", true) != 0 || s.ReadyThreshold("a", "native", false) != 0 {
		t.Fatal("threshold crossed profile boundaries")
	}
	now = now.Add(hedgeRetention)
	if s.ReadyThreshold("a", "native", true) != 0 {
		t.Fatal("expired threshold reused")
	}
}

func TestHedgeLatencyWarmupNearestRankAndIsolation(t *testing.T) {
	s := NewHedgeLatencyStore(nil, nil)
	for i := 1; i <= 20; i++ {
		o := s.Begin("a", "native", true)
		if o.Threshold != 0 {
			t.Fatal("ready before 20 successes")
		}
		o.Success(time.Duration(i) * time.Millisecond)
	}
	if got := s.Begin("a", "native", true).Threshold; got != 19*time.Millisecond {
		t.Fatalf("p95 = %v", got)
	}
	for _, key := range []hedgeProfileKey{{"b", "native", true}, {"a", "other", true}, {"a", "native", false}} {
		if s.Begin(key.provider, key.native, key.stream).Threshold != 0 {
			t.Fatalf("profile leaked into %#v", key)
		}
	}
}

func TestHedgeLatencyCensoringAndExactlyOnce(t *testing.T) {
	for _, kind := range []string{"success", "slow loser", "early loser", "discard", "zero"} {
		t.Run(kind, func(t *testing.T) {
			s := NewHedgeLatencyStore(nil, nil)
			warmLatency(s, time.Second)
			o := s.Begin("a", "native", true)
			switch kind {
			case "success":
				o.Success(time.Second / 2)
			case "slow loser":
				o.Censor(2 * time.Second)
			case "early loser":
				o.Censor(time.Second / 2)
			case "discard":
				o.Discard()
			case "zero":
				o.Success(0)
			}
			got := s.Begin("a", "native", true).Threshold
			if kind == "success" && got >= time.Second {
				t.Fatal("timely success did not lower estimate")
			}
			if kind == "slow loser" && got <= time.Second {
				t.Fatal("censored tail did not raise estimate")
			}
			if kind != "success" && kind != "slow loser" && got != time.Second {
				t.Fatal("neutral observation changed estimate")
			}
			o.Success(4 * time.Second)
			o.Censor(5 * time.Second)
			if s.Begin("a", "native", true).Threshold != got {
				t.Fatal("attempt counted twice")
			}
		})
	}
	s := NewHedgeLatencyStore(nil, nil)
	for range 30 {
		s.Begin("a", "native", true).Censor(time.Hour)
	}
	if s.Begin("a", "native", true).Threshold != 0 {
		t.Fatal("timeouts warmed profile")
	}
}

func TestHedgeLatencyExpiryEvictionAndGeneration(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	metrics := telemetry.NewMetrics()
	s := NewHedgeLatencyStore(func() time.Time { return now }, metrics)
	warmLatency(s, time.Second)
	old := s.Begin("a", "native", true)
	now = now.Add(5 * time.Minute)
	if s.Begin("a", "native", true).Threshold != 0 {
		t.Fatal("stale estimate used")
	}
	old.Success(time.Hour)
	for range 19 {
		s.Begin("a", "native", true).Success(time.Second)
	}
	if s.Begin("a", "native", true).Threshold != 0 {
		t.Fatal("old generation warmed new profile")
	}
	var b bytes.Buffer
	if err := metrics.WriteExposition(&b); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "llm_hedge_delay_seconds{") {
		t.Fatal("expired gauge retained")
	}
	now = now.Add(5 * time.Minute)
	s.Begin("b", "native", false)
	if len(s.profiles) != 1 {
		t.Fatalf("stale profiles retained: %d", len(s.profiles))
	}
}

func TestHedgeLatencyInflightWarmupAndStaleCompletion(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewHedgeLatencyStore(func() time.Time { return now }, nil)
	inflight := s.Begin("a", "native", true)
	warmLatency(s, time.Second)
	inflight.Success(time.Hour)
	if s.Begin("a", "native", true).Threshold != time.Second {
		t.Fatal("warmup attempt updated ready quantile without snapshot")
	}
	stale := s.Begin("a", "native", true)
	now = now.Add(5 * time.Minute)
	stale.Success(time.Hour)
	if s.Begin("a", "native", true).Threshold != 0 {
		t.Fatal("stale completion revived profile")
	}
}

func TestHedgeLatencyAdaptsWithoutCompleteTailSamples(t *testing.T) {
	s := NewHedgeLatencyStore(nil, nil)
	warmLatency(s, time.Second)
	r := rand.New(rand.NewSource(42))
	for _, factor := range []float64{1, 3, 1} {
		exceeded := 0
		for i := 0; i < 20000; i++ {
			latency := time.Duration(factor * math.Exp(.7*r.NormFloat64()) * float64(time.Second))
			o := s.Begin("a", "native", true)
			if latency > o.Threshold {
				exceeded++
				o.Censor(o.Threshold + time.Nanosecond) // no exact slow duration ever arrives
			} else {
				o.Success(latency)
			}
		}
		share := float64(exceeded) / 20000
		if share < .045 || share > .055 {
			t.Fatalf("factor %v: overdue share %v", factor, share)
		}
	}
}

func TestHedgeLatencyConcurrentObservationsAndDurationBounds(t *testing.T) {
	s := NewHedgeLatencyStore(nil, nil)
	warmLatency(s, time.Second)
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				o := s.Begin("a", "native", true)
				o.Success(time.Second)
				o.Censor(time.Hour)
			}
		}()
	}
	wg.Wait()
	for range 1000 {
		s.Begin("a", "native", true).Censor(time.Duration(1<<63 - 1))
	}
	q := s.Begin("a", "native", true).Threshold
	if q <= 0 {
		t.Fatal("duration overflow")
	}
	for range 10000 {
		s.Begin("a", "native", true).Success(time.Nanosecond)
	}
	if q := s.Begin("a", "native", true).Threshold; q <= 0 {
		t.Fatal("duration underflow")
	}
}
