package breaker_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jenil133/Controlplane/pkg/breaker"
)

func Example() {
	b := breaker.New(breaker.Settings{
		FailureRateThreshold: 0.5,
		MinRequests:          3,
		Window:               10 * time.Second,
		OpenDuration:         time.Minute,
		HalfOpenMaxRequests:  1,
	})
	unavailable := errors.New("payments unavailable")
	for range 3 {
		fmt.Println(b.Do(func() error { return unavailable }))
	}
	fmt.Println(b.State())
	fmt.Println(b.Do(func() error { return nil }))
	// Output:
	// payments unavailable
	// payments unavailable
	// payments unavailable
	// open
	// breaker: circuit open
}

func ExampleBreaker_Allow() {
	b := breaker.New(breaker.Settings{})
	done, err := b.Allow()
	if err != nil {
		fmt.Println("rejected:", err)
		return
	}
	// Make the call here, then report how it went.
	done(true)
	fmt.Printf("%+v\n", b.Counts())
	// Output: {Requests:1 Failures:0}
}

func ExampleSet() {
	s := breaker.NewSet(breaker.WithSetStateChange(func(key string, from, to breaker.State) {
		fmt.Printf("%s: %v -> %v\n", key, from, to)
	}))
	s.Update([]breaker.Policy{{
		Key:      "payments",
		Enabled:  true,
		Settings: breaker.Settings{MinRequests: 2, OpenDuration: time.Minute},
	}})
	for range 2 {
		_ = s.Do("payments", func() error { return errors.New("timeout") })
	}
	fmt.Println(s.Do("payments", func() error { return nil }))
	fmt.Println(s.Do("search", func() error { return nil })) // no policy, never rejected
	// Output:
	// payments: closed -> open
	// breaker: circuit open
	// <nil>
}
