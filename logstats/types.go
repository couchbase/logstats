package logstats

import (
	"encoding/json"
	"time"
)

type Timestamp struct {
	timestamp        time.Time
	customMarsheller func(Timestamp) ([]byte, error)
}

func (ts Timestamp) Equal(otherTs Timestamp) bool {
	var timeA, timeB = time.Time(ts.timestamp), time.Time(otherTs.timestamp)
	return timeA.Equal(timeB)
}

func (ts Timestamp) Since(curr Timestamp) string {
	var currTime = time.Time(curr.timestamp)
	return currTime.Sub(time.Time(ts.timestamp)).String()
}

func NewTimestampWithCustomMarshaller(ts time.Time, cm func(Timestamp) ([]byte, error)) Timestamp {
	return Timestamp{
		timestamp:        ts,
		customMarsheller: cm,
	}
}

func NewTimestamp(ts time.Time) Timestamp {
	return NewTimestampWithCustomMarshaller(ts, nil)
}

func NowTimestamp() Timestamp {
	return NewTimestamp(time.Now())
}

func (ts Timestamp) MarshalText() ([]byte, error) {
	if ts.customMarsheller != nil {
		return ts.customMarsheller(ts)
	}
	return []byte(ts.Since(NowTimestamp())), nil
}

func (ts Timestamp) MarshalJSON() ([]byte, error) {
	if ts.customMarsheller != nil {
		return ts.customMarsheller(ts)
	}
	return json.Marshal(ts.Since(NowTimestamp()))
}
