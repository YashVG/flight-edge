package models

import "time"

// Flight represents a tracked flight with its current state (ingestion DTO).
type Flight struct {
	ICAO24      string    `json:"icao24"`
	Callsign    string    `json:"callsign"`
	Origin      string    `json:"origin_country"`
	Longitude   float64   `json:"longitude"`
	Latitude    float64   `json:"latitude"`
	Altitude    float64   `json:"baro_altitude"`
	Velocity    float64   `json:"velocity"`
	Heading     float64   `json:"true_track"`
	OnGround    bool      `json:"on_ground"`
	LastContact time.Time `json:"last_contact"`
}

// DelayRecord captures a delay event tied to a flight.
type DelayRecord struct {
	FlightID    string        `json:"flight_id"`
	Callsign    string        `json:"callsign"`
	Origin      string        `json:"origin"`
	Destination string        `json:"destination"`
	Delay       time.Duration `json:"delay"`
	Cause       DelayCause    `json:"cause"`
	Timestamp   time.Time     `json:"timestamp"`
}

// DelayCause categorises why a delay occurred.
type DelayCause string

const (
	CauseWeather    DelayCause = "weather"
	CauseMechanical DelayCause = "mechanical"
	CauseATC        DelayCause = "atc"
	CauseUnknown    DelayCause = "unknown"
)
