package core

import "time"


type TracerEvent struct {
	// -- the name of the service
	Application string

	// -- the moment at which the event occured
	Timestamp time.Time

	// -- to which is the event targetted
	Component string

	// -- the type of event
	EventType string

	// -- the event payload
	Payload string
}

type TracerData struct {
	Application string

	// -- the moment at which the data came in occured
	Timestamp time.Time

	// -- the source of the data
	Source string

	// -- the difference between the time reported in the event vs the time at which the event is handled
	TimeDifferenceMs int64
}

func NewTracerEvent(app string, component string, kind string, payload string ) *TracerEvent {
	return &TracerEvent{Application: app,
		Timestamp: time.Now().UTC(),
		Component: component,
		EventType: kind,
		Payload: payload,
	}
}