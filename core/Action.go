package core

import (
	"time"
)

func NewAction(app string, component string, kind string) *Action {
	return &Action{
		Application: app,
		Start: time.Now().UTC(),
		Component: component,
		Kind: kind,
	};
}

type Action struct {
	// -- the name of the service
	Application string

	// -- the moment at which the action occured
	Start time.Time

	// -- the moment at which the action finished
	End time.Time

	// -- the amount of time it took to perform the action
	Duration time.Duration

	// -- to which is the action targetted
	Component string

	// -- the type of action
	Kind string

	// -- the error in case an error occured
	Error error
}

func (a *Action) Ended(err error) {
	a.End = time.Now()
	a.Duration = a.End.Sub(a.Start)

	if err != nil {
		a.Error = err;
	}
}