package core

import (
	"time"
	"github.com/sirupsen/logrus"
)

func NewAction(app string, component string, kind string) *Action {
	return &Action{
		Application: app,
		Timestamp: time.Now().UTC(),
		Component: component,
		Kind: kind,
	};
}

type Action struct {
	// -- the name of the service
	Application string

	// -- the moment at which the action occured
	Timestamp time.Time

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
	a.Duration = a.End.Sub(a.Timestamp)

	if err != nil {
		a.Error = err;
		logrus.Warn("Action '" + a.Kind + "' Ended: ", err.Error());
	}
}