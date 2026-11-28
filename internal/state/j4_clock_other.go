//go:build !linux

package state

import "errors"

func newJ4HostClock() (string, func() (int64, error), error) {
	return "", nil, errors.New("J4 provider control requires the shared Linux host clock")
}
