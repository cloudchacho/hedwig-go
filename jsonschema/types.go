/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package jsonschema

import (
	"strconv"
	"time"
)

// JSONTime is just a wrapper around time that serializes time to epoch in milliseconds
type JSONTime time.Time

// MarshalJSON changes time to epoch in milliseconds
func (t JSONTime) MarshalJSON() ([]byte, error) {
	epochMs := time.Time(t).UnixNano() / int64(time.Millisecond)
	return []byte(strconv.FormatInt(epochMs, 10)), nil
}

// UnmarshalJSON changes time from epoch in milliseconds
func (t *JSONTime) UnmarshalJSON(b []byte) error {
	epochMs, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	duration := time.Duration(epochMs) * time.Millisecond
	epochNS := duration.Nanoseconds()
	*t = JSONTime(time.Unix(0, epochNS))
	return nil
}
