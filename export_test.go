package kagedcap

import "time"

// SetLegacyRetryWindow shrinks the legacy /solve retry window for a test, so the guard can be
// provoked without waiting 100s, and returns the call that restores it.
func SetLegacyRetryWindow(d time.Duration) (restore func()) {
	prev := legacyRetryWindow
	legacyRetryWindow = d
	return func() { legacyRetryWindow = prev }
}
