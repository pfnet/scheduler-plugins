package gang

import (
	"io"
	"net/http"
)

type gangHealthServer struct {
	gangs *Gangs
}

func newGangHealthServer(gangs *Gangs) *gangHealthServer {
	return &gangHealthServer{
		gangs: gangs,
	}
}

func (ghs *gangHealthServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check if gangs.mapLock can be locked and unlocked.
	ghs.gangs.mapLock.Lock()
	ghs.gangs.mapLock.Unlock() // nolint:staticcheck
	if _, err := io.WriteString(w, "gangs.mapLock is fine\n"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Check if gangs.activateGangsPoolLock can be locked and unlocked.
	ghs.gangs.activateGangsPoolLock.Lock()
	ghs.gangs.activateGangsPoolLock.Unlock() // nolint:staticcheck
	if _, err := io.WriteString(w, "gangs.activateGangsPoolLock is fine\n"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Check if gangs.podUIDsLock can be locked and unlocked.
	ghs.gangs.podUIDsLock.Lock()
	ghs.gangs.podUIDsLock.Unlock() // nolint:staticcheck
	if _, err := io.WriteString(w, "gangs.podUIDsLock is fine\n"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)
}
