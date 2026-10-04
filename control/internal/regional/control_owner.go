package regional

import (
	"encoding/hex"
	"fmt"
	"io"
)

const controlOwnerNonceBytes = 16

// processControlOwnerID keeps the deployment label recognizable while binding
// ownership to one process incarnation. A pod name or hostname alone can be
// reused while a disconnected predecessor is still alive.
func processControlOwnerID(instanceID string, entropy io.Reader) (string, error) {
	if !validControlOwner(instanceID) {
		return "", fmt.Errorf("control instance ID must be a canonical 1-%d byte identifier", maxControlOwnerBytes)
	}
	var nonce [controlOwnerNonceBytes]byte
	if _, err := io.ReadFull(entropy, nonce[:]); err != nil {
		return "", fmt.Errorf("generate control process incarnation: %w", err)
	}
	prefixLimit := maxControlOwnerBytes - 1 - hex.EncodedLen(len(nonce))
	if len(instanceID) > prefixLimit {
		instanceID = instanceID[:prefixLimit]
	}
	return instanceID + "@" + hex.EncodeToString(nonce[:]), nil
}
