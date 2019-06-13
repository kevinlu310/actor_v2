package decoder

import (
	"encoding/json"
	"errors"
)

// --- json unmarshal ---

// JSONDecoder decodes payload's json
func JSONDecoder(d []byte, payload interface{}) error {
	if json.Valid(d) {
		return json.Unmarshal(d, payload)
	}

	err := errors.New("invalid json format")
	return err
}
