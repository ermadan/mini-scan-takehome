package scanning

import (
	"encoding/json"
	"fmt"
)

// additional marshal/unmarshal is suboptimal provided we are developing high thoughput solution
func ToString(scan *Scan) (string, error) {
	data, err := json.Marshal(scan.Data)
	if err != nil {
		return "", err
	}

	switch scan.DataVersion {
	case V1:
		var v1 V1Data
		if err := json.Unmarshal(data, &v1); err == nil && len(v1.ResponseBytesUtf8) > 0 {
			return string(v1.ResponseBytesUtf8), nil
		}
	case V2:
		var v2 V2Data
		if err := json.Unmarshal(data, &v2); err == nil && v2.ResponseStr != "" {
			return v2.ResponseStr, nil
		}
	}
	return "", fmt.Errorf("unknown data version: %d", scan.DataVersion)
}
