package scanning

import (
	"testing"
)

func TestScan_ToString(t *testing.T) {
	tests := []struct {
		name    string
		scan    Scan
		want    string
		wantErr bool
	}{
		{
			name: "V1_ValidData",
			scan: Scan{
				Ip:          "127.0.0.1",
				Port:        80,
				Service:     "HTTP",
				Timestamp:   1678886400,
				DataVersion: V1,
				Data:        &V1Data{ResponseBytesUtf8: []byte("hello world V1")},
			},
			want:    "hello world V1",
			wantErr: false,
		},
		{
			name: "V2_ValidData",
			scan: Scan{
				Ip:          "192.168.1.1",
				Port:        22,
				Service:     "SSH",
				Timestamp:   1678886400,
				DataVersion: V2,
				Data:        &V2Data{ResponseStr: "hello world V2"},
			},
			want:    "hello world V2",
			wantErr: false,
		},
		{
			name: "UnknownVersion",
			scan: Scan{
				DataVersion: 99, // Unknown version
				Data:        nil,
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToString(&tt.scan)
			if (err != nil) != tt.wantErr {
				t.Errorf("Scan.ToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Scan.ToString() got = %v, want %v", got, tt.want)
			}
		})
	}
}
