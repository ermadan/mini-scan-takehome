package database

import "github.com/censys/scan-takehome/pkg/scanning"

type Database interface {
	StartBatch() error
	FinishBatch() error
	WriteScan(scan *scanning.Scan) error
	PrintAllScans()
	Close()
}
