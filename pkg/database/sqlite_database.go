package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/censys/scan-takehome/pkg/scanning"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

type SQLiteDatabase struct {
	db   *sql.DB
	tx   *sql.Tx
	stmt *sql.Stmt
}

func NewDatabase(dataSourceName string) (Database, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}

	sqlStmt := `
	create table if not exists scans (
		ip text,
		port integer,
		service text,
		timestamp integer,
		data_version integer,
		response text,
		PRIMARY KEY (ip, port, service)
	);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}

	return &SQLiteDatabase{db: db}, nil
}

func (d *SQLiteDatabase) StartBatch() error {
	var err error
	d.tx, err = d.db.Begin()
	if err != nil {
		return err
	}

	d.stmt, err = d.tx.Prepare(`insert into scans(ip, port, service, timestamp, data_version, response) 
		values(?, ?, ?, ?, ?, ?)
		on conflict(ip, port, service) do 
		update set
			timestamp  = excluded.timestamp,
			response    = excluded.response,
			data_version = excluded.data_version
		where excluded.timestamp > scans.timestamp;		`)
	return err
}

func (d *SQLiteDatabase) WriteScan(scan *scanning.Scan) error {
	response, err := scanning.ToString(scan)
	if err != nil {
		return err
	}
	_, err = d.stmt.Exec(scan.Ip, scan.Port, scan.Service, scan.Timestamp, scan.DataVersion, response)
	return err
}

func (d *SQLiteDatabase) FinishBatch() error {
	if err := d.stmt.Close(); err != nil {
		log.Printf("Error closing stmt, %v", err)
	}
	return d.tx.Commit()
}

func (d *SQLiteDatabase) Close() {
	d.db.Close()
}

func (d *SQLiteDatabase) PrintAllScans() {
	rows, err := d.db.Query("SELECT ip, port, service, timestamp, data_version, response FROM scans ORDER BY timestamp DESC limit 20")
	if err != nil {
		log.Fatalf("Failed to query scans: %v", err)
	}
	defer rows.Close()

	fmt.Println("--- Stored Scans ---")
	for rows.Next() {
		var (
			ip          string
			port        uint32
			service     string
			timestamp   int64
			dataVersion int
			response    string
		)
		if err := rows.Scan(&ip, &port, &service, &timestamp, &dataVersion, &response); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("IP: %s, Port: %d, Service: %s, Timestamp: %s, Data Version: %d, Data: %s\n",
			ip, port, service, time.Unix(timestamp, 0).Format(time.RFC3339), dataVersion, response)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating through rows: %v", err)
	}
}
