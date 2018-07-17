package storage

import (
	"database/sql"
	
	. "github.com/FMNSSun/mydb"
	_ "github.com/mattn/go-sqlite3"
)

type SqliteStorage struct {
	db *sql.DB
	logger Logger
}

func NewSqliteStorage(dsn string, logger Logger) (Storage, error) {
	db, err := sql.Open("sqlite3", dsn)

	if err != nil {
		logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		return nil, err
	}

	return &SqliteStorage {
		db: db,
		logger: logger,
	}, nil
}

func (ss *SqliteStorage) Put(key, value []byte) StorageError {
	stmt, err := ss.db.Prepare("INSERT OR REPLACE INTO tbl_kv(a_key, a_value) values(?,?)")

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not prepare statement.")
	}

	_, err = stmt.Exec(key, value)

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		stmt.Close()
		return StorageErrorf2(ERR_STORAGE, err, "Could not execute statement.")
	}

	err = stmt.Close()

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not close statement.")
	}

	return nil
}

func (ss *SqliteStorage) Get(key []byte) ([]byte, StorageError) {
	stmt, err := ss.db.Prepare("SELECT a_value FROM tbl_kv WHERE a_key = ? LIMIT 1")

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not prepare statement.")
	}

	res, err := stmt.Query(key)

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
		stmt.Close()
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not execute statement.")
	}

	for res.Next() {
		var data []byte
		err = res.Scan(&data)

		if err != nil {
			ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", err.Error())
			
			errOther := res.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", errOther.Error())
			}

			errOther = stmt.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", errOther.Error())
			}

			return nil, StorageErrorf2(ERR_STORAGE, err, "Could not read result.")
		} else {

			errOther := res.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", errOther.Error())
			}

			errOther = stmt.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[SQLITESTORAGE] ERROR: %s", errOther.Error())
			}

			return data, nil
		}
	}

	return nil, nil
}
