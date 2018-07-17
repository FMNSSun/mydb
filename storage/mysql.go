package storage

import (
	"database/sql"
	
	. "github.com/FMNSSun/mydb"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLStorage struct {
	db *sql.DB
	logger Logger
}

func NewMySQLStorage(dsn string, logger Logger) (Storage, error) {
	db, err := sql.Open("mysql", dsn)

	if err != nil {
		logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		return nil, err
	}

	return &MySQLStorage {
		db: db,
		logger: logger,
	}, nil
}

func (ss *MySQLStorage) Put(key, value []byte) StorageError {
	stmt, err := ss.db.Prepare("REPLACE INTO tbl_kv(a_key, a_value, a_hash) values(?,?,?)")

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not prepare statement.")
	}

	hash := Hash(key)

	_, err = stmt.Exec(key, value, hash[:])

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		stmt.Close()
		return StorageErrorf2(ERR_STORAGE, err, "Could not execute statement.")
	}

	err = stmt.Close()

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not close statement.")
	}

	return nil
}

func (ss *MySQLStorage) Get(key []byte) ([]byte, StorageError) {
	stmt, err := ss.db.Prepare("SELECT a_value FROM tbl_kv WHERE a_key = ? AND a_hash = ? LIMIT 1")

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not prepare statement.")
	}

	hash := Hash(key)

	res, err := stmt.Query(key, hash[:])

	if err != nil {
		ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
		stmt.Close()
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not execute statement.")
	}

	for res.Next() {
		var data []byte
		err = res.Scan(&data)

		if err != nil {
			ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", err.Error())
			
			errOther := res.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", errOther.Error())
			}

			errOther = stmt.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", errOther.Error())
			}

			return nil, StorageErrorf2(ERR_STORAGE, err, "Could not read result.")
		} else {

			errOther := res.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", errOther.Error())
			}

			errOther = stmt.Close()

			if errOther != nil {
				ss.logger.Outf(LOGLVL_ERROR, "[MYSQLSTORAGE] ERROR: %s", errOther.Error())
			}

			return data, nil
		}
	}

	return nil, nil
}
