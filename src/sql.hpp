/*
 * Author: Vincenzo M. (2021)
 */

#ifndef __SQL_HPP__
#define __SQL_HPP__

#include <iostream>
#include <memory>
#include <mysql/mysql.h>
#include <sqlite3.h>
#include <string>
#include <sstream>

namespace sql {

/*
 * Support for database access.
 */

struct DbSpec {
	/* In case of SQLite. */
	std::string dbfile;

	/* In case of MySQL. */
	std::string host;
	unsigned short port = 3306;
	std::string dbname;
	std::string user;
	std::string password;
	std::string tablename;

	std::string Repr() const;
	void Dump() const;
	bool IsMySQL() const;
	bool IsSQLite() const;
};

class DbCursor {
    public:
	virtual int NextRow()			      = 0;
	virtual bool RowColumnCheck(unsigned int idx) = 0;
	virtual bool RowColumn(unsigned int idx, int &val,
			       bool mayfail = false)  = 0;
	virtual bool RowColumn(unsigned int idx, std::string &s,
			       bool mayfail = false)  = 0;
	virtual ~DbCursor() {}
};

class DbConn {
    public:
	virtual int ModifyStmt(const std::stringstream &ss, int verbose) = 0;
	virtual std::unique_ptr<DbCursor> SelectStmt(
	    const std::stringstream &ss, int verbose) = 0;
	virtual ~DbConn() {}
	virtual std::string CurrentUnixTime() const = 0;
};

/*
 * SQLite3 database support.
 */

class SQLiteDbCursor : public DbCursor {
	sqlite3 *dbh	   = nullptr;
	sqlite3_stmt *stmt = nullptr;
	bool row_valid	   = false;

    public:
	SQLiteDbCursor(sqlite3 *dbh, sqlite3_stmt *stmt) : dbh(dbh), stmt(stmt)
	{
	}
	~SQLiteDbCursor();
	int NextRow();
	bool RowColumnCheck(unsigned int idx);
	bool RowColumn(unsigned int idx, int &val, bool mayfail = false);
	bool RowColumn(unsigned int idx, std::string &s, bool mayfail = false);
};

class SQLiteDbConn : public DbConn {
	sqlite3 *dbh = nullptr;

    public:
	static std::unique_ptr<DbConn> Create(const std::string &dbfile);
	SQLiteDbConn(sqlite3 *dbh) : dbh(dbh) {}
	~SQLiteDbConn();

	int ModifyStmt(const std::stringstream &ss, int verbose);
	std::unique_ptr<DbCursor> SelectStmt(const std::stringstream &ss,
					     int verbose);
	std::string CurrentUnixTime() const { return "strftime('%s', 'now')"; }
};

/*
 * MySQL/mariadb database support.
 */

class MySQLDbCursor : public DbCursor {
	MYSQL *dbc	= nullptr;
	MYSQL_RES *qres = nullptr;
	MYSQL_ROW row	= nullptr;

    public:
	MySQLDbCursor(MYSQL *dbc, MYSQL_RES *qres) : dbc(dbc), qres(qres) {}
	~MySQLDbCursor();
	int NextRow();
	bool RowColumnCheck(unsigned int idx);
	bool RowColumn(unsigned int idx, int &val, bool mayfail = false);
	bool RowColumn(unsigned int idx, std::string &s, bool mayfail = false);
};

class MySQLDbConn : public DbConn {
	MYSQL *dbc = nullptr;

	int QueryReconnect(const std::stringstream &ss, int verbose);

    public:
	static std::unique_ptr<DbConn> Create(const DbSpec &dbspec);
	MySQLDbConn(MYSQL *dbc) : dbc(dbc) {}
	~MySQLDbConn();

	int ModifyStmt(const std::stringstream &ss, int verbose);
	std::unique_ptr<DbCursor> SelectStmt(const std::stringstream &ss,
					     int verbose);
	std::string CurrentUnixTime() const { return "UNIX_TIMESTAMP(NOW())"; }
};

} // namespace sql

#endif /* __SQL_HPP__ */
