/*
 * SQL functionalities
 *
 * Author: Vincenzo M. (2021)
 */
#include <cassert>

#include "sql.hpp"
#include "utils.hpp"

using utils::logb;
using utils::LogErr;

namespace sql {

std::string
DbSpec::Repr() const
{
	if (IsMySQL()) {
		return user + "@" + host + ":" + dbname + "." + tablename;
	}

	if (IsSQLite()) {
		return dbfile;
	}

	return std::string();
}

void
DbSpec::Dump() const
{
	if (!dbfile.empty()) {
		std::cout << "DB file: " << dbfile << std::endl;
	} else {
		std::cout << "DB host  : " << host << ":" << port << std::endl;
		std::cout << "DB name  : " << dbname << std::endl;
		std::cout << "DB user  : " << user << std::endl;
		std::cout << "DB passwd: " << password << std::endl;
		std::cout << "DB table : " << tablename << std::endl;
	}
}

bool
DbSpec::IsMySQL() const
{
	return !(host.empty() || dbname.empty() || user.empty());
}

bool
DbSpec::IsSQLite() const
{
	return !dbfile.empty();
}

SQLiteDbCursor::~SQLiteDbCursor()
{
	if (stmt != nullptr) {
		int ret = sqlite3_finalize(stmt);

		if (ret != SQLITE_OK) {
			std::cerr << logb(LogErr)
				  << "Failed to finalize cursor statement: "
				  << sqlite3_errmsg(dbh) << std::endl;
		}
	}
}

int
SQLiteDbCursor::NextRow()
{
	int ret = sqlite3_step(stmt);

	row_valid = false; /* Reset the flag. */

	switch (ret) {
	case SQLITE_ROW:
		/* A row is available. */
		row_valid = true;
		return 1;
		break;

	case SQLITE_DONE:
		/* No more rows are available. */
		return 0;
		break;

	default:
		std::cerr << logb(LogErr)
			  << "Failed to step statement: " << sqlite3_errmsg(dbh)
			  << std::endl;
		return -1;
		break;
	}

	return -1; /* Not reachable. */
}

bool
SQLiteDbCursor::RowColumnCheck(unsigned int idx)
{
	if (!row_valid) {
		std::cerr << logb(LogErr) << "No row is available" << std::endl;
		return false;
	}

	if (idx >= static_cast<unsigned int>(sqlite3_column_count(stmt))) {
		std::cerr << logb(LogErr) << "Field index " << idx
			  << " out of range" << std::endl;
		return false;
	}

	return true;
}

bool
SQLiteDbCursor::RowColumn(unsigned int idx, int &val, bool mayfail)
{
	if (!RowColumnCheck(idx)) {
		assert(mayfail);
		return false;
	}

	val = sqlite3_column_int(stmt, idx);

	return true;
}

bool
SQLiteDbCursor::RowColumn(unsigned int idx, std::string &s, bool mayfail)
{
	if (!RowColumnCheck(idx)) {
		assert(mayfail);
		return false;
	}

	std::stringstream ss;

	ss << sqlite3_column_text(stmt, idx);
	s = ss.str();

	return true;
}

std::unique_ptr<DbConn>
SQLiteDbConn::Create(const std::string &dbfile)
{
	sqlite3 *pdbh;
	int ret;

	ret = sqlite3_open_v2(dbfile.c_str(), &pdbh,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      nullptr);
	if (ret != SQLITE_OK) {
		std::cerr << logb(LogErr) << "Failed to open " << dbfile << ": "
			  << sqlite3_errmsg(pdbh) << std::endl;
		return nullptr;
	}

	return std::make_unique<SQLiteDbConn>(pdbh);
}

SQLiteDbConn::~SQLiteDbConn()
{
	if (dbh != nullptr) {
		sqlite3_close(dbh);
	}
}

int
SQLiteDbConn::ModifyStmt(const std::stringstream &ss, int verbose)
{
	if (verbose) {
		std::cout << "Q: " << ss.str() << std::endl;
	}

	sqlite3_stmt *pstmt;
	int ret = sqlite3_prepare_v2(dbh, ss.str().c_str(), /*nByte=*/-1,
				     &pstmt, /*pzTail=*/nullptr);

	if (ret != SQLITE_OK) {
		std::cerr << logb(LogErr) << "Failed to prepare statement "
			  << ss.str() << ": " << sqlite3_errmsg(dbh)
			  << std::endl;
		return -1;
	}

	ret = sqlite3_step(pstmt);
	assert(ret != SQLITE_ROW); /* no results expected */

	if (ret != SQLITE_DONE) {
		std::cerr << logb(LogErr) << "Failed to evaluate statement "
			  << ss.str() << ": " << sqlite3_errmsg(dbh)
			  << std::endl;
	}

	int fret = sqlite3_finalize(pstmt);
	if (ret == SQLITE_DONE && fret != SQLITE_OK) {
		std::cerr << logb(LogErr) << "Failed to finalize statement "
			  << ss.str() << ": " << sqlite3_errmsg(dbh)
			  << std::endl;
	}

	return fret == SQLITE_OK ? 0 : -1;
}

std::unique_ptr<DbCursor>
SQLiteDbConn::SelectStmt(const std::stringstream &ss, int verbose)
{
	if (verbose) {
		std::cout << "Q: " << ss.str() << std::endl;
	}

	sqlite3_stmt *pstmt;
	int ret = sqlite3_prepare_v2(dbh, ss.str().c_str(), /*nByte=*/-1,
				     &pstmt, /*pzTail=*/nullptr);

	if (ret != SQLITE_OK) {
		std::cerr << logb(LogErr) << "Failed to prepare statement "
			  << ss.str() << ": " << sqlite3_errmsg(dbh)
			  << std::endl;
		return nullptr;
	}

	// TODO reference count the cursors ?
	return std::make_unique<SQLiteDbCursor>(dbh, pstmt);
}

MySQLDbCursor::~MySQLDbCursor()
{
	if (qres) {
		mysql_free_result(qres);
	}
}

int
MySQLDbCursor::NextRow()
{
	row = mysql_fetch_row(qres);

	return (row != nullptr) ? 1 : 0;
}

bool
MySQLDbCursor::RowColumnCheck(unsigned int idx)
{
	if (row == nullptr || qres == nullptr) {
		std::cerr << logb(LogErr) << "No row is available" << std::endl;
		return false;
	}

	if (idx >= static_cast<unsigned int>(mysql_num_fields(qres))) {
		std::cerr << logb(LogErr) << "Field index " << idx
			  << " out of range" << std::endl;
		return false;
	}

	return row[idx] != nullptr;
}

bool
MySQLDbCursor::RowColumn(unsigned int idx, int &val, bool mayfail)
{
	if (!RowColumnCheck(idx)) {
		assert(mayfail);
		return false;
	}

	return utils::Str2Num<int>(std::string(row[idx]), val);
}

bool
MySQLDbCursor::RowColumn(unsigned int idx, std::string &s, bool mayfail)
{
	if (!RowColumnCheck(idx)) {
		assert(mayfail);
		return false;
	}

	s = std::string(row[idx]);

	return true;
}

std::unique_ptr<DbConn>
MySQLDbConn::Create(const DbSpec &dbspec)
{
	MYSQL *dbc = mysql_init(nullptr);

	if (dbc == nullptr) {
		std::cerr << logb(LogErr)
			  << "Failed to initialize MySQL client: "
			  << mysql_error(dbc) << std::endl;
		return nullptr;
	}
	if (mysql_real_connect(dbc, dbspec.host.c_str(), dbspec.user.c_str(),
			       dbspec.password.c_str(), dbspec.dbname.c_str(),
			       0, NULL, 0) == nullptr) {
		std::cerr << logb(LogErr)
			  << "Failed to connect to DB: " << mysql_error(dbc)
			  << std::endl;
		return nullptr;
	}

	/* Note: autocommit is on by default. */

	my_bool reconnect = 1;
	if (mysql_options(dbc, MYSQL_OPT_RECONNECT, &reconnect)) {
		std::cerr << logb(utils::LogWrn)
			  << "Failed to set MYSQL_OPT_RECONNECT" << std::endl;
	}

	if (true) {
		std::cout << "Connected to MySQL server "
			  << mysql_get_server_info(dbc) << ", "
			  << mysql_get_host_info(dbc) << std::endl;
	}

	return std::make_unique<MySQLDbConn>(dbc);
}

MySQLDbConn::~MySQLDbConn()
{
	if (dbc != nullptr) {
		mysql_close(dbc);
	}
}

int
MySQLDbConn::QueryReconnect(const std::stringstream &ss, int verbose)
{
	if (verbose) {
		std::cout << "Q: " << ss.str() << std::endl;
	}

	for (int retry = 0; retry < 2; retry++) {
		int ret = mysql_query(dbc, ss.str().c_str());

		if (ret == 0) {
			/* Query was successful. We can return. */
			return 0;
		}

		if (retry > 0) {
			/* Second query failure in a row. Give up. */
			break;
		}

		/*
		 * An error occurred, maybe because the connection with the
		 * server went down. We therefore try to ping the server,
		 * because a ping will trigger a reconnection (if needed).
		 */
		if (mysql_ping(dbc)) {
			/* The reconnect failed. Give up. */
			break;
		}

		/* We reconnected successfully. Retry the query again. */
	}

	std::cerr << logb(LogErr) << "Query failed: " << mysql_error(dbc)
		  << std::endl;

	return -1;
}

int
MySQLDbConn::ModifyStmt(const std::stringstream &ss, int verbose)
{
	return QueryReconnect(ss, verbose);
}

std::unique_ptr<DbCursor>
MySQLDbConn::SelectStmt(const std::stringstream &ss, int verbose)
{
	int ret = QueryReconnect(ss, verbose);

	if (ret) {
		return nullptr;
	}

	/* See mysql_store_result() vs mysql_use_result(). */
	MYSQL_RES *qres = mysql_store_result(dbc);
	if (qres == nullptr) {
		std::cerr << logb(LogErr)
			  << "mysql_store_result() failed: " << mysql_error(dbc)
			  << std::endl;
		return nullptr;
	}

	return std::make_unique<MySQLDbCursor>(dbc, qres);
}

} // namespace sql
