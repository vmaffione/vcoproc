#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <curl/curl.h>
#include <deque>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <poll.h>
#include <signal.h>
#include <sqlite3.h>
#include <sstream>
#include <string>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "utils.hpp"
#include "json11.hpp"
#include "version.h"

using namespace utils;

namespace {

void
Usage(const char *progname)
{
	std::cout
	    << progname << " usage:" << std::endl
	    << "    -h (show this help and exit)" << std::endl
	    << "    -V (print version info and exit)" << std::endl
	    << "    -v (increase verbosity level)" << std::endl
	    << "    -s SOURCE_NAME (origin name)" << std::endl
	    << "    -i INPUT_DIR (add directory to look for files)" << std::endl
	    << "    -o OUTPUT_DIR (directory where to store "
	       "JSON processing output)"
	    << std::endl
	    << "    -F FAILED_DIR (directory where to store failed files)"
	    << std::endl
	    << "    -f FORWARD_DIR (directory where to move processed files)"
	    << std::endl
	    << "    -D DB_FILE (path to the sqlite3 database file)" << std::endl
	    << "    -c (consume input files)" << std::endl
	    << "    -m (monitor input directories rather than stop when "
	       "running out of files)"
	    << std::endl
	    << "    -H BACKEND_HOST (address or name of the backend engine)"
	    << std::endl
	    << "    -p BACKEND_PORT (TCP port of the backend engine)"
	    << std::endl;
}

void
PrintVersionInfo()
{
	std::cout << "Version info:" << std::endl
		  << "    release version  : " << VC_VERSION << std::endl
		  << "    build id         : " << VC_REVISION_ID << std::endl
		  << "    build date       : " << VC_REVISION_DATE << std::endl;
}

float
MsecsElapsed(std::chrono::time_point<std::chrono::system_clock> tstart)
{
	auto tend  = std::chrono::system_clock::now();
	auto usecs = std::chrono::duration_cast<std::chrono::microseconds>(
	    tend - tstart);

	return usecs.count() / 1000.0;
}

float
SecsElapsed(std::chrono::time_point<std::chrono::system_clock> tstart)
{
	return MsecsElapsed(tstart) / 1000.0;
}

UniqueFd stopfd_global;

void
SigintHandler(int signum)
{
	int efd = stopfd_global;

	if (efd >= 0) {
		EventFdSignal(efd);
	}
}

} // namespace

class SQLiteDbCursor {
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

class SQLiteDbConn {
	sqlite3 *dbh = nullptr;

    public:
	static std::unique_ptr<SQLiteDbConn> Create(const std::string &dbfile);
	SQLiteDbConn(sqlite3 *dbh) : dbh(dbh) {}
	~SQLiteDbConn();

	int ModifyStmt(const std::stringstream &ss, int verbose);
	std::unique_ptr<SQLiteDbCursor> SelectStmt(const std::stringstream &ss,
						   int verbose);
};

std::unique_ptr<SQLiteDbConn>
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

std::unique_ptr<SQLiteDbCursor>
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

enum class ProcStatus {
	None	    = 0,
	New	    = 1,
	Waiting	    = 2,
	ProcSuccess = 3,
	ProcFailure = 4,
	Complete    = 5,
};

/*
 * A class representing an entry in the pending table.
 */
class PendingFile {
    public:
	enum class CurlStatus {
		Idle	 = 0,
		Prepared = 1,
	};

    private:
	CURLM *curlm = nullptr;
	CURL *curl   = nullptr;
	std::string src_path;
	size_t src_size = 0;
	int verbose	= 0;
	std::string postdata;
	std::chrono::time_point<std::chrono::system_clock> last_activity;

	CurlStatus curl_status = CurlStatus::Idle;
	ProcStatus proc_status = ProcStatus::New;
	std::stringstream postresp;

    public:
	PendingFile(CURLM *curlm, CURL *curl, const std::string &src_path,
		    size_t src_size, int verbose)
	    : curlm(curlm),
	      curl(curl),
	      src_path(src_path),
	      src_size(src_size),
	      verbose(verbose),
	      last_activity(std::chrono::system_clock::now())
	{
	}

	~PendingFile();

	ProcStatus Status() const { return proc_status; }
	void SetStatus(ProcStatus status);
	std::string FilePath() const { return src_path; }
	size_t FileSize() const { return src_size; }
	static std::unique_ptr<PendingFile> Create(CURLM *curlm,
						   const std::string &src_path,
						   int verbose);
	static size_t CurlWriteCallback(void *data, size_t size, size_t nmemb,
					void *userp);
	void AppendResponse(void *data, size_t size);
	int PreparePost(const std::string &url, const json11::Json &jsreq);
	int CompletePost(json11::Json::object &jsresp);
	size_t InactivitySeconds() const { return SecsElapsed(last_activity); }
};

std::unique_ptr<PendingFile>
PendingFile::Create(CURLM *curlm, const std::string &src_path, int verbose)
{
	CURLcode cc;
	CURL *curl;

	if (curlm == nullptr) {
		return nullptr;
	}

	curl = curl_easy_init();
	if (curl == nullptr) {
		std::cerr << logb(LogErr) << "Failed to create CURL easy handle"
			  << std::endl;
		return nullptr;
	}

	/* Set our write callback. */
	cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
			      &PendingFile::CurlWriteCallback);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_WRITEFUNCTION: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	/* Add this handle to our multi stack. */
	CURLMcode cm = curl_multi_add_handle(curlm, curl);
	if (cm != CURLM_OK) {
		std::cerr << "Failed to add handle to multi stack: "
			  << curl_multi_strerror(cm) << std::endl;
		return nullptr;
	}

	long long int src_size = utils::FileSize(src_path);
	if (src_size < 0) {
		return nullptr;
	}

	auto proc = std::make_unique<PendingFile>(
	    curlm, curl, src_path, static_cast<size_t>(src_size), verbose);

	/* Link the new PendingFile instance to the curl handle. */
	cc = curl_easy_setopt(curl, CURLOPT_PRIVATE, (void *)proc.get());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_PRIVATE: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	/* Link the new PendingFile instance to our write callback. */
	cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)proc.get());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_WRITEDATA: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	return proc;
}

PendingFile::~PendingFile()
{
	if (curl != nullptr) {
		CURLMcode cm;

		assert(curlm != nullptr);
		cm = curl_multi_remove_handle(curlm, curl);
		if (cm != CURLM_OK) {
			std::cerr
			    << "Failed to remove handle from multi stack: "
			    << curl_multi_strerror(cm) << std::endl;
		}
		curl_easy_cleanup(curl);
	}
}

size_t
PendingFile::CurlWriteCallback(void *data, size_t size, size_t nmemb,
			       void *userp)
{
	size_t chunksz	  = size * nmemb;
	PendingFile *proc = reinterpret_cast<PendingFile *>(userp);

	assert(proc != nullptr);
	proc->AppendResponse(data, chunksz);

	return chunksz;
}

void
PendingFile::SetStatus(ProcStatus status)
{
	if (verbose >= 2) {
		std::cout << logb(LogDbg) << src_path << ": "
			  << static_cast<int>(proc_status) << " -> "
			  << static_cast<int>(status) << std::endl;
	}
	proc_status = status;
}

void
PendingFile::AppendResponse(void *data, size_t size)
{
	postresp.write((const char *)data, size);
	last_activity = std::chrono::system_clock::now();
}

/* Prepare a post request without performing it. */
int
PendingFile::PreparePost(const std::string &url, const json11::Json &jsreq)
{
	CURLcode cc;

	if (curl_status != CurlStatus::Idle) {
		std::cerr << "CURL status (" << static_cast<int>(curl_status)
			  << ") is not idle" << std::endl;
		return -1;
	}

	cc = curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_URL: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	}

	/*
	 * The data passed to CURLOPT_POSTFIELDS is not copied by libcurl
	 * (by default), and therefore we must preserve it until the
	 * end of the POST transfer.
	 */
	postdata = jsreq.dump();
	cc	 = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postdata.c_str());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_POSTFIELDS: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	}

	/*
	 * If we don't provide POSTFIELDSIZE, libcurl will strlen() by
	 * itself.
	 */
	cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postdata.size());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_POSTFIELDSIZE: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	}

	curl_status   = CurlStatus::Prepared;
	last_activity = std::chrono::system_clock::now();
#if 0
	cc = curl_easy_perform(curl);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to perform request: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	} else {
		long code = 0;
		cc = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (cc != CURLE_OK) {
			std::cerr << "Failed to get CURLINFO_RESPONSE_CODE: "
				<< curl_easy_strerror(cc) << std::endl;
			return -1;
		}
		std::cout << "REQUEST DONE: " << code << std::endl;
	}
#endif

	return 0;
}

int
PendingFile::CompletePost(json11::Json::object &jsresp)
{
	std::string respstr;
	std::string errs;

	if (curl_status != CurlStatus::Prepared) {
		std::cerr << logb(LogErr)
			  << "Cannot compete POST: Invalid state "
			  << static_cast<int>(curl_status) << std::endl;
		return -1;
	}

	respstr = postresp.str();
	if (respstr.empty()) {
		std::cerr << logb(LogErr) << "Response is empty" << std::endl;
		return -1;
	}

	json11::Json js = json11::Json::parse(respstr, errs);
	if (!errs.empty() && js == json11::Json()) {
		std::cerr << logb(LogErr)
			  << "Response is not a JSON: " << respstr << std::endl;
		return -1;
	}
	jsresp = js.object_items();

	/* Reset CURL status to allow more requests. */
	curl_status   = CurlStatus::Idle;
	postresp      = std::stringstream();
	last_activity = std::chrono::system_clock::now();

	return 0;
}

class Backend {
    public:
	virtual bool Probe()		    = 0;
	virtual std::string BaseUrl() const = 0;
};

class PsBackend : public Backend {
	std::string base_url;

    public:
	static std::unique_ptr<Backend> Create(std::string host,
					       unsigned short port);
	PsBackend(std::string base_url);
	virtual bool Probe();

	std::string BaseUrl() const { return base_url; }
};

std::unique_ptr<Backend>
PsBackend::Create(std::string host, unsigned short port)
{
	std::stringstream base_url;

	base_url << "http://" << host << ":" << port;

	return std::make_unique<PsBackend>(base_url.str());
}

PsBackend::PsBackend(std::string base_url) : base_url(base_url) {}

bool
PsBackend::Probe()
{
	std::string url = base_url + std::string("/ping");
	long http_code	= 0;
	CURLcode cc;
	CURL *curl;

	curl = curl_easy_init();
	if (curl == nullptr) {
		std::cerr << logb(LogErr) << "Failed to create CURL easy handle"
			  << std::endl;
		return false;
	}

	/* Write callback to push data to a local stringstream variable. */
	std::stringstream getresp;
	auto writef = [](void *data, size_t size, size_t nitems, void *userp) {
		std::stringstream *getresp = (std::stringstream *)userp;
		getresp->write((const char *)data, size * nitems);
		return size * nitems;
	};

	cc = curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_URL: "
			  << curl_easy_strerror(cc) << std::endl;
		goto end;
	}

	cc = curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_HTTPGET: "
			  << curl_easy_strerror(cc) << std::endl;
		goto end;
	}

	/*
	 * The "+" magic forces a conversion to a C-style function pointer.
	 * The writef function cannot have captures.
	 */
	cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +writef);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_READFUNCTION: "
			  << curl_easy_strerror(cc) << std::endl;
		goto end;
	}

	cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&getresp);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_READDATA: "
			  << curl_easy_strerror(cc) << std::endl;
		goto end;
	}

	cc = curl_easy_perform(curl);
	if (cc != CURLE_OK) {
		std::cerr << "Failed to perform request: "
			  << curl_easy_strerror(cc) << std::endl;
		goto end;
	} else {
		cc =
		    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
		if (cc != CURLE_OK) {
			std::cerr << "Failed to get CURLINFO_RESPONSE_CODE: "
				  << curl_easy_strerror(cc) << std::endl;
			goto end;
		}
	}

end:
	curl_easy_cleanup(curl);

	return http_code == 200;
}

/* Main class. */
class VCoproc {
	int stopfd   = -1; /* owned by the caller, not by us */
	CURLM *curlm = nullptr;
	std::unique_ptr<Backend> be;
	int verbose  = 0;
	bool consume = false;
	bool monitor = false;
	std::string source;
	std::vector<std::string> input_dirs;
	std::string output_dir;
	std::string failed_dir;
	std::string forward_dir;
	std::string dbfile;
	std::unique_ptr<SQLiteDbConn> dbconn;
	std::string host;
	unsigned short port  = 0;
	size_t input_dir_idx = 0;

	struct {
		uint64_t files_scored	 = 0;
		uint64_t bytes_scored	 = 0;
		uint64_t files_nomdata	 = 0;
		uint64_t bytes_nomdata	 = 0;
		uint64_t files_failed	 = 0;
		uint64_t bytes_failed	 = 0;
		uint64_t files_timedout	 = 0;
		uint64_t bytes_timedout	 = 0;
		uint64_t files_completed = 0;
		uint64_t bytes_completed = 0;
	} stats;
	std::chrono::time_point<std::chrono::system_clock> stats_start;

	/*
	 * Max number of in progress entries that we allow in the
	 * pending table at any time.
	 */
	static constexpr size_t MaxEntries = 5;

	/* Map of in-progress proc entries. */
	std::unordered_map<std::string, std::unique_ptr<PendingFile>> pending;

	int FetchMoreFiles();
	int FetchFilesFromDir(const std::string &dir,
			      std::deque<std::string> &frontier, int &credits);
	int CleanupCompleted();
	int TimeoutWaiting();
	int UpdateStatistics();
	int WaitForBackend();

    public:
	static std::unique_ptr<VCoproc> Create(
	    int stopfd, int verbose, bool consume, bool monitor,
	    std::string source, std::vector<std::string> input_dirs,
	    std::string output_dir, std::string failed_dir,
	    std::string forward_dir, std::string dbfile, std::string host,
	    unsigned short port);

	VCoproc(int stopfd, CURLM *curlm, std::unique_ptr<Backend>, int verbose,
		bool consume, bool monitor, std::string source,
		std::vector<std::string> input_dirs, std::string output_dir,
		std::string failed_dir, std::string forward_dir,
		std::string dbfile, std::unique_ptr<SQLiteDbConn> dbconn,
		std::string host, unsigned short port);
	~VCoproc();
	int MainLoop();
};

std::unique_ptr<VCoproc>
VCoproc::Create(int stopfd, int verbose, bool consume, bool monitor,
		std::string source, std::vector<std::string> input_dirs,
		std::string output_dir, std::string failed_dir,
		std::string forward_dir, std::string dbfile, std::string host,
		unsigned short port)
{
	if (source.empty()) {
		std::cerr << logb(LogErr) << "No source/origin specified"
			  << std::endl;
		return nullptr;
	}

	if (input_dirs.empty()) {
		std::cerr << logb(LogErr) << "No input directories specified"
			  << std::endl;
		return nullptr;
	}

	if (output_dir.empty()) {
		std::cerr << logb(LogErr) << "No output directory specified"
			  << std::endl;
		return nullptr;
	}

	if (failed_dir.empty()) {
		std::cerr << logb(LogErr) << "No failed directory specified"
			  << std::endl;
		return nullptr;
	}

	if (dbfile.empty()) {
		std::cerr << logb(LogErr) << "No database file specified"
			  << std::endl;
		return nullptr;
	}

	if (host.empty()) {
		std::cerr << logb(LogErr) << "No hostname specified"
			  << std::endl;
		return nullptr;
	}

	if (port == 0) {
		std::cerr << logb(LogErr) << "No port specified" << std::endl;
		return nullptr;
	}

	if (monitor) {
		/* We must consume in monitor mode. */
		consume = true;
	}

	auto be = PsBackend::Create(host, port);
	if (be == nullptr) {
		std::cerr << logb(LogErr) << "Failed to create backend"
			  << std::endl;
		return nullptr;
	}

	/* Open a (long-lived) database connection. */
	auto dbconn = SQLiteDbConn::Create(dbfile);
	if (dbconn == nullptr) {
		std::cerr << logb(LogErr) << "Failed to connect to database "
			  << dbfile << std::endl;
		return nullptr;
	}

	/* Create the proc and stats tables if they do not exist already. */
	{
		std::stringstream qss;
		qss << "CREATE TABLE IF NOT EXISTS proc ("
		    << "src_path VARCHAR(255) PRIMARY KEY NOT NULL, "
		    << "status TINYINT NOT NULL, "
		    << "mjson TEXT)";
		if (dbconn->ModifyStmt(qss, verbose)) {
			return nullptr;
		}
	}
	{
		std::stringstream qss;
		qss << "CREATE TABLE IF NOT EXISTS stats ("
		    << "timestamp UNSIGNED INTEGER PRIMARY KEY NOT NULL, "
		    << "files_scored UNSIGNED INTEGER NOT NULL, "
		    << "bytes_scored UNSIGNED INTEGER NOT NULL, "
		    << "files_nomdata UNSIGNED INTEGER NOT NULL, "
		    << "bytes_nomdata UNSIGNED INTEGER NOT NULL, "
		    << "files_failed UNSIGNED INTEGER NOT NULL, "
		    << "bytes_failed UNSIGNED INTEGER NOT NULL, "
		    << "files_timedout UNSIGNED INTEGER NOT NULL, "
		    << "bytes_timedout UNSIGNED INTEGER NOT NULL, "
		    << "files_completed UNSIGNED INTEGER NOT NULL, "
		    << "bytes_completed UNSIGNED INTEGER NOT NULL"
		    << ")";
		if (dbconn->ModifyStmt(qss, verbose)) {
			return nullptr;
		}
	}

	CURLM *curlm = curl_multi_init();
	if (curlm == nullptr) {
		std::cerr << logb(LogErr)
			  << "Failed to create CURL multi handle" << std::endl;
		return nullptr;
	}

	return std::make_unique<VCoproc>(
	    stopfd, curlm, std::move(be), verbose, consume, monitor,
	    std::move(source), std::move(input_dirs), std::move(output_dir),
	    std::move(failed_dir), std::move(forward_dir), std::move(dbfile),
	    std::move(dbconn), std::move(host), port);
}

VCoproc::VCoproc(int stopfd, CURLM *curlm, std::unique_ptr<Backend> be,
		 int verbose, bool consume, bool monitor, std::string source,
		 std::vector<std::string> input_dirs, std::string output_dir,
		 std::string failed_dir, std::string forward_dir,
		 std::string dbfile, std::unique_ptr<SQLiteDbConn> dbconn,
		 std::string host, unsigned short port)
    : stopfd(stopfd),
      curlm(curlm),
      be(std::move(be)),
      verbose(verbose),
      consume(consume),
      monitor(monitor),
      source(std::move(source)),
      input_dirs(std::move(input_dirs)),
      output_dir(std::move(output_dir)),
      failed_dir(std::move(failed_dir)),
      forward_dir(std::move(forward_dir)),
      dbfile(std::move(dbfile)),
      dbconn(std::move(dbconn)),
      host(std::move(host)),
      port(port),
      stats_start(std::chrono::system_clock::now())
{
}

VCoproc::~VCoproc()
{
	/*
	 * PendingFile objects must be destroyed before calling
	 * curl_multi_cleanup(). Force destruction with clear().
	 */
	pending.clear();

	if (curlm != nullptr) {
		curl_multi_cleanup(curlm);
	}
}

int
VCoproc::FetchMoreFiles()
{
	/*
	 * If we are consuming the files, the corresponding entries are
	 * removed as soon as the post processing is complete, and so
	 * we can limit the pending table to MaxEntries.
	 * Otherwise we must use a much larger limit.
	 * TODO: maybe use DB to store the completed entries...
	 */
	int credits = consume ? MaxEntries : 8192;

	assert(input_dir_idx < input_dirs.size());

	/*
	 * Scan all the input directories, starting from the one that was
	 * scanned less recently.
	 */
	for (size_t n = 0; credits > 0 && n < input_dirs.size(); n++) {
		/*
		 * Visit this input directory and all of its input
		 * subdirectories (recursively).
		 * The visit is implemented as a BFS (Breadth First Search).
		 */
		std::deque<std::string> frontier = {input_dirs[input_dir_idx]};

		for (int c = 0; !frontier.empty() && credits > 0 && c < 32;
		     c++) {
			std::string &dir = frontier.front();
			int ret;

			ret = FetchFilesFromDir(dir, frontier, credits);
			if (ret) {
				return ret;
			}

			frontier.pop_front();
		}
		if (++input_dir_idx >= input_dirs.size()) {
			input_dir_idx = 0;
		}
	}

	return 0;
}

int
VCoproc::FetchFilesFromDir(const std::string &dirname,
			   std::deque<std::string> &frontier, int &credits)
{
	struct dirent *dent;
	DIR *dir;

	dir = opendir(dirname.c_str());
	if (dir == nullptr) {
		std::cerr << logb(LogErr) << "Failed to opendir(" << dirname
			  << "): " << strerror(errno) << std::endl;
		return -1;
	}

	while (credits > 0 && (dent = readdir(dir)) != nullptr) {
		if (dent->d_name[0] == '.') {
			/*
			 * Ignore hidden files and directories,
			 * including the current and parent
			 * directories ("." and "..").
			 */
			continue;
		}

		/*
		 * DT_UNKNOWN means that the file system does not
		 * return file type information in d_type.
		 * For such filesystem it is necessary to
		 * use stat() or lstat() to check that it
		 * is indeed a regular file.
		 */
		bool is_file =
		    dent->d_type == DT_REG ||
		    (dent->d_type == DT_UNKNOWN && IsFile(dent->d_name));
		bool is_dir =
		    dent->d_type == DT_DIR ||
		    (dent->d_type == DT_UNKNOWN && IsDir(dent->d_name));
		std::string path = PathJoin(dirname, dent->d_name);

		if (is_dir) {
			if (DirEmpty(path)) {
				/*
				 * If we find an empty directory, we remove it.
				 * This will also help to do less BFS work.
				 */
				if (rmdir(path.c_str())) {
					std::cerr << logb(LogErr)
						  << "Failed to remove empty "
						     "directory "
						  << path << ": "
						  << strerror(errno)
						  << std::endl
						  << std::flush;
				} else if (verbose) {
					std::cout << logb(LogDbg)
						  << "Removed empty directory "
						  << path << std::endl
						  << std::flush;
				}
			} else {
				/*
				 * We found a non-empty subdirectory.
				 * Append it to the BFS frontier set.
				 */
				frontier.push_back(std::move(path));
			}
			continue;
		}

		if (!is_file) {
			continue;
		}

		/*
		 * We got a file good for processing. Insert it into the
		 * database (if it's not already there) and decrease the
		 * credits.
		 */
		std::string apath = AbsPath(path);

		if (apath.empty()) {
			std::cerr << logb(LogErr) << "AbsPath(" << path
				  << ") failed!" << std::endl;
		} else {
			path = apath;
		}

		if (!pending.count(path)) {
			pending[path] = std::move(
			    PendingFile::Create(curlm, path, verbose));
			if (verbose) {
				std::cout << logb(LogDbg) << "New file " << path
					  << std::endl;
			}
		}
		credits--;
	}

	closedir(dir);

	return 0;
}

int
VCoproc::CleanupCompleted()
{
	for (auto it = pending.begin(); it != pending.end();) {
		auto &proc = it->second;

		/*
		 * Don't remove the entry from the pending
		 * table if we are not consuming the files,
		 * otherwise we would reprocess it.
		 */
		if (proc->Status() == ProcStatus::Complete && consume) {
			if (verbose) {
				std::cout << logb(LogDbg) << "Completed "
					  << proc->FilePath() << std::endl;
			}
			it = pending.erase(it);
		} else if (proc->Status() == ProcStatus::Waiting &&
			   proc->InactivitySeconds() > 1) {
			if (verbose) {
				std::cout << logb(LogDbg) << "Timed out "
					  << proc->FilePath() << std::endl;
			}
			it = pending.erase(it);
		} else {
			++it;
		}
	}

	return 0;
}

int
VCoproc::TimeoutWaiting()
{
	for (auto &kv : pending) {
		auto &proc = kv.second;

		if (proc->Status() == ProcStatus::Waiting &&
		    proc->InactivitySeconds() > 1) {
			proc->SetStatus(ProcStatus::ProcFailure);
			std::cerr << logb(LogErr) << "File " << proc->FilePath()
				  << " timed out" << std::endl;
			stats.files_failed++;
			stats.bytes_failed += proc->FileSize();
			stats.files_timedout++;
			stats.bytes_timedout += proc->FileSize();
		}
	}

	return 0;
}

int
VCoproc::UpdateStatistics()
{
	std::stringstream qss;

	if (SecsElapsed(stats_start) < 2) {
		return 0;
	}

	qss << "INSERT INTO stats(timestamp, files_scored, "
	       "bytes_scored, files_nomdata, bytes_nomdata, "
	       "files_failed, bytes_failed, files_timedout, bytes_timedout, "
	       "files_completed, "
	       "bytes_completed) VALUES(strftime('%s','now'), "
	    << stats.files_scored << "," << stats.bytes_scored << ","
	    << stats.files_nomdata << "," << stats.bytes_nomdata << ","
	    << stats.files_failed << "," << stats.bytes_failed << ","
	    << stats.files_timedout << "," << stats.bytes_timedout << ","
	    << stats.files_completed << "," << stats.bytes_completed << ")";
	if (dbconn->ModifyStmt(qss, verbose)) {
		return -1;
	}

	stats_start = std::chrono::system_clock::now();
	stats	    = {};

	return 0;
}

/*
 * Wait for the backend to come online.
 */
int
VCoproc::WaitForBackend()
{
	unsigned int milliseconds = 5000;
	struct pollfd pfd[1];

	pfd[0].fd     = stopfd;
	pfd[0].events = POLLIN;

	do {
		int ret = poll(pfd, 1, /*timeout_ms=*/milliseconds);

		if (ret < 0) {
			if (errno == EINTR) {
				/*
				 * This happens if a signal was caught
				 * during poll. We just continue, so that
				 * the signal handler can write to the
				 * stopfd and poll() returns 1.
				 */
				continue;
			}
			std::cerr << logb(LogErr)
				  << "poll() failed: " << strerror(errno)
				  << std::endl;
			return ret;
		}

		if (ret > 0) {
			/*
			 * We got a termination signal. Return 1 to inform
			 * the user.
			 */
			assert(pfd[0].revents & POLLIN);
			EventFdDrain(stopfd);

			return 1;
		}

		/*
		 * It's safer to have the sleep before the first check
		 * to rate limit this function (e.g., if Probe() always
		 * returns true but there is some issue that triggers the
		 * MainLoop to call WaitForBackend() again and again).
		 */
	} while (be->Probe() == false);

	/* Backend is online. We can return. */
	return 0;
}

int
VCoproc::MainLoop()
{
	bool forward = !forward_dir.empty();
	/*
	 * The loop below will set bail_out to true when we get and error
	 * from which we cannot recover.
	 */
	bool bail_out	      = false;
	int num_running_curls = 0;
	int timeout_ms	      = 5000;
	std::string base_url;
	int err = 0;

	while (!bail_out) {
		bool backend_down = false;

		/*
		 * Refill the pending table by fetching more files from the
		 * input directories.
		 */
		err = FetchMoreFiles();
		if (err) {
			break;
		}

		/*
		 * Scan any new entries and prepare a POST request to
		 * submit to the backend engine.
		 */
		int new_files = 0;
		for (auto &kv : pending) {
			auto &proc = kv.second;

			if (proc->Status() != ProcStatus::New) {
				continue;
			}

			new_files++;

			json11::Json jsreq = json11::Json::object{
			    {"file_name", proc->FilePath()},
			};
			if (proc->PreparePost(be->BaseUrl() + "/process",
					      jsreq)) {
				continue;
			}
			proc->SetStatus(ProcStatus::Waiting);
		}

		/* Advance any pending POST transfers. */
		CURLMcode cm = curl_multi_perform(curlm, &num_running_curls);
		if (cm != CURLM_OK) {
			std::cerr << "Failed to perform multi handle: "
				  << curl_multi_strerror(cm) << std::endl;
			break;
		}

		/* Process any completed POST transfers. */
		int msgs_left = -1;
		CURLMsg *msg;
		while ((msg = curl_multi_info_read(curlm, &msgs_left)) !=
		       nullptr) {
			PendingFile *p;
			long http_code;
			CURLcode cc;

			if (msg->msg != CURLMSG_DONE) {
				std::cerr << logb(LogErr) << "Got CURLM msg "
					  << msg->msg << std::endl;
				continue;
			}

			cc = curl_easy_getinfo(msg->easy_handle,
					       CURLINFO_PRIVATE, (char **)&p);
			if (cc != CURLE_OK) {
				std::cerr << "Failed to get CURLINFO_PRIVATE: "
					  << curl_easy_strerror(cc)
					  << std::endl;
				continue;
			}

			cc = curl_easy_getinfo(msg->easy_handle,
					       CURLINFO_RESPONSE_CODE,
					       &http_code);
			if (cc != CURLE_OK) {
				std::cerr
				    << "Failed to get CURLINFO_RESPONSE_CODE: "
				    << curl_easy_strerror(cc) << std::endl;
				http_code = 400;
			}

			if (http_code == 0) {
				/*
				 * Backend down. No reason to advance the
				 * pending requests. Those will be reprocessed.
				 */
				backend_down = true;
				break;
			}

			bool success = (http_code == 200);
			json11::Json::object jsresp;

			if (p->CompletePost(jsresp)) {
				success = false;
			}

			if (verbose) {
				std::cout << logb(LogDbg) << "Processed "
					  << p->FilePath() << " --> "
					  << http_code << " "
					  << json11::Json(jsresp).dump()
					  << std::endl;
			}

			if (!jsresp.count("status")) {
				std::cerr << logb(LogErr)
					  << "Missing status key" << std::endl;
				success = false;
			} else {
				success = (jsresp["status"] == "COMPLETE") ||
					  (jsresp["status"] == "NOMETADATA");
			}

			if (success) {
				/* Output JSON. */
				std::string jsname =
				    FileBaseName(p->FilePath());
				std::string jspath;

				jsresp["origin"] = source;

				jsname = PathNameNoExt(jsname) + ".json";
				jspath = PathJoin(output_dir, jsname);
				std::ofstream fout(jspath);
				fout << json11::Json(jsresp).dump();
				fout << std::endl;
			}

			if (!success) {
				p->SetStatus(ProcStatus::ProcFailure);
				stats.files_failed++;
				stats.bytes_failed += p->FileSize();
			} else {
				p->SetStatus(ProcStatus::ProcSuccess);
				if (jsresp["status"] == "COMPLETE") {
					stats.files_scored++;
					stats.bytes_scored += p->FileSize();
				} else {
					stats.files_nomdata++;
					stats.bytes_nomdata += p->FileSize();
				}
			}
		}

		if (backend_down) {
			/*
			 * The backend went down for some reason.
			 * Flush any pending requests and wait
			 * for the backend to go back online.
			 */
			int ret;

			/*
			 * We could clear only the ones in waiting state,
			 * but we won't bother because it's harmless to
			 * reprocess already processed files.
			 */
			pending.clear();

			std::cout << "Backend went offline. Waiting ..."
				  << std::endl;
			ret = WaitForBackend();
			if (ret != 0) {
				/*
				 * Stop on error (ret < 0) or because we got
				 * the termination signal (ret > 0).
				 */
				break;
			}
			std::cout << "Backend is back online!" << std::endl;

			/*
			 * It's convenient to start from the beginning
			 * of the iteration, so that we fetch more files,
			 * including the ones to reprocess.
			 */
			continue;
		}

		/*
		 * Post process any entries in ProcSuccess or ProcFailure state.
		 */
		for (auto &kv : pending) {
			bool remove, copy, move, copymove, success, failure;
			auto &proc = kv.second;
			std::string dstdir;

			success = proc->Status() == ProcStatus::ProcSuccess;
			failure = proc->Status() == ProcStatus::ProcFailure;

			if (!(success || failure)) {
				continue;
			}

			remove	 = success && consume && !forward;
			copymove = failure || (success && forward);
			copy	 = copymove && !consume;
			move	 = copymove && consume;
			dstdir	 = failure ? failed_dir : forward_dir;

			if (remove) {
				if (RemoveFile(proc->FilePath())) {
					bail_out = true;
				} else if (verbose) {
					std::cout << logb(LogDbg) << "Removed "
						  << proc->FilePath()
						  << std::endl;
				}

			} else if (move) {
				if (MoveToDir(dstdir, proc->FilePath())) {
					bail_out = true;
				} else if (verbose) {
					std::cout << logb(LogDbg) << "Moved "
						  << proc->FilePath() << " --> "
						  << dstdir << std::endl;
				}

			} else if (copy) {
				if (CopyToDir(dstdir, proc->FilePath())) {
					bail_out = true;
				} else if (verbose) {
					std::cout << logb(LogDbg) << "Copied "
						  << proc->FilePath() << " --> "
						  << dstdir << std::endl;
				}
			} else {
				/* success && !consume && !forward */
			}

			stats.files_completed++;
			stats.bytes_completed += proc->FileSize();

			proc->SetStatus(ProcStatus::Complete);
		}

		/*
		 * Mark timed out entries as failed. They will be
		 * removed during the next step.
		 */
		err = TimeoutWaiting();
		if (err) {
			break;
		}

		/*
		 * Remove any completed entries from the
		 * pending table, to make space for new input
		 * files.
		 */
		err = CleanupCompleted();
		if (err) {
			break;
		}

		/* Update the statistics. */
		UpdateStatistics();

		/*
		 * When there are no more files to be processed or pending
		 * activities, stop if we are not in monitor mode.
		 */
		if (num_running_curls == 0 && new_files == 0 && !monitor) {
			break;
		}

		/*
		 * Wait for any activity on POST transfers or on the stop
		 * file descriptor.
		 */
		struct curl_waitfd wfd[1];
		wfd[0].fd      = stopfd;
		wfd[0].events  = CURL_WAIT_POLLIN;
		wfd[0].revents = 0;
		cm	       = curl_multi_wait(curlm, wfd, 1, timeout_ms,
					 /*&numfds=*/NULL);
		if (cm != CURLM_OK) {
			std::cerr << "Failed to wait multi handle: "
				  << curl_multi_strerror(cm) << std::endl;
			break;
		}

		if (wfd[0].revents & CURL_WAIT_POLLIN) {
			EventFdDrain(stopfd);
			if (verbose) {
				std::cout << "Stopping the event loop"
					  << std::endl;
			}
			break;
		}
	}

	return err;
}

int
main(int argc, char **argv)
{
	std::vector<std::string> input_dirs;
	std::string output_dir;
	std::string failed_dir;
	std::string forward_dir;
	std::string dbfile;
	std::string source;
	std::string host;
	unsigned short port = 0;
	struct sigaction sa;
	int verbose  = 0;
	bool consume = false;
	bool monitor = false;
	int opt, ret;

	/*
	 * Open an eventfd to be used for synchronization with
	 * the main loop.
	 */
	stopfd_global = UniqueFd(eventfd(0, 0));
	if (stopfd_global < 0) {
		std::cerr << logb(LogErr) << "Failed to open eventfd()"
			  << std::endl;
		return -1;
	}

	/*
	 * Install a signal handler for SIGINT and SIGTERM to
	 * tell the event loop to stop.
	 */
	sa.sa_handler = SigintHandler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	ret	    = sigaction(SIGINT, &sa, NULL);
	if (ret) {
		std::cerr << logb(LogErr)
			  << "Failed to sigaction(SIGINT): " << strerror(errno)
			  << std::endl;
		return ret;
	}

	ret = sigaction(SIGTERM, &sa, NULL);
	if (ret) {
		std::cerr << logb(LogErr)
			  << "Failed to sigaction(SIGTERM): " << strerror(errno)
			  << std::endl;
		return ret;
	}

	while ((opt = getopt(argc, argv, "hVvi:o:F:f:cmD:H:p:s:")) != -1) {
		switch (opt) {
		case 'h':
			Usage(argv[0]);
			return 0;
			break;

		case 'V':
			PrintVersionInfo();
			return 0;
			break;

		case 'v':
			verbose++;
			break;

		case 's':
			source = optarg;
			break;

		case 'i': {
			if (!DirExists(optarg)) {
				std::cerr << logb(LogErr) << "Directory "
					  << optarg << " not found "
					  << std::endl;
				return -1;
			}
			input_dirs.push_back(std::string(optarg));
			break;
		}

		case 'o': {
			if (!DirExists(optarg)) {
				std::cerr << logb(LogErr) << "Directory "
					  << optarg << " not found "
					  << std::endl;
				return -1;
			}
			output_dir = std::string(optarg);
			break;
		}

		case 'F': {
			if (!DirExists(optarg)) {
				std::cerr << logb(LogErr) << "Directory "
					  << optarg << " not found "
					  << std::endl;
				return -1;
			}
			failed_dir = std::string(optarg);
			break;
		}

		case 'f': {
			if (!DirExists(optarg)) {
				std::cerr << logb(LogErr) << "Directory "
					  << optarg << " not found "
					  << std::endl;
				return -1;
			}
			forward_dir = std::string(optarg);
			break;
		}

		case 'c':
			consume = true;
			break;

		case 'm':
			monitor = true;
			break;

		case 'D':
			dbfile = std::string(optarg);
			break;

		case 'H':
			host = optarg;
			break;

		case 'p':
			if (!Str2Num<unsigned short>(optarg, port)) {
				std::cerr << logb(LogErr) << "Invalid port "
					  << optarg << std::endl;
				return -1;
			}
			break;
		}
	}

	curl_global_init(CURL_GLOBAL_ALL);

	auto vcoproc = VCoproc::Create(
	    stopfd_global, verbose, consume, monitor, std::move(source),
	    std::move(input_dirs), std::move(output_dir), std::move(failed_dir),
	    std::move(forward_dir), dbfile, host, port);
	if (vcoproc == nullptr) {
		return -1;
	}

	return vcoproc->MainLoop();
}
