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
#include <regex>
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
	    << "    -e INPUT_FILE_EXT (consider input files with this "
	       "extension)"
	    << std::endl
	    << "    -o OUTPUT_DIR (directory where to store "
	       "JSON processing output)"
	    << std::endl
	    << "    -F FAILED_DIR (directory where to store failed files)"
	    << std::endl
	    << "    -f FORWARD_DIR (directory where to move processed files)"
	    << std::endl
	    << "    -a ARCHIVE_DIR (directory where to archive processed wav "
	       "files)"
	    << std::endl
	    << "    -C (compress archived files to mp3)" << std::endl
	    << "    -D DB_FILE (path to the sqlite3 database file)" << std::endl
	    << "    -c (consume input files)" << std::endl
	    << "    -m (monitor input directories rather than stop when "
	       "running out of files)"
	    << std::endl
	    << "    -n MAX_PENDING_TRANSACTIONS (max number of concurrent "
	       "processing transactions)"
	    << std::endl
	    << "    -A DIR_MIN_AGE (minimum age, in seconds, of empty "
	       "directories to be removed; defaults to 0)"
	    << std::endl
	    << "    -T STATS_PERIOD (how often to update statistics, in "
	       "seconds)"
	    << std::endl
	    << "    -R RETENTION_DAYS (how long to keep statistics, "
	       "in days)"
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

class BackendTransaction {
    public:
	virtual int PrepareRequest(std::string &url, std::string &req) = 0;
	virtual int ProcessResponse(std::string &resp,
				    json11::Json::object &jout)	       = 0;
};

class Backend {
    public:
	virtual bool Probe() = 0;
	virtual std::unique_ptr<BackendTransaction> CreateTransaction(
	    const std::string &filepath) = 0;
};

enum class PendState {
	New		= 0,
	ReadyToSubmit	= 1,
	WaitingResponse = 2,
	ProcSuccess	= 3,
	ProcFailure	= 4,
	Complete	= 5,
};

/*
 * A class representing an entry in the pending table.
 */
class PendingFile {
    public:
	enum class CurlState {
		Idle	 = 0,
		Prepared = 1,
	};

	std::unique_ptr<BackendTransaction> bt;

    private:
	CURLM *curlm = nullptr;
	CURL *curl   = nullptr;
	std::string src_path; /* file path */
	size_t src_size = 0;  /* file size in bytes */
	int verbose	= 0;

	/* Temporary storage for the current CURL request body. */
	std::string postreq;

	/* Temporary storage accumulator for the current CURL response body. */
	std::stringstream postresp;

	/* Time of last interaction with the backend. */
	std::chrono::time_point<std::chrono::system_clock> last_activity;

	/* Time of processing start. */
	std::chrono::time_point<std::chrono::system_clock> proc_start;

	/* State of this pending file. */
	PendState state = PendState::New;

	/* State of the CURL easy handle associated to this file. */
	CurlState curl_state = CurlState::Idle;

	/* Temporary storage for external JSON metadata for this file. */
	json11::Json jmdata;
	std::string jmdatapath;

    public:
	/* Accumulator for the JSON output associated to this file. */
	json11::Json::object jout;

	PendingFile(std::unique_ptr<BackendTransaction> bt, CURLM *curlm,
		    CURL *curl, const std::string &src_path, size_t src_size,
		    int verbose)
	    : bt(std::move(bt)),
	      curlm(curlm),
	      curl(curl),
	      src_path(src_path),
	      src_size(src_size),
	      verbose(verbose),
	      last_activity(std::chrono::system_clock::now()),
	      proc_start(std::chrono::system_clock::now())
	{
	}

	~PendingFile();

	PendState State() const { return state; }
	std::string StateStr() const;
	void SetState(PendState state);
	std::string FilePath() const { return src_path; }
	std::string FileName() const { return FileBaseName(src_path); }
	size_t FileSize() const { return src_size; }
	static std::unique_ptr<PendingFile> Create(
	    std::unique_ptr<BackendTransaction> bt, CURLM *curlm,
	    const std::string &src_path, int verbose);
	int LoadMetadata(const std::string &mdatapath);
	json11::Json GetMetadata() const;
	static size_t CurlWriteCallback(void *data, size_t size, size_t nmemb,
					void *userp);
	void AppendResponse(void *data, size_t size);
	int PreparePostCurl(const std::string &url, const std::string &req);
	int RetirePostCurl(std::string &resp);
	float InactivitySeconds() const { return SecsElapsed(last_activity); }
	float AgeSeconds() const { return SecsElapsed(proc_start); }
};

std::unique_ptr<PendingFile>
PendingFile::Create(std::unique_ptr<BackendTransaction> bt, CURLM *curlm,
		    const std::string &src_path, int verbose)
{
	CURLcode cc;
	CURL *curl;

	if (curlm == nullptr || bt == nullptr) {
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

	long long int src_size = utils::FileSize(src_path);
	if (src_size < 0) {
		return nullptr;
	}

	auto pf = std::make_unique<PendingFile>(
	    std::move(bt), curlm, curl, src_path, static_cast<size_t>(src_size),
	    verbose);

	/* Link the new PendingFile instance to the curl handle. */
	cc = curl_easy_setopt(curl, CURLOPT_PRIVATE, (void *)pf.get());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_PRIVATE: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	/* Link the new PendingFile instance to our write callback. */
	cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)pf.get());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_WRITEDATA: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	return pf;
}

PendingFile::~PendingFile()
{
	if (!jmdatapath.empty()) {
		RemoveFile(jmdatapath, /*may_not_exist=*/true);
	}

	if (curl != nullptr) {
		curl_easy_cleanup(curl);
	}
}

std::string
PendingFile::StateStr() const
{
	switch (state) {
	case PendState::New:
		return "New";
	case PendState::ReadyToSubmit:
		return "ReadyToSubmit";
	case PendState::WaitingResponse:
		return "WaitingResponse";
	case PendState::ProcSuccess:
		return "ProcSuccess";
	case PendState::ProcFailure:
		return "ProcFailure";
	case PendState::Complete:
		return "Complete";
	}

	return std::string();
}

int
PendingFile::LoadMetadata(const std::string &mdatapath)
{
	std::ifstream fin(mdatapath);
	if (!fin) {
		std::cerr << "Failed to open " << mdatapath << std::endl;
		return -1;
	}

	std::string jstr((std::istreambuf_iterator<char>(fin)),
			 std::istreambuf_iterator<char>());
	std::string errs;

	json11::Json js = json11::Json::parse(jstr, errs);
	if (!errs.empty() && js == json11::Json()) {
		std::cerr << logb(LogErr) << "Metadata is not a JSON: " << jstr
			  << std::endl;
		return -1;
	}
	jmdata	   = js;
	jmdatapath = mdatapath;

	return 0;
}

json11::Json
PendingFile::GetMetadata() const
{
	return jmdata;
}

size_t
PendingFile::CurlWriteCallback(void *data, size_t size, size_t nmemb,
			       void *userp)
{
	size_t chunksz	= size * nmemb;
	PendingFile *pf = reinterpret_cast<PendingFile *>(userp);

	assert(pf != nullptr);
	pf->AppendResponse(data, chunksz);

	return chunksz;
}

void
PendingFile::SetState(PendState next_state)
{
	if (verbose >= 2) {
		std::cout << logb(LogDbg) << src_path << ": "
			  << static_cast<int>(state) << " -> "
			  << static_cast<int>(next_state) << std::endl;
	}
	state = next_state;
}

void
PendingFile::AppendResponse(void *data, size_t size)
{
	postresp.write((const char *)data, size);
	last_activity = std::chrono::system_clock::now();
}

/* Prepare a post request without performing it. */
int
PendingFile::PreparePostCurl(const std::string &url, const std::string &req)
{
	CURLcode cc;

	if (curl_state != CurlState::Idle) {
		std::cerr << "CURL state (" << static_cast<int>(curl_state)
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
	postreq = req;
	cc	= curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postreq.c_str());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_POSTFIELDS: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	}

	/*
	 * If we don't provide POSTFIELDSIZE, libcurl will strlen() by
	 * itself.
	 */
	cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postreq.size());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_POSTFIELDSIZE: "
			  << curl_easy_strerror(cc) << std::endl;
		return -1;
	}

	/* Add this handle to our multi stack. */
	CURLMcode cm = curl_multi_add_handle(curlm, curl);
	if (cm != CURLM_OK) {
		std::cerr << "Failed to add handle to multi stack: "
			  << curl_multi_strerror(cm) << std::endl;
		return -1;
	}

	curl_state    = CurlState::Prepared;
	last_activity = std::chrono::system_clock::now();

	return 0;
}

int
PendingFile::RetirePostCurl(std::string &respstr)
{
	std::string errs;

	if (curl_state != CurlState::Prepared) {
		std::cerr << logb(LogErr)
			  << "Cannot compete POST: Invalid state "
			  << static_cast<int>(curl_state) << std::endl;
		return -1;
	}

	CURLMcode cm = curl_multi_remove_handle(curlm, curl);
	if (cm != CURLM_OK) {
		std::cerr << "Failed to remove handle from multi stack: "
			  << curl_multi_strerror(cm) << std::endl;
	}

	respstr = postresp.str();
	if (respstr.empty()) {
		std::cerr << logb(LogErr) << "Response is empty" << std::endl;
		return -1;
	}

	/* Reset CURL state to allow more requests. */
	curl_state    = CurlState::Idle;
	postresp      = std::stringstream();
	last_activity = std::chrono::system_clock::now();

	return 0;
}

class PsBackend : public Backend {
	std::string int_url;
	std::string bat_url;

    public:
	static std::unique_ptr<Backend> Create(std::string host,
					       unsigned short port);
	PsBackend(std::string host, unsigned short port);
	virtual bool Probe();
	std::unique_ptr<BackendTransaction> CreateTransaction(
	    const std::string &filepath);
	std::string InteractiveURL() const { return int_url; }
	std::string BatchURL() const { return bat_url; }
};

class PsBackendTransaction : public BackendTransaction {
	PsBackend *be = nullptr;
	std::string filepath;
	enum class State {
		Init	       = 0,
		WaitForPing    = 1,
		ReadyToProcess = 2,
		WaitForProcess = 3,
		Finished       = 4,
	};
#if 0
	State state = State::Init;
#else
	State state = State::ReadyToProcess;
#endif

    public:
	PsBackendTransaction(Backend *be, const std::string &filepath)
	    : be(dynamic_cast<PsBackend *>(be)), filepath(filepath)
	{
		assert(this->be != nullptr);
	}
	int PrepareRequest(std::string &url, std::string &req);
	int ProcessResponse(std::string &resp, json11::Json::object &jout);
};

int
PsBackendTransaction::PrepareRequest(std::string &url, std::string &req)
{
	json11::Json jsreq;

	switch (state) {
	case State::Init: {
		url   = be->InteractiveURL() + "/ping";
		state = State::WaitForPing;
		break;
	}

	case State::ReadyToProcess: {
		url   = be->BatchURL() + "/process";
		jsreq = json11::Json::object{
		    {"file_name", filepath},
		};
		state = State::WaitForProcess;
		break;
	}

	default:
		std::cerr << "PrepareRequest() called in "
			     "illegal state "
			  << static_cast<int>(state) << std::endl;
		return -1;
		break;
	}

	req = jsreq.dump();

	return 0;
}

int
PsBackendTransaction::ProcessResponse(std::string &resp,
				      json11::Json::object &jout)
{
	std::string errs;
	json11::Json jsresp = json11::Json::parse(resp, errs);

	if (!errs.empty() && jsresp == json11::Json()) {
		std::cerr << logb(LogErr) << "Response is not a JSON: " << resp
			  << std::endl;
		return -1;
	}

	switch (state) {
	case State::WaitForPing:
		state = State::ReadyToProcess;
		break;

	case State::WaitForProcess:
		/* Set jout. */
		jout  = jsresp.object_items();
		state = State::Finished; /* Transaction is now
					    complete. */
		return 1;
		break;

	default:
		std::cerr << logb(LogErr)
			  << "ProcessResponse() called in "
			     "illegal state "
			  << static_cast<int>(state) << std::endl;
		return -1;
		break;
	}

	return 0;
}

std::unique_ptr<Backend>
PsBackend::Create(std::string host, unsigned short port)
{
	return std::make_unique<PsBackend>(host, port);
}

PsBackend::PsBackend(std::string host, unsigned short port)
{
	std::stringstream ss;

	ss << "http://" << host << ":" << port;
	int_url = ss.str();

	ss = std::stringstream();
	ss << "http://" << host << ":" << port;
	bat_url = ss.str();
}

bool
PsBackend::Probe()
{
	std::string url = InteractiveURL() + std::string("/ping");
	long http_code	= 0;
	CURLcode cc;
	CURL *curl;

	curl = curl_easy_init();
	if (curl == nullptr) {
		std::cerr << logb(LogErr) << "Failed to create CURL easy handle"
			  << std::endl;
		return false;
	}

	/* Write callback to push data to a local stringstream
	 * variable. */
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
	 * The "+" magic forces a conversion to a C-style
	 * function pointer. The writef function cannot have
	 * captures.
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
			std::cerr << "Failed to get "
				     "CURLINFO_RESPONSE_CODE: "
				  << curl_easy_strerror(cc) << std::endl;
			goto end;
		}
	}

end:
	curl_easy_cleanup(curl);

	return http_code == 200;
}

std::unique_ptr<BackendTransaction>
PsBackend::CreateTransaction(const std::string &filepath)
{
	return std::make_unique<PsBackendTransaction>(this, filepath);
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
	std::vector<std::string> input_exts;
	std::string output_dir;
	std::string failed_dir;
	std::string forward_dir;
	std::string archive_dir;
	bool compress_archived = false;

	/*
	 * Max number of entries that we allow in the pending table
	 * at any time.
	 */
	unsigned short max_pending = 5;

	/*
	 * Minimum age of an empty input subdirectory to be elegible
	 * for removal. This is useful for those cases where the
	 * producer creates visible directories before filling
	 * them with their complete contents.
	 */
	unsigned short dir_min_age = 0;

	/*
	 * Statistics update period in seconds, and retention days.
	 */
	unsigned int stats_period   = 300;
	unsigned int retention_days = 7;

	std::string dbfile;
	std::unique_ptr<SQLiteDbConn> dbconn;
	std::string host;
	unsigned short port  = 0;
	size_t input_dir_idx = 0;

	/*
	 * Set by any routine in the MainLoop on unrecoverable
	 * errors. The MainLoop will stop ASAP.
	 * TODO currently only set by PostProcessFiles(). If
	 * this is the final situation, get rid of this variable
	 * and use the PostProcessFiles() return value.
	 */
	bool bail_out = false;

	/*
	 * Incremented by the MainLoop anytime there is some
	 * activity. If an iteration completes without any
	 * activity, we allow one more iteration before
	 * stopping, because more files may be available.
	 */
	int progress = 0;

	struct {
		uint64_t files_scored	 = 0;
		uint64_t bytes_scored	 = 0;
		double audiosec_scored	 = 0;
		double speechsec_scored	 = 0;
		uint64_t files_nomdata	 = 0;
		uint64_t bytes_nomdata	 = 0;
		double audiosec_nomdata	 = 0;
		uint64_t files_failed	 = 0;
		uint64_t bytes_failed	 = 0;
		uint64_t files_timedout	 = 0;
		uint64_t bytes_timedout	 = 0;
		uint64_t files_completed = 0;
		uint64_t bytes_completed = 0;
		double procsec_completed = 0;
	} stats;
	std::chrono::time_point<std::chrono::system_clock> stats_start;

	/* Map of in-progress pf entries. */
	std::unordered_map<std::string, std::unique_ptr<PendingFile>> pending;

	/* How many files are currently in progress. */
	int inprogress_counter = 0;

	/* A pair (path, depth) useful for depth-limited DFS. */
	struct DFSDir {
		std::string path;
		int depth = 0;
		DFSDir(std::string p, int d = 0) : path(std::move(p)), depth(d)
		{
		}
	};

	void SetPendingFileState(const std::unique_ptr<PendingFile> &pf,
				 PendState next_state);
	void SetPendingFileState(PendingFile *pf, PendState next_state);
	size_t FetchMoreFiles();
	int FetchFilesFromDir(const DFSDir &dfsdir,
			      std::deque<DFSDir> &frontier, int &credits);
	bool AnyPendingActivity(int num_running_curls) const;
	bool AnyImmediateAction() const;
	void PreProcessNewFiles();
	void PreparePostRequests();
	bool RetireAndProcessPostResponses();
	void TimeoutWaitingRequests();
	void PostProcessFiles();
	void CleanupCompletedFiles();
	int UpdateStatistics(bool force);
	int WaitForBackend();

	void DumpPendingTable() const;

    public:
	static std::unique_ptr<VCoproc> Create(
	    int stopfd, int verbose, bool consume, bool monitor,
	    std::string source, std::vector<std::string> input_dirs,
	    std::vector<std::string> input_exts, std::string output_dir,
	    std::string failed_dir, std::string forward_dir,
	    std::string archive_dir, bool compress_archived,
	    unsigned short max_pending, unsigned short dir_min_age,
	    unsigned int stats_period, unsigned int retention_days,
	    std::string dbfile, std::string host, unsigned short port);

	VCoproc(int stopfd, CURLM *curlm, std::unique_ptr<Backend>, int verbose,
		bool consume, bool monitor, std::string source,
		std::vector<std::string> input_dirs,
		std::vector<std::string> input_exts, std::string output_dir,
		std::string failed_dir, std::string forward_dir,
		std::string archive_dir, bool compress_archived,
		unsigned short max_pending, unsigned short dir_min_age,
		unsigned int stats_period, unsigned int retention_days,
		std::string dbfile, std::unique_ptr<SQLiteDbConn> dbconn,
		std::string host, unsigned short port);
	~VCoproc();
	int MainLoop();
};

std::unique_ptr<VCoproc>
VCoproc::Create(int stopfd, int verbose, bool consume, bool monitor,
		std::string source, std::vector<std::string> input_dirs,
		std::vector<std::string> input_exts, std::string output_dir,
		std::string failed_dir, std::string forward_dir,
		std::string archive_dir, bool compress_archived,
		unsigned short max_pending, unsigned short dir_min_age,
		unsigned int stats_period, unsigned int retention_days,
		std::string dbfile, std::string host, unsigned short port)
{
	if (source.empty()) {
		std::cerr << logb(LogErr) << "No source/origin specified (-s)"
			  << std::endl;
		return nullptr;
	}

	if (input_dirs.empty()) {
		std::cerr << logb(LogErr)
			  << "No input directories specified (-i)" << std::endl;
		return nullptr;
	}

	if (input_exts.empty()) {
		input_exts.push_back("wav");
	}

	if (output_dir.empty()) {
		std::cerr << logb(LogErr)
			  << "No output directory specified (-o)" << std::endl;
		return nullptr;
	}

	if (failed_dir.empty()) {
		std::cerr << logb(LogErr)
			  << "No failed directory specified (-F)" << std::endl;
		return nullptr;
	}

	if (max_pending < 1 || max_pending > 512) {
		std::cerr << logb(LogErr)
			  << "Number of max pending "
			     "transactions out of range"
			  << std::endl;
		return nullptr;
	}

	if (dir_min_age < 0 || dir_min_age > 60) {
		std::cerr << logb(LogErr)
			  << "Directory minimum age out of range" << std::endl;
		return nullptr;
	}

	if (stats_period < 5 || stats_period > 10000) {
		std::cerr << logb(LogErr) << "Stats period out of range"
			  << std::endl;
		return nullptr;
	}

	if (retention_days < 1 || retention_days > 10000) {
		std::cerr << logb(LogErr) << "Retention days out of range"
			  << std::endl;
		return nullptr;
	}

	if (dbfile.empty()) {
		std::cerr << logb(LogErr) << "No database file specified (-D)"
			  << std::endl;
		return nullptr;
	}

	if (host.empty()) {
		std::cerr << logb(LogErr) << "No hostname specified (-H)"
			  << std::endl;
		return nullptr;
	}

	if (port == 0) {
		std::cerr << logb(LogErr) << "No port specified (-p)"
			  << std::endl;
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

	/* Create the stats table if it does not exist
	 * already. */
	{
		std::stringstream qss;
		qss << "CREATE TABLE IF NOT EXISTS stats ("
		    << "timestamp UNSIGNED INTEGER PRIMARY KEY "
		       "NOT NULL, "
		    << "diffseconds DOUBLE "
		       "NOT NULL CHECK(diffseconds > 0.0), "
		    << "files_scored UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "bytes_scored UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "audiosec_scored DOUBLE NOT NULL, "
		    << "speechsec_scored DOUBLE NOT NULL, "
		    << "files_nomdata UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "bytes_nomdata UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "audiosec_nomdata DOUBLE NOT NULL, "
		    << "files_failed UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "bytes_failed UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "files_timedout UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "bytes_timedout UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "files_completed UNSIGNED INTEGER NOT "
		       "NULL, "
		    << "bytes_completed UNSIGNED INTEGER NOT "
		       "NULL,"
		    << "procsec_completed DOUBLE NOT NULL"
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
	    std::move(source), std::move(input_dirs), std::move(input_exts),
	    std::move(output_dir), std::move(failed_dir),
	    std::move(forward_dir), std::move(archive_dir), compress_archived,
	    max_pending, dir_min_age, stats_period, retention_days,
	    std::move(dbfile), std::move(dbconn), std::move(host), port);
}

VCoproc::VCoproc(int stopfd, CURLM *curlm, std::unique_ptr<Backend> be,
		 int verbose, bool consume, bool monitor, std::string source,
		 std::vector<std::string> input_dirs,
		 std::vector<std::string> input_exts, std::string output_dir,
		 std::string failed_dir, std::string forward_dir,
		 std::string archive_dir, bool compress_archived,
		 unsigned short max_pending, unsigned short dir_min_age,
		 unsigned int stats_period, unsigned int retention_days,
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
      input_exts(std::move(input_exts)),
      output_dir(std::move(output_dir)),
      failed_dir(std::move(failed_dir)),
      forward_dir(std::move(forward_dir)),
      archive_dir(std::move(archive_dir)),
      compress_archived(compress_archived),
      max_pending(max_pending),
      dir_min_age(dir_min_age),
      stats_period(stats_period),
      retention_days(retention_days),
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

void
VCoproc::SetPendingFileState(const std::unique_ptr<PendingFile> &pf,
			     PendState next_state)
{
	if (next_state == PendState::Complete) {
		inprogress_counter--;
	}
	pf->SetState(next_state);
}

void
VCoproc::SetPendingFileState(PendingFile *pf, PendState next_state)
{
	if (next_state == PendState::Complete) {
		inprogress_counter--;
	}
	pf->SetState(next_state);
}

size_t
VCoproc::FetchMoreFiles()
{
	int credits = max_pending;
	size_t num  = 0;

	assert(input_dir_idx < input_dirs.size());

	/*
	 * Scan all the input directories, starting from the one
	 * that was scanned less recently.
	 */
	for (size_t n = 0; credits > 0 && n < input_dirs.size(); n++) {
		/*
		 * Visit this input directory and all of its
		 * input subdirectories (recursively). The visit
		 * is implemented as a DFS (Depth First Search),
		 * in such a way that we quickly get to the "leaves"
		 * which are supposed to hold the audio files.
		 * A BFS could spend most of the time trasversing
		 * a large number of intermediate directories before
		 * finding a single audio file.
		 * We protect against file system loops by limiting
		 * the DFS depth to a reasonable number.
		 */
		std::deque<DFSDir> frontier = {
		    DFSDir(input_dirs[input_dir_idx])};

		while (!frontier.empty() && credits > 0) {
			DFSDir dfsdir = frontier.back();
			int ret;

			frontier.pop_back();

			if (dfsdir.depth >= 32) {
				std::cout << "Possible loop detected at "
					  << dfsdir.path << std::endl;
				continue;
			}

			ret = FetchFilesFromDir(dfsdir, frontier, credits);
			if (ret > 0) {
				num += ret;
			}
		}
		if (++input_dir_idx >= input_dirs.size()) {
			input_dir_idx = 0;
		}
	}

	return num;
}

/* On success, this function returns the number of new files found. */
int
VCoproc::FetchFilesFromDir(const DFSDir &dfsdir, std::deque<DFSDir> &frontier,
			   int &credits)
{
	struct dirent *dent;
	int ret = 0;
	DIR *dir;

	dir = opendir(dfsdir.path.c_str());
	if (dir == nullptr) {
		std::cerr << logb(LogErr) << "Failed to opendir(" << dfsdir.path
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
		 * DT_UNKNOWN means that the file system does
		 * not return file type information in d_type.
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
		std::string path = PathJoin(dfsdir.path, dent->d_name);

		if (is_dir) {
			if (DirEmpty(path) &&
			    (dir_min_age == 0 ||
			     FileAgeSeconds(path) >= dir_min_age)) {
				/*
				 * If we find an empty directory that has
				 * not been modified in a short while, we
				 * remove it. The age check prevents
				 * situations where we remove directories
				 * created by the input producer before the
				 * producer has the chance to move something
				 * inside. Note that this should not happen
				 * if the producer employs a strict "rsync"
				 * convention. The removal also helps to do less
				 * DFS work.
				 */
				if (rmdir(path.c_str())) {
					std::cerr << logb(LogErr)
						  << "Failed to "
						     "remove empty "
						     "directory "
						  << path << ": "
						  << strerror(errno)
						  << std::endl
						  << std::flush;
				} else if (verbose) {
					std::cout << logb(LogDbg)
						  << "Removed empty "
						     "directory "
						  << path << std::endl
						  << std::flush;
				}
			} else {
				/*
				 * We found a non-empty subdirectory. Append
				 * it to the DFS frontier set.
				 */
				frontier.push_back(
				    DFSDir(std::move(path), dfsdir.depth + 1));
			}
			continue;
		}

		if (!is_file || !FileHasAnyExtension(path, input_exts)) {
			continue;
		}

		/*
		 * We got a file good for processing. Insert it into the
		 * pending table (if it's not already there) and decrease
		 * the credits. To support the case where files are not
		 * consumed, we don't decrement the credits if the file
		 * is in the Complete state. In any case we enforce
		 * a sane limit.
		 */
		std::string apath = AbsPath(path);

		if (apath.empty()) {
			std::cerr << logb(LogErr) << "AbsPath(" << path
				  << ") failed!" << std::endl;
		} else {
			path = apath;
		}

		const auto pit = pending.find(path);

		if (pit != pending.end() &&
		    (consume || pit->second->State() != PendState::Complete)) {
			credits--;
		} else if (pit == pending.end() && pending.size() < 16384) {
			pending[path] = std::move(PendingFile::Create(
			    std::move(be->CreateTransaction(path)), curlm, path,
			    verbose));
			if (verbose) {
				std::cout << logb(LogDbg) << "New file "
					  << pending[path]->FileName()
					  << std::endl;
			}
			ret++; /* increment file count */
			credits--;
			inprogress_counter++;
		}
	}

	closedir(dir);

	return ret;
}

void
VCoproc::PreProcessNewFiles()
{
	for (auto &kv : pending) {
		auto &pf = kv.second;

		if (pf->State() != PendState::New) {
			continue;
		}

		/* Check if we have a JSON file containing
		 * metadata. */
		std::vector<std::string> mdatapaths = {
		    PathNameNewExt(pf->FilePath(), "json"),
		    PathJoin(FileParentDir(pf->FilePath()), "metadata.json")};
		for (const auto &mdatapath : mdatapaths) {
			if (IsFile(mdatapath) &&
			    pf->LoadMetadata(mdatapath) == 0) {
				break;
			}
		}
		SetPendingFileState(pf, PendState::ReadyToSubmit);
		progress++;
	}
}

void
VCoproc::PreparePostRequests()
{
	for (auto &kv : pending) {
		auto &pf = kv.second;

		if (pf->State() != PendState::ReadyToSubmit) {
			continue;
		}

		progress++;

		std::string req;
		std::string url;

		/* Get the URL and content for the next POST. */
		int r = pf->bt->PrepareRequest(url, req);
		if (r) {
			SetPendingFileState(pf, PendState::ProcFailure);
			continue;
		}
		/* Setup the POST request with CURL. */
		if (pf->PreparePostCurl(url, req)) {
			SetPendingFileState(pf, PendState::ProcFailure);
			continue;
		}
		SetPendingFileState(pf, PendState::WaitingResponse);
	}
}

bool
VCoproc::RetireAndProcessPostResponses()
{
	int msgs_left = -1;
	CURLMsg *msg;

	while ((msg = curl_multi_info_read(curlm, &msgs_left)) != nullptr) {
		PendingFile *pf;
		long http_code;
		CURLcode cc;

		if (msg->msg != CURLMSG_DONE) {
			std::cerr << logb(LogErr) << "Got CURLM msg "
				  << msg->msg << std::endl;
			continue;
		}

		cc = curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE,
				       (char **)&pf);
		if (cc != CURLE_OK) {
			std::cerr << "Failed to get "
				     "CURLINFO_PRIVATE: "
				  << curl_easy_strerror(cc) << std::endl;
			continue;
		}

		cc = curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE,
				       &http_code);
		if (cc != CURLE_OK) {
			std::cerr << "Failed to get "
				     "CURLINFO_RESPONSE_CODE: "
				  << curl_easy_strerror(cc) << std::endl;
			http_code = 400;
		}

		progress++;

		if (http_code == 0) {
			/*
			 * Backend down. No reason to advance
			 * the pending requests. Those will be
			 * reprocessed.
			 */
			return false;
		}

		bool success = (http_code == 200);
		std::string resp;

		if (pf->RetirePostCurl(resp)) {
			success = false;
		} else {
			int r = pf->bt->ProcessResponse(resp, pf->jout);
			if (r == 0) {
				/* The transaction is not
				 * complete yet. */
				SetPendingFileState(pf,
						    PendState::ReadyToSubmit);
				continue;
			}
			success = r > 0;
		}

		/*
		 * Transaction is complete. Finalize and output
		 * the JSON.
		 */
		double audio_len  = 0;
		double speech_len = 0;

		if (verbose) {
			std::cout << logb(LogDbg) << "Processed "
				  << pf->FileName() << " --> " << http_code
				  << " " << json11::Json(pf->jout).dump()
				  << std::endl;
		}

		if (!pf->jout.count("status")) {
			std::cerr << logb(LogErr) << "Missing status key"
				  << std::endl;
			success = false;
		} else {
			success = (pf->jout["status"] == "COMPLETE") ||
				  (pf->jout["status"] == "NOMETADATA");
		}

		if (pf->jout.count("length") &&
		    pf->jout["length"].is_number()) {
			audio_len = pf->jout["length"].number_value();
		}
		if (pf->jout.count("net_speech") &&
		    pf->jout["net_speech"].is_number()) {
			speech_len = pf->jout["net_speech"].number_value();
		}

		if (success) {
			/* Output JSON. */
			std::string jsname = pf->FileName();
			std::string jspath;

			pf->jout["origin"] = source;

			json11::Json jmdata = pf->GetMetadata();
			if (jmdata != json11::Json()) {
				pf->jout["metadata"] = jmdata;
			}

			jsname = PathNameNewExt(jsname, "json");
			jspath = PathJoin(output_dir, jsname);
			std::ofstream fout(jspath);
			fout << json11::Json(pf->jout).dump();
			fout << std::endl;
		}

		if (!success) {
			SetPendingFileState(pf, PendState::ProcFailure);
			stats.files_failed++;
			stats.bytes_failed += pf->FileSize();
		} else {
			SetPendingFileState(pf, PendState::ProcSuccess);
			if (pf->jout["status"] == "COMPLETE") {
				stats.files_scored++;
				stats.bytes_scored += pf->FileSize();
				stats.audiosec_scored += audio_len;
				stats.speechsec_scored += speech_len;
			} else {
				stats.files_nomdata++;
				stats.bytes_nomdata += pf->FileSize();
				stats.audiosec_nomdata += audio_len;
			}
		}
	}

	return true;
}

void
VCoproc::CleanupCompletedFiles()
{
	for (auto it = pending.begin(); it != pending.end();) {
		auto &pf = it->second;

		/*
		 * Don't remove the entry from the pending
		 * table if we are not consuming the files,
		 * otherwise we would reprocess it.
		 */
		if (pf->State() == PendState::Complete && consume) {
			if (verbose) {
				std::cout << logb(LogDbg) << "Completed "
					  << pf->FileName() << std::endl;
			}
			it = pending.erase(it);
			progress++;
		} else {
			++it;
		}
	}
}

void
VCoproc::PostProcessFiles()
{
	bool forward = !forward_dir.empty();
	bool archive = !archive_dir.empty();

	for (auto &kv : pending) {
		std::vector<std::string> dstdirs;
		bool success, failure;
		auto &pf = kv.second;

		success = pf->State() == PendState::ProcSuccess;
		failure = pf->State() == PendState::ProcFailure;

		if (!(success || failure)) {
			continue;
		}

		/*
		 * Build a list of destination directories we want to
		 * copy or move the file to.
		 */
		if (failure) {
			dstdirs.push_back(failed_dir);
		}
		if (success && forward) {
			dstdirs.push_back(forward_dir);
		}
		if (success && archive) {
			if (compress_archived) {
				std::string comprfile = PathJoin(
				    archive_dir,
				    PathNameNewExt(pf->FilePath(), "mp3"));
				std::stringstream cmd;

				cmd << "ffmpeg -loglevel quiet -y -i "
				    << pf->FilePath() << " " << comprfile;
				if (ExecuteCommand(cmd, verbose)) {
					std::cerr << logb(LogErr)
						  << "Failed to encode "
						  << pf->FilePath() << " to MP3"
						  << std::endl;
				} else {
					std::cout << logb(LogDbg) << "Encoded "
						  << pf->FilePath() << " --> "
						  << comprfile << std::endl;
				}
			} else {
				dstdirs.push_back(archive_dir);
			}
		}

		/*
		 * No destination directories and we need to consume.
		 * Just remove the file.
		 */
		if (dstdirs.empty() && consume) {
			if (RemoveFile(pf->FilePath())) {
				bail_out = true;
			} else if (verbose) {
				std::cout << logb(LogDbg) << "Removed "
					  << pf->FileName() << std::endl;
			}
		}

		/*
		 * If we have some destination directories, copy the file
		 * to them. If we need to consume the file, perform a final
		 * file move to the first destination.
		 */
		for (size_t i = !!consume; i < dstdirs.size(); i++) {
			if (CopyToDir(dstdirs[i], pf->FilePath())) {
				bail_out = true;
			} else if (verbose) {
				std::cout << logb(LogDbg) << "Copied "
					  << pf->FileName() << " --> "
					  << dstdirs[i] << std::endl;
			}
		}
		if (!dstdirs.empty() && consume) {
			if (MoveToDir(dstdirs[0], pf->FilePath())) {
				bail_out = true;
			} else if (verbose) {
				std::cout << logb(LogDbg) << "Moved "
					  << pf->FileName() << " --> "
					  << dstdirs[0] << std::endl;
			}
		}

		stats.files_completed++;
		stats.bytes_completed += pf->FileSize();
		stats.procsec_completed += pf->AgeSeconds();

		SetPendingFileState(pf, PendState::Complete);
		progress++;
	}
}

void
VCoproc::TimeoutWaitingRequests()
{
	for (auto &kv : pending) {
		auto &pf = kv.second;

		if (pf->State() == PendState::WaitingResponse &&
		    pf->InactivitySeconds() > 120.0) {
			SetPendingFileState(pf, PendState::ProcFailure);
			progress++;
			std::cerr << logb(LogErr) << "File " << pf->FileName()
				  << " timed out" << std::endl;
			stats.files_failed++;
			stats.bytes_failed += pf->FileSize();
			stats.files_timedout++;
			stats.bytes_timedout += pf->FileSize();
		}
	}
}

int
VCoproc::UpdateStatistics(bool force = false)
{
	float diff_seconds = SecsElapsed(stats_start);
	std::stringstream qss;

	if (!force && diff_seconds < stats_period) {
		return 0;
	}

	qss << "INSERT INTO stats(timestamp, diffseconds, files_scored, "
	       "bytes_scored, audiosec_scored, "
	       "speechsec_scored, "
	       "files_nomdata, bytes_nomdata, "
	       "audiosec_nomdata, "
	       "files_failed, bytes_failed, files_timedout, "
	       "bytes_timedout, "
	       "files_completed, bytes_completed, "
	       "procsec_completed) "
	       "VALUES(strftime('%s','now'), "
	    << diff_seconds << "," << stats.files_scored << ","
	    << stats.bytes_scored << "," << stats.audiosec_scored << ","
	    << stats.speechsec_scored << "," << stats.files_nomdata << ","
	    << stats.bytes_nomdata << "," << stats.audiosec_nomdata << ","
	    << stats.files_failed << "," << stats.bytes_failed << ","
	    << stats.files_timedout << "," << stats.bytes_timedout << ","
	    << stats.files_completed << "," << stats.bytes_completed << ","
	    << stats.procsec_completed << ")";
	if (dbconn->ModifyStmt(qss, verbose)) {
		return -1;
	}

	/* Apply the retention policy to the stats table. */
	time_t retention_t = time(nullptr);

	retention_t -= retention_days * 24 * 60 * 60;
	qss = std::stringstream();
	qss << "DELETE FROM stats WHERE timestamp < " << retention_t;
	dbconn->ModifyStmt(qss, verbose);

	if (diff_seconds > 0.0) {
		double fcmps =
		    static_cast<double>(stats.files_completed) / diff_seconds;
		double bcmps = static_cast<double>(stats.bytes_completed) /
			       diff_seconds / 1024.0 / 1024.0;
		double fscps =
		    static_cast<double>(stats.files_scored) / diff_seconds;
		double bscps = static_cast<double>(stats.bytes_scored) /
			       diff_seconds / 1024.0 / 1024.0;
		double amscps = stats.audiosec_scored / diff_seconds / 60.0;
		double smscps = stats.speechsec_scored / diff_seconds / 60.0;
		double fnmps =
		    static_cast<double>(stats.files_nomdata) / diff_seconds;
		double bnmps = static_cast<double>(stats.bytes_nomdata) /
			       diff_seconds / 1024.0 / 1024.0;
		double fflps =
		    static_cast<double>(stats.files_failed) / diff_seconds;
		double bflps = static_cast<double>(stats.bytes_failed) /
			       diff_seconds / 1024.0 / 1024.0;
		std::cout << "| total " << fcmps << " fps, " << bcmps
			  << " MBps | "
			  << "scored " << fscps << " fps, " << bscps
			  << " MBps, " << amscps << " amps, " << smscps
			  << " smps | "
			  << "nomdata " << fnmps << " fps, " << bnmps
			  << " MBps | "
			  << "failed " << fflps << " fps, " << bflps
			  << " MBps |" << std::endl
			  << std::flush;
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
				 * This happens if a signal was
				 * caught during poll. We just
				 * continue, so that the signal
				 * handler can write to the
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
			 * We got a termination signal. Return 1
			 * to inform the user.
			 */
			assert(pfd[0].revents & POLLIN);
			EventFdDrain(stopfd);

			return 1;
		}

		/*
		 * It's safer to have the sleep before the first
		 * check to rate limit this function (e.g., if
		 * Probe() always returns true but there is some
		 * issue that triggers the MainLoop to call
		 * WaitForBackend() again and again).
		 */
	} while (be->Probe() == false);

	/* Backend is online. We can return. */
	return 0;
}

void
VCoproc::DumpPendingTable() const
{
	std::cout << "Pending Table:" << std::endl;
	for (const auto &kv : pending) {
		const auto &pf = kv.second;

		std::cout << "    " << pf->FileName() << ": " << pf->StateStr()
			  << std::endl;
	}
	std::cout << "===============================" << std::endl;
}

bool
VCoproc::AnyPendingActivity(int num_running_curls) const
{
	if (num_running_curls > 0) {
		return true;
	}

	for (auto &kv : pending) {
		auto &pf = kv.second;

		if (pf->State() != PendState::Complete) {
			return true;
		}
	}

	return false;
}

bool
VCoproc::AnyImmediateAction() const
{
	for (auto &kv : pending) {
		auto &pf = kv.second;

		switch (pf->State()) {
		case PendState::New:
		case PendState::ReadyToSubmit:
		case PendState::ProcSuccess:
		case PendState::ProcFailure:
		case PendState::Complete:
			return true;
			break;
		default:
			break;
		}
	}

	return false;
}

int
VCoproc::MainLoop()
{
	int num_running_curls = 0;
	int err		      = 0;

	while (!bail_out) {
		CURLMcode cm;

		/*
		 * Refill the pending table by fetching more
		 * files from the input directories.
		 */
		size_t new_files = FetchMoreFiles();

		/*
		 * When there are no more files to be processed
		 * or pending activities, stop if we are not in
		 * monitor mode.
		 */
		if (!monitor && new_files == 0 &&
		    !AnyPendingActivity(num_running_curls)) {
			break;
		}

		/*
		 * Wait for any activity on CURL transfers or on the stop
		 * file descriptor. We set a zero timeout if there is any
		 * MainLoop operation that can be acted on immediately
		 * (i.e., without waiting). Otherwise we set a non-zero
		 * timeout and possibly wait.
		 */
		int timeout_ms = AnyImmediateAction() ? 0 : 5000;
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
			/* We got a signal asking us to stop. */
			EventFdDrain(stopfd);
			if (verbose) {
				std::cout << "Stopping the event loop"
					  << std::endl;
			}
			break;
		}

		/* Scan any new entries and carry out some
		 * pre-processing. */
		PreProcessNewFiles();

		/*
		 * Scan ReadyToSubmit entries, preparing the
		 * next POST request to be submitted to the
		 * backend engine.
		 */
		PreparePostRequests();

		/* Submit or advance any pending POST transfers.
		 */
		cm = curl_multi_perform(curlm, &num_running_curls);
		if (cm != CURLM_OK) {
			std::cerr << "Failed to perform multi "
				     "handle: "
				  << curl_multi_strerror(cm) << std::endl;
			break;
		}

		/* Retire and process any completed POST
		 * operations. */
		if (!RetireAndProcessPostResponses()) {
			/*
			 * The backend went down for some
			 * reason. Flush any pending requests
			 * and wait for the backend to go back
			 * online.
			 */
			int ret;

			/*
			 * We could clear only the ones in
			 * waiting state, but we won't bother
			 * because it's harmless to reprocess
			 * already processed files.
			 */
			pending.clear();

			std::cout << "Backend went offline. "
				     "Waiting ..."
				  << std::endl;
			ret = WaitForBackend();
			if (ret != 0) {
				/*
				 * Stop on error (ret < 0) or
				 * because we got the
				 * termination signal (ret > 0).
				 */
				break;
			}
			std::cout << "Backend is back online!" << std::endl;

			/*
			 * It's convenient to start from the
			 * beginning of the iteration, so that
			 * we fetch more files, including the
			 * ones to reprocess.
			 */
			continue;
		}

		/*
		 * Post process any entries in ProcSuccess or
		 * ProcFailure state.
		 */
		PostProcessFiles();

		/*
		 * Mark timed out entries as failed. They will
		 * be removed during the next step.
		 */
		TimeoutWaitingRequests();

		/*
		 * Remove any completed entries from the
		 * pending table, to make space for new input
		 * files.
		 */
		CleanupCompletedFiles();

		/* Update the statistics if necessary. */
		UpdateStatistics();
	}

	UpdateStatistics(/*force=*/true);

	return err;
}

int
main(int argc, char **argv)
{
	std::vector<std::string> input_dirs;
	std::vector<std::string> input_exts;
	unsigned short max_pending  = 5;
	unsigned short dir_min_age  = 0;
	unsigned int stats_period   = 300;
	unsigned int retention_days = 7;
	std::string output_dir;
	std::string failed_dir;
	std::string forward_dir;
	std::string archive_dir;
	std::string dbfile;
	std::string source;
	std::string host;
	unsigned short port = 0;
	struct sigaction sa;
	int verbose	       = 0;
	bool consume	       = false;
	bool monitor	       = false;
	bool compress_archived = false;
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

	while ((opt = getopt(argc, argv,
			     "hVvi:o:F:f:a:CcmD:H:p:s:e:n:T:R:A")) != -1) {
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

		case 'a':
			if (!DirExists(optarg)) {
				std::cerr << logb(LogErr) << "Directory "
					  << optarg << " not found "
					  << std::endl;
				return -1;
			}
			archive_dir = std::string(optarg);
			break;

		case 'C':
			compress_archived = true;
			break;

		case 'c':
			consume = true;
			break;

		case 'm':
			monitor = true;
			break;

		case 'n':
			if (!Str2Num<unsigned short>(optarg, max_pending)) {
				std::cerr << logb(LogErr)
					  << "Invalid value for -n: " << optarg
					  << std::endl;
				return -1;
			}
			break;

		case 'A':
			if (!Str2Num<unsigned short>(optarg, dir_min_age)) {
				std::cerr << logb(LogErr)
					  << "Invalid value for -A: " << optarg
					  << std::endl;
				return -1;
			}
			break;

		case 'T':
			if (!Str2Num<unsigned int>(optarg, stats_period)) {
				std::cerr << logb(LogErr)
					  << "Invalid value for -T: " << optarg
					  << std::endl;
				return -1;
			}
			break;

		case 'R':
			if (!Str2Num<unsigned int>(optarg, retention_days)) {
				std::cerr << logb(LogErr)
					  << "Invalid value for -R: " << optarg
					  << std::endl;
				return -1;
			}
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

		case 'e': {
			std::string ext = std::string(optarg);

			if (!std::regex_match(ext,
					      std::regex("[a-zA-Z0-9]+"))) {
				std::cerr << logb(LogErr)
					  << "Invalid extension " << ext
					  << std::endl;
				return -1;
			}
			input_exts.push_back(ext);
			break;
		}
		}
	}

	curl_global_init(CURL_GLOBAL_ALL);

	auto vcoproc = VCoproc::Create(
	    stopfd_global, verbose, consume, monitor, std::move(source),
	    std::move(input_dirs), std::move(input_exts), std::move(output_dir),
	    std::move(failed_dir), std::move(forward_dir),
	    std::move(archive_dir), compress_archived, max_pending, dir_min_age,
	    stats_period, retention_days, dbfile, host, port);
	if (vcoproc == nullptr) {
		return -1;
	}

	return vcoproc->MainLoop();
}
