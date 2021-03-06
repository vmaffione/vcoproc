#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <curl/curl.h>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <poll.h>
#include <regex>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/eventfd.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "json11.hpp"
#include "sql.hpp"
#include "utils.hpp"
#include "version.h"

using namespace sql;
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
	    << "    -D DB_FILE (mysql connection string or path to sqlite3 "
	       "database file)"
	    << std::endl
	    << "    -c (consume input files)" << std::endl
	    << "    -m (monitor input directories rather than stop when "
	       "running out of files)"
	    << std::endl
	    << "    -n MAX_PENDING_TRANSACTIONS (max number of concurrent "
	       "processing transactions)"
	    << std::endl
	    << "    -t TIMEOUT_SECS (timeout for each POST request; defaults "
	       "to 120)"
	    << std::endl
	    << "    -r MAX_RETRIES (maximum number of retries; defaults to 0)"
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
	    << "    -B BACKEND_HOST:BACKEND_PORT (address of the backend "
	       "engine)"
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

struct BackendSpec {
	std::string host;
	unsigned short port;

	BackendSpec(std::string h, unsigned short p)
	    : host(std::move(h)), port(p)
	{
	}
};

class BackendTransaction {
	int retry = 0;

    public:
	BackendTransaction(int retry) : retry(retry) {}
	int Retry() const { return retry; }
	virtual int PrepareRequest(std::string &url, std::string &req) = 0;
	virtual int ProcessResponse(std::string &resp)		       = 0;

	/* Accumulator for the JSON output associated to this transaction. */
	json11::Json::object jout;
};

class Backend {
    public:
	virtual bool Probe() = 0;
	virtual std::unique_ptr<BackendTransaction> CreateTransaction(
	    const std::string &filepath, int retry = 0) = 0;
};

enum class PendState {
	New		= 0,
	ReadyToSubmit	= 1,
	WaitingResponse = 2,
	ProcSuccess	= 3,
	ProcFailure	= 4,
	Complete	= 5,
	TimeoutRetry	= 6,
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

	std::vector<std::unique_ptr<BackendTransaction>> bts;
	size_t bt_idx = 0;

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
	PendingFile(std::vector<std::unique_ptr<BackendTransaction>> bts,
		    CURLM *curlm, CURL *curl, const std::string &src_path,
		    size_t src_size, int verbose)
	    : bts(std::move(bts)),
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
	    std::vector<std::unique_ptr<BackendTransaction>> bts, CURLM *curlm,
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
	BackendTransaction *CurBeTransaction() const;
	bool NextBeTransaction();
	int MergeTransactionsResults(json11::Json::object &jout) const;
};

std::unique_ptr<PendingFile>
PendingFile::Create(std::vector<std::unique_ptr<BackendTransaction>> bts,
		    CURLM *curlm, const std::string &src_path, int verbose)
{
	CURLcode cc;
	CURL *curl;

	if (curlm == nullptr) {
		return nullptr;
	}

	for (const auto &bt : bts) {
		if (bt == nullptr) {
			return nullptr;
		}
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
	    std::move(bts), curlm, curl, src_path,
	    static_cast<size_t>(src_size), verbose);

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
		/*
		 * Remove the easy handle from the multi handle. This is
		 * necessary in case the easy handle was not removed by
		 * RetirePostCurl() because the operation is still in progress
		 * and we want to cancel it.
		 */
		if (curlm != nullptr) {
			CURLMcode cm = curl_multi_remove_handle(curlm, curl);
			if (cm != CURLM_OK) {
				std::cerr << "Failed to remove handle from "
					     "multi stack: "
					  << curl_multi_strerror(cm)
					  << std::endl;
			}
		}
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
	case PendState::TimeoutRetry:
		return "TimeoutRetry";
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

BackendTransaction *
PendingFile::CurBeTransaction() const
{
	if (bt_idx >= bts.size()) {
		return nullptr;
	}

	return bts[bt_idx].get();
}

bool
PendingFile::NextBeTransaction()
{
	if (bt_idx >= bts.size()) {
		return false;
	}

	return ++bt_idx < bts.size();
}

int
PendingFile::MergeTransactionsResults(json11::Json::object &jout) const
{
	json11::Json::array ar;
	enum { StatusError = 0, StatusNometadata = 1, StatusComplete = 2 };
	int best_status = StatusError;

	jout = json11::Json::object();

	for (size_t bi = 0; bi < bts.size(); bi++) {
		if (bts[bi] == nullptr) {
			continue;
		}

		auto &oj      = bts[bi]->jout;
		double weight = (oj["asr"] == "NVSL") ? 0.7 : 1.0;

		/*
		 * Here it's safe to assume that scores are numeric, and
		 * that a score key is always associated with a label key.
		 */
		if (oj.count("gender") &&
		    (!jout.count("gender") ||
		     oj["gender_score"].number_value() >
			 jout["gender_score"].number_value())) {
			jout["gender"]	     = oj["gender"];
			jout["gender_score"] = oj["gender_score"];
		}

		if (oj.count("language_iso") &&
		    (!jout.count("language_iso") ||
		     weight * oj["language_score"].number_value() >
			 jout["language_score"].number_value())) {
			jout["language_iso"]   = oj["language_iso"];
			jout["language_score"] = oj["language_score"];
		}

		if (oj.count("speaker") &&
		    (!jout.count("speaker") ||
		     oj["speaker_score"].number_value() >
			 jout["speaker_score"].number_value())) {
			jout["speaker"]	      = oj["speaker"];
			jout["speaker_score"] = oj["speaker_score"];
		}

		/* We already know we have a status. */
		int status = 0;
		if (oj["status"] == "COMPLETE") {
			status = StatusComplete;
		} else if (oj["status"] == "NOMETADATA") {
			status = StatusNometadata;
		} else if (oj["status"] == "ERROR") {
			status = StatusError;
		}
		best_status = std::max(status, best_status);

		/* Merge missing fields. */
		for (const auto &kv : oj) {
			if (!jout.count(kv.first)) {
				jout[kv.first] = kv.second;
			}
		}

		/* Append backend results for reference. */
		ar.push_back(oj);
	}

	switch (best_status) {
	case StatusError:
		jout["status"] = "ERROR";
		break;

	case StatusNometadata:
		jout["status"] = "NOMETADATA";
		break;

	case StatusComplete:
		jout["status"] = "COMPLETE";
		break;
	}
	jout["backends"] = ar;

	return 0;
}

class LibraryBackend : public Backend {
	std::string int_url;
	std::string bat_url;

    public:
	static std::unique_ptr<Backend> Create(const BackendSpec &bspec);
	LibraryBackend(const BackendSpec &bspec);
	virtual bool Probe();
	std::unique_ptr<BackendTransaction> CreateTransaction(
	    const std::string &filepath, int retry);
	std::string InteractiveURL() const { return int_url; }
	std::string BatchURL() const { return bat_url; }
};

class LibraryBackendTransaction : public BackendTransaction {
	LibraryBackend *be = nullptr;
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
	LibraryBackendTransaction(Backend *be, const std::string &filepath,
				  int retry)
	    : BackendTransaction(retry),
	      be(dynamic_cast<LibraryBackend *>(be)),
	      filepath(filepath)
	{
		assert(this->be != nullptr);
	}
	int PrepareRequest(std::string &url, std::string &req);
	int ProcessResponse(std::string &resp);
};

int
LibraryBackendTransaction::PrepareRequest(std::string &url, std::string &req)
{
	json11::Json::object jsreq;

	switch (state) {
	case State::Init: {
		url   = be->InteractiveURL() + "/ping";
		state = State::WaitForPing;
		break;
	}

	case State::ReadyToProcess: {
		url		   = be->BatchURL() + "/process";
		jsreq["file_name"] = filepath;
		if (Retry() > 0) {
			/* Disable STT if this is a retry. */
			jsreq["stt_mode"] = "";
		}
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

	req = json11::Json(jsreq).dump();

	return 0;
}

int
LibraryBackendTransaction::ProcessResponse(std::string &resp)
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
LibraryBackend::Create(const BackendSpec &bspec)
{
	return std::make_unique<LibraryBackend>(bspec);
}

LibraryBackend::LibraryBackend(const BackendSpec &bspec)
{
	std::stringstream ss;

	ss << "http://" << bspec.host << ":" << bspec.port;
	int_url = ss.str();

	ss = std::stringstream();
	ss << "http://" << bspec.host << ":" << bspec.port + 1;
	bat_url = ss.str();
}

bool
LibraryBackend::Probe()
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
LibraryBackend::CreateTransaction(const std::string &filepath, int retry)
{
	return std::make_unique<LibraryBackendTransaction>(this, filepath,
							   retry);
}

/* Main class. */
class VCoproc {
	int stopfd   = -1; /* owned by the caller, not by us */
	CURLM *curlm = nullptr;
	std::vector<std::unique_ptr<Backend>> backends;
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
	 * Maximum period of inactivity for a given pending file before
	 * we decide it timed out.
	 */
	unsigned short timeout_secs = 120;

	/*
	 * How many times we retry the processing in case of timeout.
	 * STT gets disabled after the first timeout.
	 */
	unsigned short max_retries = 0;

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
	unsigned int retention_days = 60;

	struct DbSpec dbspec;
	std::unique_ptr<DbConn> dbconn;
	std::vector<BackendSpec> bspecs;
	size_t input_dir_idx = 0;

	/*
	 * Set by any routine in the MainLoop on unrecoverable
	 * errors. The MainLoop will stop ASAP.
	 * TODO currently only set by PostProcessFiles(). If
	 * this is the final situation, get rid of this variable
	 * and use the PostProcessFiles() return value.
	 */
	bool bail_out = false;

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
	bool FetchMoreFiles();
	int FetchFilesFromDir(const DFSDir &dfsdir,
			      std::deque<DFSDir> &frontier, int &credits);
	bool AnyPendingActivity(int num_running_curls) const;
	bool AnyImmediateAction() const;
	void PreProcessNewFiles();
	void PreparePostRequests();
	bool RetireAndProcessPostResponses();
	void FinalizeOutput(PendingFile *pf, json11::Json::object &jout,
			    bool success);
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
	    unsigned short max_pending, unsigned short timeout_secs,
	    unsigned short max_retries, unsigned short dir_min_age,
	    unsigned int stats_period, unsigned int retention_days,
	    struct DbSpec dbspec, std::vector<BackendSpec> bspecs);

	VCoproc(int stopfd, CURLM *curlm, std::vector<std::unique_ptr<Backend>>,
		int verbose, bool consume, bool monitor, std::string source,
		std::vector<std::string> input_dirs,
		std::vector<std::string> input_exts, std::string output_dir,
		std::string failed_dir, std::string forward_dir,
		std::string archive_dir, bool compress_archived,
		unsigned short max_pending, unsigned short timeout_secs,
		unsigned short max_retries, unsigned short dir_min_age,
		unsigned int stats_period, unsigned int retention_days,
		struct DbSpec dbspec, std::unique_ptr<DbConn> dbconn,
		std::vector<BackendSpec> bspecs);
	~VCoproc();
	int MainLoop();
};

std::unique_ptr<VCoproc>
VCoproc::Create(int stopfd, int verbose, bool consume, bool monitor,
		std::string source, std::vector<std::string> input_dirs,
		std::vector<std::string> input_exts, std::string output_dir,
		std::string failed_dir, std::string forward_dir,
		std::string archive_dir, bool compress_archived,
		unsigned short max_pending, unsigned short timeout_secs,
		unsigned short max_retries, unsigned short dir_min_age,
		unsigned int stats_period, unsigned int retention_days,
		struct DbSpec dbspec, std::vector<BackendSpec> bspecs)
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

	if (timeout_secs < 1 || timeout_secs > 300) {
		std::cerr << logb(LogErr) << "Timeout seconds out of range"
			  << std::endl;
		return nullptr;
	}

	if (max_retries < 0 || max_retries > 4) {
		std::cerr << logb(LogErr) << "Max retries out of range"
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

	if (!dbspec.IsMySQL() && !dbspec.IsSQLite()) {
		std::cerr << logb(LogErr) << "No valid database specified (-D)"
			  << std::endl;
		return nullptr;
	}

	if (bspecs.empty()) {
		std::cerr << logb(LogErr) << "No backend specified (-B)"
			  << std::endl;
		return nullptr;
	}

	if (monitor) {
		/* We must consume in monitor mode. */
		consume = true;
	}

	/* Create the backends. */
	std::vector<std::unique_ptr<Backend>> backends;

	for (size_t bi = 0; bi < bspecs.size(); bi++) {
		auto be = LibraryBackend::Create(bspecs[bi]);

		if (be == nullptr) {
			std::cerr << logb(LogErr)
				  << "Failed to create backend #" << bi + 1
				  << std::endl;
			return nullptr;
		}

		backends.push_back(std::move(be));
	}

	/* Open a (long-lived) database connection. */
	std::unique_ptr<DbConn> dbconn;

	if (dbspec.IsMySQL()) {
		dbconn = MySQLDbConn::Create(dbspec);
		if (dbconn == nullptr) {
			std::cerr << logb(LogErr)
				  << "Failed to connect to MySQL database "
				  << dbspec.Repr() << std::endl;
			return nullptr;
		}

	} else if (dbspec.IsSQLite()) {
		dbconn = SQLiteDbConn::Create(dbspec.dbfile);
		if (dbconn == nullptr) {
			std::cerr << logb(LogErr)
				  << "Failed to open SQLite database "
				  << dbspec.dbfile << std::endl;
			return nullptr;
		}

	} else {
		std::cerr << logb(LogErr) << "No suitable database to open"
			  << std::endl;
		return nullptr;
	}

	/* Create the stats table if it does not exist
	 * already. */
	{
		if (dbspec.tablename.empty()) {
			dbspec.tablename = "stats";
		}

		std::stringstream qss;
		qss << "CREATE TABLE IF NOT EXISTS " << dbspec.tablename << " ("
		    << "timestamp INTEGER UNSIGNED PRIMARY KEY "
		       "NOT NULL, "
		    << "diffseconds DOUBLE "
		       "NOT NULL CHECK(diffseconds > 0.0), "
		    << "files_scored INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "bytes_scored INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "audiosec_scored DOUBLE NOT NULL, "
		    << "speechsec_scored DOUBLE NOT NULL, "
		    << "files_nomdata INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "bytes_nomdata INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "audiosec_nomdata DOUBLE NOT NULL, "
		    << "files_failed INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "bytes_failed INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "files_timedout INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "bytes_timedout INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "files_completed INTEGER UNSIGNED NOT "
		       "NULL, "
		    << "bytes_completed INTEGER UNSIGNED NOT "
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
	    stopfd, curlm, std::move(backends), verbose, consume, monitor,
	    std::move(source), std::move(input_dirs), std::move(input_exts),
	    std::move(output_dir), std::move(failed_dir),
	    std::move(forward_dir), std::move(archive_dir), compress_archived,
	    max_pending, timeout_secs, max_retries, dir_min_age, stats_period,
	    retention_days, std::move(dbspec), std::move(dbconn),
	    std::move(bspecs));
}

VCoproc::VCoproc(int stopfd, CURLM *curlm,
		 std::vector<std::unique_ptr<Backend>> backends, int verbose,
		 bool consume, bool monitor, std::string source,
		 std::vector<std::string> input_dirs,
		 std::vector<std::string> input_exts, std::string output_dir,
		 std::string failed_dir, std::string forward_dir,
		 std::string archive_dir, bool compress_archived,
		 unsigned short max_pending, unsigned short timeout_secs,
		 unsigned short max_retries, unsigned short dir_min_age,
		 unsigned int stats_period, unsigned int retention_days,
		 struct DbSpec dbspec, std::unique_ptr<DbConn> dbconn,
		 std::vector<BackendSpec> bspecs)
    : stopfd(stopfd),
      curlm(curlm),
      backends(std::move(backends)),
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
      timeout_secs(timeout_secs),
      max_retries(max_retries),
      dir_min_age(dir_min_age),
      stats_period(stats_period),
      retention_days(retention_days),
      dbspec(std::move(dbspec)),
      dbconn(std::move(dbconn)),
      bspecs(std::move(bspecs)),
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

bool
VCoproc::FetchMoreFiles()
{
	int credits = max_pending;

	if (inprogress_counter >= max_pending) {
		/* We're full. Don't even try. */
		return true;
	}

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

			frontier.pop_back();

			if (dfsdir.depth >= 32) {
				std::cout << "Possible loop detected at "
					  << dfsdir.path << std::endl;
				continue;
			}

			FetchFilesFromDir(dfsdir, frontier, credits);
		}
		if (++input_dir_idx >= input_dirs.size()) {
			input_dir_idx = 0;
		}
	}

	/*
	 * If we ran out of credits it (likely) means that there are more
	 * files. If we did not consume all the credits, it means that
	 * there are no more files at the moment.
	 */
	return credits <= 0;
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
			/*
			 * If we find an empty directory that has not been
			 * modified in a short while, we remove it. The age
			 * check prevents situations where we remove
			 * directories created by the input producer before the
			 * producer has the chance to move something inside.
			 * Note that this should not happen if the producer
			 * employs a strict "rsync" convention. The removal
			 * also helps to do less DFS work.
			 */
			if (dir_min_age > 0 &&
			    FileAgeSeconds(path) < dir_min_age) {
				continue;
			}

			/*
			 * We use rmdir() also to figure out whether the
			 * directory is empty or not. This is more efficient
			 * than using DirEmpty().
			 */
			int r = rmdir(path.c_str());
			if (r == 0) {
				if (verbose) {
					std::cout << logb(LogDbg)
						  << "Removed empty directory "
						  << path << std::endl
						  << std::flush;
				}
			} else if (errno != EEXIST && errno != ENOTEMPTY) {
				std::cerr << logb(LogErr)
					  << "Failed to "
					     "remove empty directory "
					  << path << ": " << strerror(errno)
					  << std::endl
					  << std::flush;
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
			std::vector<std::unique_ptr<BackendTransaction>> bts;

			for (size_t bi = 0; bi < backends.size(); bi++) {
				bts.emplace_back(
				    backends[bi]->CreateTransaction(path));
			}

			pending[path] = std::move(PendingFile::Create(
			    std::move(bts), curlm, path, verbose));
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

		/* Retry to process this file after the timeout. */
		if (pf->State() == PendState::TimeoutRetry) {
			std::vector<std::unique_ptr<BackendTransaction>> bts;

			for (size_t bi = 0; bi < backends.size(); bi++) {
				bts.emplace_back(
				    backends[bi]->CreateTransaction(
					pf->FilePath(),
					pf->CurBeTransaction()->Retry() + 1));
			}

			pf = std::move(PendingFile::Create(
			    std::move(bts), curlm, pf->FilePath(), verbose));
			if (verbose) {
				std::cout << logb(LogDbg)
					  << "Reprocessing file "
					  << pf->FileName() << std::endl;
			}
			continue;
		}

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

		if (utils::FileSize(pf->FilePath()) == 0) {
			/*
			 * If the file is empty, don't even bother processing
			 * it.
			 */
			json11::Json::object jout;

			jout["status"]	   = "NOMETADATA";
			jout["length"]	   = 0.0;
			jout["net_speech"] = 0.0;

			FinalizeOutput(pf.get(), jout, /*success=*/true);
		} else {
			SetPendingFileState(pf, PendState::ReadyToSubmit);
		}
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

		std::string req;
		std::string url;

		/* Get the URL and content for the next POST. */
		int r = pf->CurBeTransaction()->PrepareRequest(url, req);
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

		if (http_code == 0) {
			/*
			 * Backend down. No reason to advance
			 * the pending requests. Those will be
			 * reprocessed.
			 */
			return false;
		}

		bool success	       = (http_code == 200);
		BackendTransaction *bt = pf->CurBeTransaction();
		std::string resp;

		if (pf->RetirePostCurl(resp)) {
			success = false;
		} else {
			int r = bt->ProcessResponse(resp);
			if (r == 0) {
				/*
				 * The transaction is not
				 * complete yet.
				 */
				SetPendingFileState(pf,
						    PendState::ReadyToSubmit);
				continue;
			}
			success = r > 0;
		}

		/*
		 * The current transaction is complete. Perform some sanity
		 * checks before going ahead.
		 */
		if (verbose) {
			std::cout << logb(LogDbg) << "Processed "
				  << pf->FileName() << " --> " << http_code
				  << " " << json11::Json(bt->jout).dump()
				  << std::endl;
		}

		if (!bt->jout.count("status")) {
			std::cerr << logb(LogErr) << "Missing status key"
				  << std::endl;
			success	 = false;
			bt->jout = json11::Json::object(); /* drastic */
		}

		if (bt->jout.count("gender") !=
			bt->jout.count("gender_score") ||
		    (bt->jout.count("gender_score") &&
		     !bt->jout["gender_score"].is_number())) {
			std::cerr << logb(LogErr) << "Invalid gender results"
				  << std::endl;
			success	 = false;
			bt->jout = json11::Json::object();
		}

		if (bt->jout.count("language_iso") +
			    bt->jout.count("language_ietf") !=
			bt->jout.count("language_score") ||
		    (bt->jout.count("language_score") &&
		     !bt->jout["language_score"].is_number())) {
			std::cerr << logb(LogErr) << "Invalid language results"
				  << std::endl;
			success	 = false;
			bt->jout = json11::Json::object();
		}

		if (bt->jout.count("speaker") !=
			bt->jout.count("speaker_score") ||
		    (bt->jout.count("speaker_score") &&
		     !bt->jout["speaker_score"].is_number())) {
			std::cerr << logb(LogErr) << "Invalid speaker results"
				  << std::endl;
			success	 = false;
			bt->jout = json11::Json::object();
		}

		if (pf->NextBeTransaction()) {
			/* More transactions are pending. */
			SetPendingFileState(pf, PendState::ReadyToSubmit);
			continue;
		}

		/*
		 * All transactions are complete. Merge all the JSON results
		 * into a single one, then finalize and output.
		 */
		json11::Json::object jout;
		pf->MergeTransactionsResults(jout);
		FinalizeOutput(pf, jout, success);
	}

	return true;
}

void
VCoproc::FinalizeOutput(PendingFile *pf, json11::Json::object &jout,
			bool success)
{
	double audio_len  = 0;
	double speech_len = 0;

	jout["asr"]	    = "VCoProc";
	jout["asr_version"] = VC_VERSION;

	if (!jout.count("status")) {
		std::cerr << logb(LogErr) << "Missing status key" << std::endl;
		success = false;
	} else {
		success = (jout["status"] == "COMPLETE") ||
			  (jout["status"] == "NOMETADATA");
	}

	if (jout.count("length") && jout["length"].is_number()) {
		audio_len = jout["length"].number_value();
	}
	if (jout.count("net_speech") && jout["net_speech"].is_number()) {
		speech_len = jout["net_speech"].number_value();
	}

	if (success) {
		/* Output JSON. */
		std::stringstream jsname;
		std::string jspath;

		jout["origin"] = source;

		json11::Json jmdata = pf->GetMetadata();
		if (jmdata != json11::Json()) {
			jout["metadata"] = jmdata;
		}

		jsname
		    << source << "_"
		    << std::chrono::duration_cast<std::chrono::microseconds>(
			   std::chrono::system_clock::now().time_since_epoch())
			   .count()
		    << ".json";

		jspath = PathJoin(output_dir, jsname.str());
		std::ofstream fout(jspath);
		fout << json11::Json(jout).dump();
		fout << std::endl;
	}

	if (!success) {
		SetPendingFileState(pf, PendState::ProcFailure);
		stats.files_failed++;
		stats.bytes_failed += pf->FileSize();
	} else {
		SetPendingFileState(pf, PendState::ProcSuccess);
		if (jout["status"] == "COMPLETE") {
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

		/* Timeout check. */
		if (pf->State() == PendState::WaitingResponse &&
		    pf->InactivitySeconds() >
			static_cast<float>(timeout_secs)) {
			std::cerr << logb(LogErr) << "File " << pf->FileName()
				  << " timed out" << std::endl;
			if (pf->CurBeTransaction()->Retry() < max_retries) {
				SetPendingFileState(pf,
						    PendState::TimeoutRetry);
				stats.files_timedout++;
				stats.bytes_timedout += pf->FileSize();
			} else {
				SetPendingFileState(pf, PendState::ProcFailure);
				stats.files_failed++;
				stats.bytes_failed += pf->FileSize();
			}
		}

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

	qss << "INSERT INTO " << dbspec.tablename
	    << "(timestamp, diffseconds, files_scored, "
	       "bytes_scored, audiosec_scored, "
	       "speechsec_scored, "
	       "files_nomdata, bytes_nomdata, "
	       "audiosec_nomdata, "
	       "files_failed, bytes_failed, files_timedout, "
	       "bytes_timedout, "
	       "files_completed, bytes_completed, "
	       "procsec_completed) "
	       "VALUES("
	    << dbconn->CurrentUnixTime() << ", " << diff_seconds << ","
	    << stats.files_scored << "," << stats.bytes_scored << ","
	    << stats.audiosec_scored << "," << stats.speechsec_scored << ","
	    << stats.files_nomdata << "," << stats.bytes_nomdata << ","
	    << stats.audiosec_nomdata << "," << stats.files_failed << ","
	    << stats.bytes_failed << "," << stats.files_timedout << ","
	    << stats.bytes_timedout << "," << stats.files_completed << ","
	    << stats.bytes_completed << "," << stats.procsec_completed << ")";
	if (dbconn->ModifyStmt(qss, verbose)) {
		return -1;
	}

	/* Apply the retention policy to the stats table. */
	time_t retention_t = time(nullptr);

	retention_t -= retention_days * 24 * 60 * 60;
	qss = std::stringstream();
	qss << "DELETE FROM " << dbspec.tablename << " WHERE timestamp < "
	    << retention_t;
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

	for (;;) {
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
		bool everybody_up = true;

		for (size_t bi = 0; bi < backends.size(); bi++) {
			if (!backends[bi]->Probe()) {
				everybody_up = false;
				break;
			}
		}

		if (everybody_up) {
			break;
		}
	}

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
		bool more_files = FetchMoreFiles();

		/*
		 * When there are no more files to be processed
		 * or pending activities, stop if we are not in
		 * monitor mode.
		 */
		if (!monitor && !more_files &&
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

		int timeout_ms = (AnyImmediateAction() ||
				  (num_running_curls == 0 && more_files))
				     ? 0
				     : std::min(5000, timeout_secs * 1000);
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

		/*
		 * Scan any new entries and carry out some
		 * pre-processing.
		 */
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
			inprogress_counter = 0;

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
		 * Post process any entries in ProcSuccess or ProcFailure state.
		 * Also mark timed out entries as failed. They will be removed
		 * during the next step.
		 */
		PostProcessFiles();

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
	unsigned short timeout_secs = 120;
	unsigned short dir_min_age  = 0;
	unsigned int stats_period   = 300;
	unsigned int retention_days = 60;
	unsigned short max_retries  = 0;
	std::string output_dir;
	std::string failed_dir;
	std::string forward_dir;
	std::string archive_dir;
	std::string source;
	std::vector<BackendSpec> backend_specs;
	struct sigaction sa;
	int verbose	       = 0;
	bool consume	       = false;
	bool monitor	       = false;
	bool compress_archived = false;
	struct DbSpec dbspec;
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
			     "hVvi:o:F:f:a:CcmD:B:s:e:n:T:R:A:t:r:")) != -1) {
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

		case 'D': {
			/* Try to parse a connection string. */
			std::string connstr = optarg;
			std::regex e("([A-Za-z]+)=([^;]+);");
			std::sregex_iterator rit(connstr.begin(), connstr.end(),
						 e);
			std::sregex_iterator rend;

			if (rit != rend) {
				for (; rit != rend; rit++) {
					std::string key = (*rit)[1];
					std::string val = (*rit)[2];
					StrLower(key);
					if (key == "server") {
						dbspec.host = val;
					} else if (key == "port") {
						if (!Str2Num<unsigned short>(
							val, dbspec.port)) {
							std::cerr
							    << "Invalid Port "
							       "in connection "
							       "string"
							    << std::endl;
							return -1;
						}
					} else if (key == "database") {
						dbspec.dbname = val;
					} else if (key == "uid") {
						dbspec.user = val;
					} else if (key == "pwd") {
						dbspec.password = val;
					} else if (key == "table") {
						dbspec.tablename = val;
					}
				}
				dbspec.Dump();
			} else {
				/* Not a connection string. Assume it's a sqlite
				 * DB file. */
				dbspec.dbfile = connstr;
			}

			break;
		}

		case 'B': {
			std::string optstr = optarg;
			unsigned short port;
			std::smatch match;
			std::string host;

			if (!std::regex_match(
				optstr, match,
				std::regex("([a-zA-Z0-9.-]+):([1-9][0-9]*)"))) {
				std::cerr << logb(LogErr)
					  << "Invalid backend address "
					  << optarg << std::endl;
				return -1;
			}
			host = match[1];
			Str2Num<unsigned short>(match[2], port);
			backend_specs.emplace_back(
			    BackendSpec(std::move(host), port));
			break;
		}

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

		case 't':
			if (!Str2Num<unsigned short>(optarg, timeout_secs)) {
				std::cerr << logb(LogErr)
					  << "Invalid value for -t: " << optarg
					  << std::endl;
				return -1;
			}
			break;

		case 'r':
			if (!Str2Num<unsigned short>(optarg, max_retries)) {
				std::cerr << logb(LogErr)
					  << "Invalid max retries " << optarg
					  << std::endl;
				return -1;
			}
			break;

		default:
			return -1;
			break;
		}
	}

	curl_global_init(CURL_GLOBAL_ALL);

	auto vcoproc = VCoproc::Create(
	    stopfd_global, verbose, consume, monitor, std::move(source),
	    std::move(input_dirs), std::move(input_exts), std::move(output_dir),
	    std::move(failed_dir), std::move(forward_dir),
	    std::move(archive_dir), compress_archived, max_pending,
	    timeout_secs, max_retries, dir_min_age, stats_period,
	    retention_days, std::move(dbspec), std::move(backend_specs));
	if (vcoproc == nullptr) {
		return -1;
	}

	return vcoproc->MainLoop();
}
