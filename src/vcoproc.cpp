#include <algorithm>
#include <cassert>
#include <cstring>
#include <curl/curl.h>
#include <deque>
#include <dirent.h>
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
	    << "    -i INPUT_DIR (add directory to look for files)" << std::endl
	    << "    -o OUTPUT_DIR (directory to store "
	       "incoming files)"
	    << std::endl
	    << "    -D DB_FILE (path to the sqlite3 database file)" << std::endl
	    << "    -c (consume input files)" << std::endl
	    << "    -m (monitor input directories rather than stop when "
	       "running out of files)"
	    << "    -H BACKEND_HOST (address or name of the backend engine)"
	    << std::endl
	    << "    -p BACKEND_PORT (TCP port of the backend engine)"
	    << std::endl
	    << std::endl
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

	int ModifyStmt(const std::stringstream &ss, bool verbose);
	std::unique_ptr<SQLiteDbCursor> SelectStmt(const std::stringstream &ss,
						   bool verbose);
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
SQLiteDbConn::ModifyStmt(const std::stringstream &ss, bool verbose)
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
SQLiteDbConn::SelectStmt(const std::stringstream &ss, bool verbose)
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
	None	 = 0,
	New	 = 1,
	Waiting	 = 2,
	Success	 = 3,
	Failure	 = 4,
	Complete = 5,
};

/*
 * A class representing an entry in the pending table.
 */
class PendingProc {
    public:
	enum class CurlStatus {
		Idle	 = 0,
		Prepared = 1,
	};

    private:
	CURLM *curlm = nullptr;
	CURL *curl   = nullptr;
	std::string src_path;
	std::string postdata;

	CurlStatus curl_status = CurlStatus::Idle;
	ProcStatus proc_status = ProcStatus::New;

    public:
	PendingProc(CURLM *curlm, CURL *curl, const std::string &src_path)
	    : curlm(curlm), curl(curl), src_path(src_path)
	{
	}

	~PendingProc();

	ProcStatus Status() const { return proc_status; }
	void SetStatus(ProcStatus status) { proc_status = status; }
	std::string FilePath() const { return src_path; }
	static std::unique_ptr<PendingProc> Create(CURLM *curlm,
						   const std::string &src_path);
	int PreparePost(const std::string &url, const json11::Json &jsreq);
	int PostCompleted();
};

std::unique_ptr<PendingProc>
PendingProc::Create(CURLM *curlm, const std::string &src_path)
{
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

	CURLMcode cm = curl_multi_add_handle(curlm, curl);
	if (cm != CURLM_OK) {
		std::cerr << "Failed to add handle to multi stack: "
			  << curl_multi_strerror(cm) << std::endl;
		return nullptr;
	}

	auto proc = std::make_unique<PendingProc>(curlm, curl, src_path);

	CURLcode cc =
	    curl_easy_setopt(curl, CURLOPT_PRIVATE, (void *)proc.get());
	if (cc != CURLE_OK) {
		std::cerr << "Failed to set CURLOPT_PRIVATE: "
			  << curl_easy_strerror(cc) << std::endl;
		return nullptr;
	}

	return proc;
}

PendingProc::~PendingProc()
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

/* Prepare a post request without performing it. */
int
PendingProc::PreparePost(const std::string &url, const json11::Json &jsreq)
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

	curl_status = CurlStatus::Prepared;
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
PendingProc::PostCompleted()
{
	if (curl_status != CurlStatus::Prepared) {
		return -1;
	}

	curl_status = CurlStatus::Idle;

	return 0;
}

/* Main class. */
class VCoproc {
	int stopfd   = -1; /* owned by the caller, not by us */
	CURLM *curlm = nullptr;
	bool verbose = false;
	bool consume = false;
	bool monitor = false;
	std::vector<std::string> input_dirs;
	std::string output_dir;
	std::string dbfile;
	std::unique_ptr<SQLiteDbConn> dbconn;
	std::string host;
	unsigned short port  = 0;
	size_t input_dir_idx = 0;

	/*
	 * Max number of in progress entries that we allow in the
	 * pending table at any time.
	 */
	static constexpr size_t MaxEntries = 5;

	/* Map of in-progress proc entries. */
	std::unordered_map<std::string, std::unique_ptr<PendingProc>> pending;

	int StoppableSleep(int milliseconds);
	int FetchMoreFiles();
	int FetchFilesFromDir(const std::string &dir,
			      std::deque<std::string> &frontier, int &credits);
	int CleanupComplete();

    public:
	static std::unique_ptr<VCoproc> Create(
	    int stopfd, bool verbose, bool consume, bool monitor,
	    std::vector<std::string> input_dirs, std::string output_dir,
	    std::string dbfile, std::string host, unsigned short port);

	VCoproc(int stopfd, CURLM *curlm, bool verbose, bool consume,
		bool monitor, std::vector<std::string> input_dirs,
		std::string output_dir, std::string dbfile,
		std::unique_ptr<SQLiteDbConn> dbconn, std::string host,
		unsigned short port);
	~VCoproc();
	int MainLoop();
};

std::unique_ptr<VCoproc>
VCoproc::Create(int stopfd, bool verbose, bool consume, bool monitor,
		std::vector<std::string> input_dirs, std::string output_dir,
		std::string dbfile, std::string host, unsigned short port)
{
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

	CURLM *curlm = curl_multi_init();
	if (curlm == nullptr) {
		std::cerr << logb(LogErr)
			  << "Failed to create CURL multi handle" << std::endl;
		return nullptr;
	}

	/* Open a (long-lived) database connection. */
	auto dbconn = SQLiteDbConn::Create(dbfile);
	if (dbconn == nullptr) {
		std::cerr << logb(LogErr) << "Failed to connect to database "
			  << dbfile << std::endl;
		return nullptr;
	}

	/* Create the proc table if it does not exist already. */
	std::stringstream qss;
	qss << "CREATE TABLE IF NOT EXISTS proc ("
	    << "src_path VARCHAR(255) PRIMARY KEY NOT NULL, "
	    << "status TINYINT NOT NULL, "
	    << "mjson TEXT)";
	if (dbconn->ModifyStmt(qss, verbose)) {
		return nullptr;
	}

	return std::make_unique<VCoproc>(
	    stopfd, curlm, verbose, consume, monitor, std::move(input_dirs),
	    std::move(output_dir), std::move(dbfile), std::move(dbconn),
	    std::move(host), port);
}

VCoproc::VCoproc(int stopfd, CURLM *curlm, bool verbose, bool consume,
		 bool monitor, std::vector<std::string> input_dirs,
		 std::string output_dir, std::string dbfile,
		 std::unique_ptr<SQLiteDbConn> dbconn, std::string host,
		 unsigned short port)
    : stopfd(stopfd),
      curlm(curlm),
      verbose(verbose),
      consume(consume),
      monitor(monitor),
      input_dirs(std::move(input_dirs)),
      output_dir(std::move(output_dir)),
      dbfile(std::move(dbfile)),
      dbconn(std::move(dbconn)),
      host(std::move(host)),
      port(port)
{
}

VCoproc::~VCoproc()
{
	/*
	 * PendingProc objects must be destroyed before calling
	 * curl_multi_cleanup(). Force destruction with clear().
	 */
	pending.clear();

	if (curlm != nullptr) {
		curl_multi_cleanup(curlm);
	}
}

/*
 * Sleep for a given number of milliseconds. On error, -1 is returned.
 * If the loop was stopped during the sleep, it returns 1.
 * If the sleep was not interrupted, it returns 0.
 */
int
VCoproc::StoppableSleep(int milliseconds)
{
	struct pollfd pfd[1];

	pfd[0].fd     = stopfd;
	pfd[0].events = POLLIN;

	for (;;) {
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
			assert(pfd[0].revents & POLLIN);
			EventFdDrain(stopfd);

			return 1;
		}
		break;
	}

	return 0;
}

int
VCoproc::FetchMoreFiles()
{
	int credits = MaxEntries;

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
		std::stringstream qss;

		if (!pending.count(path)) {
			pending[path] =
			    std::move(PendingProc::Create(curlm, path));
			if (verbose) {
				std::cout << "New file " << path << std::endl;
			}
		}
		credits--;
	}

	closedir(dir);

	return 0;
}

int
VCoproc::CleanupComplete()
{
	std::stringstream qss;

	for (auto it = pending.begin(); it != pending.end();) {
		auto &proc = it->second;

		if (proc->Status() == ProcStatus::Complete) {
			std::cout << logb(LogDbg) << "Completed "
				  << proc->FilePath() << std::endl;
			++it;
			// it = pending.erase(it);
		} else {
			++it;
		}
	}

	return 0;
}

int
VCoproc::MainLoop()
{
	std::string base_url;
	int err = 0;

	{
		// TODO move in a backend class
		std::stringstream u;
		u << "http://" << host << ":" << port;
		base_url = u.str();
	}

	int num_running_curls = 0, numfds = 0;
	for (;;) {
		/*
		 * First, remove any completed or failed entries
		 * from the pending, to make space for new
		 * files.
		 */
		err = CleanupComplete();
		if (err) {
			break;
		}

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
		for (auto &kv : pending) {
			auto &proc = kv.second;

			if (proc->Status() != ProcStatus::New) {
				continue;
			}

			json11::Json jsreq = json11::Json::object{
			    {"file_name", proc->FilePath()},
			};
			if (proc->PreparePost(base_url + "/process", jsreq)) {
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

		/* Wait for any activity on a POST transfer. */
		cm = curl_multi_wait(curlm, NULL, 0, 1000, &numfds);
		if (cm != CURLM_OK) {
			std::cerr << "Failed to wait multi handle: "
				  << curl_multi_strerror(cm) << std::endl;
			break;
		}

		/* Process any completed transfers. */
		int msgs_left = -1;
		CURLMsg *msg;

		while ((msg = curl_multi_info_read(curlm, &msgs_left)) !=
		       nullptr) {
			PendingProc *p;
			long http_code;
			CURLcode cc;

			if (msg->msg != CURLMSG_DONE) {
				std::cout << logb(LogInf) << "Got CURLM msg "
					  << msg->msg << std::endl;
			}

			cc = curl_easy_getinfo(msg->easy_handle,
					       CURLINFO_PRIVATE, (char **)&p);
			if (cc != CURLE_OK) {
				std::cerr << "Failed to get CURLINFO_PRIVATE: "
					  << curl_easy_strerror(cc)
					  << std::endl;
				continue;
			}

			int ret = p->PostCompleted();
			assert(ret == 0);

			cc = curl_easy_getinfo(msg->easy_handle,
					       CURLINFO_RESPONSE_CODE,
					       &http_code);
			if (cc != CURLE_OK) {
				std::cerr
				    << "Failed to get CURLINFO_RESPONSE_CODE: "
				    << curl_easy_strerror(cc) << std::endl;
				http_code = 400;
			}
			std::cout << "Completed " << p->FilePath() << " --> "
				  << http_code << std::endl;
			if (http_code == 200) {
				p->SetStatus(ProcStatus::Success);
			} else {
				p->SetStatus(ProcStatus::Failure);
			}
		}

#if 0
		if (ProcStatusCount(ProcStatus::New) == 0) {
			/*
			 * No files to process. In monitor mode, sleep for
			 * a little bit. Otherwise just stop.
			 */
			if (!monitor) {
				break;
			}

			if (StoppableSleep(3000) > 0) {
				std::cout << logb(LogInf)
					  << "Stopping the main loop"
					  << std::endl
					  << std::flush;
				break;
			}
			continue;
		}

		std::cout << "I could process "
			  << ProcStatusCount(ProcStatus::New) << " files"
			  << std::endl;
#endif
		if (num_running_curls == 0) {
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
	std::string dbfile;
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

	while ((opt = getopt(argc, argv, "hVvi:o:cmD:H:p:")) != -1) {
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
	    stopfd_global, verbose, consume, monitor, std::move(input_dirs),
	    std::move(output_dir), dbfile, host, port);
	if (vcoproc == nullptr) {
		return -1;
	}

	return vcoproc->MainLoop();
}
