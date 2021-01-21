#include <algorithm>
#include <cassert>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <iostream>
#include <memory>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include <utils.hpp>
#include "version.h"

using namespace utils;

namespace {

void
Usage(const char *progname)
{
	std::cout << progname << " usage:" << std::endl
		  << "    -h (show this help and exit)" << std::endl
		  << "    -V (print version info and exit)" << std::endl
		  << "    -v (increase verbosity level)" << std::endl
		  << "    -i INPUT_DIR (add directory to look for files)"
		  << std::endl
		  << "    -o OUTPUT_DIR[,PAR1=VAL1,...] (directory to store "
		     "incoming files)"
		  << "    -c (consume input files)" << std::endl
		  << "    -m (monitor input directories rather than stop when "
		     "running out of files)"
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

class VCoproc {
	int stopfd   = -1; /* owned by the caller, not by us */
	bool verbose = false;
	bool consume = false;
	bool monitor = false;
	std::vector<std::string> input_dirs;
	std::string output_dir;
	size_t input_dir_idx = 0;
	bool sorted	     = false;

	struct InputFileInfo {
		std::string filepath;
		std::string filesubpath;
		unsigned int dir_idx;
	};

	int StoppableSleep(int milliseconds);
	int NextInputFile(InputFileInfo &finfo);
	int FirstInputFileFromDir(const std::string &dir,
				  std::deque<std::string> &frontier,
				  InputFileInfo &finfo);

    public:
	static std::unique_ptr<VCoproc> CreateVCoproc(
	    int stopfd, bool verbose, bool consume, bool monitor,
	    std::vector<std::string> input_dirs, std::string output_dir);

	VCoproc(int stopfd, bool verbose, bool consume, bool monitor,
		std::vector<std::string> input_dirs, std::string output_dir);
	int MainLoop();
};

std::unique_ptr<VCoproc>
VCoproc::CreateVCoproc(int stopfd, bool verbose, bool consume, bool monitor,
		       std::vector<std::string> input_dirs,
		       std::string output_dir)
{
	if (input_dirs.empty()) {
		std::cout << logb(LogErr) << "No input directories specified"
			  << std::endl;
		return nullptr;
	}

	if (output_dir.empty()) {
		std::cout << logb(LogErr) << "No output directory specified"
			  << std::endl;
		return nullptr;
	}

	return std::make_unique<VCoproc>(stopfd, verbose, consume, monitor,
					 std::move(input_dirs),
					 std::move(output_dir));
}

VCoproc::VCoproc(int stopfd, bool verbose, bool consume, bool monitor,
		 std::vector<std::string> input_dirs, std::string output_dir)
    : stopfd(stopfd),
      verbose(verbose),
      consume(consume),
      monitor(monitor),
      input_dirs(std::move(input_dirs)),
      output_dir(std::move(output_dir))
{
}

int
VCoproc::MainLoop()
{
	int err = 0;

	for (;;) {
		/* Fetch the next file from one of the input directories. */
		InputFileInfo finfo;
		int ret;

		ret = NextInputFile(finfo);
		if (ret < 0) {
			err = ret;
			break;
		}

		if (ret == 0 || finfo.filepath.empty()) {
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

		// process
		std::cout << "I should process " << finfo.filepath << std::endl;
		break;
	}

	return err;
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
VCoproc::NextInputFile(InputFileInfo &finfo)
{
	assert(input_dir_idx < input_dirs.size());

	finfo.filepath.clear();
	finfo.filesubpath.clear();

	/*
	 * Scan all the input directories, starting from the one that was
	 * scanned less recently.
	 */
	for (size_t n = 0; n < input_dirs.size() && finfo.filepath.empty();
	     n++) {
		/*
		 * Visit this input directory and all of its input
		 * subdirectories (recursively), stopping as soon as the
		 * first file is found.
		 * The visit is implemented as a BFS (Breadth First Search).
		 */
		std::deque<std::string> frontier = {input_dirs[input_dir_idx]};

		for (int c = 0;
		     !frontier.empty() && finfo.filepath.empty() && c < 32;
		     c++) {
			std::string &dir = frontier.front();
			int ret;

			ret = FirstInputFileFromDir(dir, frontier, finfo);
			if (ret) {
				return ret;
			}

			frontier.pop_front();
		}
		if (!finfo.filepath.empty()) {
			/*
			 * We found a file to transmit, and we need to compute
			 * the subpath relative to the input directory (-i).
			 */
			assert(finfo.filepath.size() >
			       input_dirs[input_dir_idx].size());
			finfo.filesubpath = finfo.filepath.substr(
			    input_dirs[input_dir_idx].size());
			finfo.filesubpath = finfo.filesubpath.substr(
			    finfo.filesubpath.find_first_not_of('/'));
			finfo.dir_idx = input_dir_idx;
		}
		if (++input_dir_idx >= input_dirs.size()) {
			input_dir_idx = 0;
		}
	}

	return finfo.filepath.empty() ? 0 : 1;
}

int
VCoproc::FirstInputFileFromDir(const std::string &dirname,
			       std::deque<std::string> &frontier,
			       InputFileInfo &finfo)
{
	std::vector<std::string> avail_files;
	struct dirent *dent;
	DIR *dir;

	dir = opendir(dirname.c_str());
	if (dir == nullptr) {
		std::cerr << logb(LogErr) << "Failed to opendir(" << dirname
			  << "): " << strerror(errno) << std::endl;
		return -1;
	}

	while ((dent = readdir(dir)) != nullptr) {
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

		if (sorted) {
			avail_files.push_back(path);
		} else {
			/*
			 * We finally got a file good for transfer. Fill in
			 * the required information and return to the caller.
			 */
			finfo.filepath = std::move(path);
			break;
		}
	}

	closedir(dir);

	if (sorted && !avail_files.empty()) {
		std::sort(avail_files.begin(), avail_files.end());
		finfo.filepath = std::move(avail_files.front());
	}

	return 0;
}

int
main(int argc, char **argv)
{
	std::vector<std::string> input_dirs;
	std::string output_dir;
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

	while ((opt = getopt(argc, argv, "hVvi:o:cm")) != -1) {
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
		}
	}

	auto vcoproc = VCoproc::CreateVCoproc(stopfd_global, verbose, consume,
					      monitor, std::move(input_dirs),
					      std::move(output_dir));
	if (vcoproc == nullptr) {
		return -1;
	}

	return vcoproc->MainLoop();
}
