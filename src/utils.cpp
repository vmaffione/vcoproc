/*
 * utils
 *
 * Author: Vincenzo M. (2020)
 */

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include <utils.hpp>

namespace utils {

bool
DirExists(const char *path)
{
	struct stat st;

	if (stat(path, &st)) {
		return false;
	}

	return S_ISDIR(st.st_mode);
}

bool
DirExists(const std::string &s)
{
	return DirExists(s.c_str());
}

bool
DirEmpty(const std::string &dirpath)
{
	struct dirent *dent;
	bool result = true;
	DIR *dir;

	dir = opendir(dirpath.c_str());
	if (dir == nullptr) {
		std::cerr << logb(LogErr) << "Failed to opendir(" << dirpath
			  << "): " << strerror(errno) << std::endl;
		return false;
	}

	while ((dent = readdir(dir)) != nullptr) {
		if (strcmp(dent->d_name, ".") && strcmp(dent->d_name, "..")) {
			result = false;
			break;
		}
	}

	closedir(dir);

	return result;
}

bool
IsDir(const char *path)
{
	return DirExists(path);
}

bool
IsDir(const std::string &s)
{
	return DirExists(s);
}

std::string
CurrentDirectory()
{
	char buf[PATH_MAX];

	if (getcwd(buf, sizeof(buf)) == nullptr) {
		return std::string();
	}

	return std::string(buf);
}

bool
IsFile(const char *path)
{
	struct stat st;

	if (stat(path, &st)) {
		return false;
	}

	return S_ISREG(st.st_mode);
}

bool
IsFile(const std::string &path)
{
	return IsFile(path.c_str());
}

long long int
FileSize(const char *path)
{
	struct stat st;
	int ret = stat(path, &st);

	if (ret) {
		std::cerr << logb(LogErr) << "Failed to stat(" << path
			  << "): " << strerror(errno) << std::endl;
		return -1;
	}

	return static_cast<long long int>(st.st_size);
}

long long int
FileAgeSeconds(const char *path)
{
	struct stat st;
	int ret	   = stat(path, &st);
	time_t now = time(nullptr);

	if (ret) {
		std::cerr << logb(LogErr) << "Failed to stat(" << path
			  << "): " << strerror(errno) << std::endl;
		return -1;
	}

	return static_cast<long long int>(now - st.st_mtime);
}

long long int
FileAgeSeconds(const std::string &path)
{
	return FileAgeSeconds(path.c_str());
}

long long int
FileSize(const std::string &filename)
{
	std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);

	if (!in.good()) {
		return 0;
	}

	return in.tellg();
}

std::string
FileBaseName(const std::string &path)
{
	size_t slashpos = path.rfind('/');

	if (slashpos == std::string::npos) {
		return path;
	}

	return path.substr(slashpos + 1);
}

std::string
FileParentDir(const std::string &path)
{
	size_t slashpos = path.rfind('/');

	if (slashpos == std::string::npos) {
		return std::string();
	}

	return path.substr(0, slashpos);
}

std::string
AbsPath(const std::string &path)
{
	char result[PATH_MAX + 1];

	if (realpath(path.c_str(), result) == nullptr) {
		return std::string();
	}
	result[PATH_MAX] = '\0';

	return std::string(result);
}

bool
FileHasExtension(const std::string &filename, const std::string &ext)
{
	return filename.size() > ext.size() &&
	       filename.substr(filename.size() - ext.size()) == ext;
}

bool
FileHasAnyExtension(const std::string &filename,
		    const std::vector<std::string> &exts)
{
	for (const auto &ext : exts) {
		if (FileHasExtension(filename, ext)) {
			return true;
		}
	}

	return false;
}

std::string
PathNameNoExt(const std::string &path)
{
	size_t slashpos = path.rfind('.');

	if (slashpos == std::string::npos) {
		return path;
	}

	return path.substr(0, slashpos);
}

std::string
PathNameNewExt(const std::string &path, std::string new_ext)
{
	return PathNameNoExt(path) + std::string(".") + new_ext;
}

static std::string
CurtimeString()
{
	time_t ctime = time(nullptr);
	struct tm *time_info;
	char tbuf[9];

	time_info = localtime(&ctime);
	strftime(tbuf, sizeof(tbuf), "%H:%M:%S", time_info);

	return std::string(tbuf);
}

std::string &
StrLower(std::string &s)
{
	std::transform(s.begin(), s.end(), s.begin(),
		       [](unsigned char c) { return std::tolower(c); });
	return s;
}

std::string
logb(int loglevel)
{
	std::string ret;

	ret = std::string("[") + CurtimeString() + ":";

	switch (loglevel) {
	case LogErr:
		ret += "ERR] ";
		break;

	case LogWrn:
		ret += "WRN] ";
		break;

	case LogInf:
		ret += "INF] ";
		break;

	case LogDbg:
		ret += "DBG] ";
		break;

	default:
		assert(false);
		break;
	}

	return ret;
}

std::string
PathJoin(const std::string &dirpath, const std::string &name,
	 bool preserve_path)
{
	std::string basename = preserve_path ? name : FileBaseName(name);

	if (dirpath.empty()) {
		return basename;
	}

	std::string res = dirpath;

	if (dirpath[dirpath.size() - 1] != '/') {
		res += '/';
	}

	return res + basename;
}

int
MkdirIfNotExists(const std::string &path)
{
	int ret;

	if (DirExists(path)) {
		return 0;
	}

	ret = mkdir(path.c_str(), 0775);
	if (ret) {
		std::cerr << logb(LogErr) << "Failed to mkdir(" << path
			  << "): " << strerror(errno) << std::endl;
		return ret;
	}

	return 0;
}

std::string
JoinStrings(const std::vector<std::string> &strings, const std::string &delim)
{
	std::stringstream ss;
	bool first = true;

	for (const std::string &s : strings) {
		if (!first) {
			ss << delim;
		} else {
			first = false;
		}
		ss << s;
	}

	return ss.str();
}

int
CopyFile(const std::string &dstname, const std::string &srcname)
{
	UniqueFd src(open(srcname.c_str(), O_RDONLY, 0));
	if (src < 0) {
		std::cerr << "Failed to open " << srcname << ": "
			  << strerror(errno) << std::endl
			  << std::flush;
		return src;
	}

	UniqueFd dst(open(dstname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644));
	if (dst < 0) {
		std::cerr << "Failed to open " << dstname << ": "
			  << strerror(errno) << std::endl
			  << std::flush;
		return dst;
	}

	char buf[128 * 1024];
	ssize_t size;

	while ((size = read(src, buf, sizeof(buf))) > 0) {
		ssize_t ofs = 0;
		ssize_t n;

		if (size < 0) {
			std::cerr << "Failed to read(" << srcname
				  << "): " << strerror(errno) << std::endl
				  << std::flush;
			return size;
		}

		while (ofs < size &&
		       (n = write(dst, buf + ofs, size - ofs)) >= 0) {
			ofs += n;
		}

		if (n < 0) {
			std::cerr << "Failed to write(" << dstname
				  << "): " << strerror(errno) << std::endl
				  << std::flush;
			return n;
		}
	}

	return 0;
}

int
MoveToDir(const std::string &dir, const std::string &src)
{
	std::string dstpath;

	if (!IsDir(dir)) {
		std::cerr << "Cannot move " << src << ": " << dir
			  << " is not a directory" << std::endl;
		return -1;
	}

	dstpath = PathJoin(dir, src);

	if (rename(src.c_str(), dstpath.c_str())) {
		if (errno != EXDEV) {
			std::cerr << "Failed to rename " << src << " --> "
				  << dstpath << ": " << strerror(errno)
				  << std::endl;
			return -1;
		}
		/*
		 * We cannot rename a file across different file systems.
		 * Fallback on copy to the destination file system and
		 * remove the original one.
		 */
		if (CopyFile(dstpath, src)) {
			std::cerr << "Failed to copy " << src << " --> "
				  << dstpath << ": " << strerror(errno)
				  << std::endl;
			return -1;
		}
		if (RemoveFile(src)) {
			return -1;
		}
	}

	return 0;
}

int
CopyToDir(const std::string &dir, const std::string &srcname)
{
	std::string dstpath;

	if (!IsDir(dir)) {
		std::cerr << "Cannot move " << srcname << ": " << dir
			  << " is not a directory" << std::endl;
		return -1;
	}

	if (!IsFile(srcname)) {
		std::cerr << "Cannot move " << srcname << ": not a file"
			  << std::endl;
		return -1;
	}

	dstpath = PathJoin(dir, srcname);

	return CopyFile(dstpath, srcname);
}

int
RemoveFile(const std::string &path, bool may_not_exist)
{
	if (!IsFile(path)) {
		if (may_not_exist) {
			return 0;
		}
		std::cerr << "Cannot remove " << path << ": not a file"
			  << std::endl;
		return -1;
	}

	if (unlink(path.c_str())) {
		if (may_not_exist && errno == ENOENT) {
			return 0;
		}
		std::cerr << "Failed to remove " << path << ": "
			  << strerror(errno) << std::endl;
		return -1;
	}

	return 0;
}

std::unique_ptr<DirScanner>
DirScanner::Create(const std::string &path, bool safe)
{
	DIR *dir = opendir(path.c_str());

	if (dir == nullptr) {
		std::cerr << logb(LogErr) << "Failed to opendir(" << path
			  << "): " << strerror(errno) << std::endl;
		return nullptr;
	}

	return std::make_unique<DirScanner>(dir, safe);
}

DirScanner::~DirScanner()
{
	if (dir) {
		closedir(dir);
		dir = nullptr;
	}
}

bool
DirScanner::DoNext(std::string &entry)
{
	struct dirent *dent;

	while ((dent = readdir(dir)) != nullptr) {
		if (strcmp(dent->d_name, ".") && strcmp(dent->d_name, "..")) {
			entry = std::string(dent->d_name);
			return true;
		}
	}

	return false;
}

bool
DirScanner::Next(std::string &entry)
{
	if (!safe) {
		return DoNext(entry);
	}

	if (next_file_idx < 0) {
		/* Pre-load. */
		std::string file;

		while (DoNext(file)) {
			files.push_back(file);
		}
		next_file_idx = 0;
	}
	if (next_file_idx < static_cast<int>(files.size())) {
		entry = files[next_file_idx++];
		return true;
	}
	return false;
}

int
ExecuteCommand(std::stringstream &cmdss, bool verbose, bool daemonize)
{
	std::vector<std::string> tokens;
	std::string token;
	pid_t child_pid;
	int child_status;
	int ret = -1;
	char **argv;

	if (verbose) {
		std::cout << logb(LogDbg) << "Exec command '" << cmdss.str()
			  << "'" << std::endl;
	}

	/* Separate the arguments into a vector. */
	while (cmdss >> token) {
		tokens.push_back(token);
	}

	/* Allocate a working copy for the arguments. */
	argv = new char *[tokens.size() + 1];
	for (unsigned i = 0; i <= tokens.size(); i++) {
		argv[i] = nullptr;
	}
	for (unsigned i = 0; i < tokens.size(); i++) {
		argv[i] = strdup(tokens[i].c_str());
		if (!argv[i]) {
			std::cerr
			    << "ExecuteCommand: out of memory while allocating "
			       "arguments"
			    << std::endl;
			goto out;
		}
	}

	child_pid = fork();
	if (child_pid == 0) {
		/* Child process: Redirect standard input, output and
		 * error to /dev/null and execute the target command. */

		close(0);
		close(1);
		close(2);
		if (open("/dev/null", O_RDONLY) < 0 ||
		    open("/dev/null", O_WRONLY) < 0 ||
		    open("/dev/null", O_WRONLY) < 0) {
			/* Redirection failed. */
			return -1;
		}
		execvp(argv[0], argv);
		perror("execvp()");
		exit(127); /* command not found */
	}

	if (daemonize) {
		ret = static_cast<int>(child_pid);
	} else {
		/* Parent process. Wait for the child to exit. */
		waitpid(child_pid, &child_status, 0);
		if (WIFEXITED(child_status)) {
			ret = WEXITSTATUS(child_status);
			ret *= -1;
		}
	}

out:
	for (unsigned i = 0; i < tokens.size(); i++) {
		if (argv[i]) {
			free(argv[i]);
			argv[i] = nullptr;
		}
	}
	delete[] argv;

	if (verbose && ret == -127) {
		std::cerr << "ExecuteCommand: command not found" << std::endl;
	}

	return ret;
}

int
ExecuteCommand(const std::string &cmdstring, bool verbose, bool daemonize)
{
	std::stringstream cmdss(cmdstring);

	return ExecuteCommand(cmdss, verbose, daemonize);
}

int
FdSetBlocking(int fd, bool blocking)
{
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags == -1) {
		std::cerr << logb(LogErr) << "fcntl(F_GETFL) failed"
			  << std::endl;

		return flags;
	}

	if (blocking) {
		flags &= ~O_NONBLOCK;
	} else {
		flags |= O_NONBLOCK;
	}

	flags = fcntl(fd, F_SETFL, flags);
	if (flags < 0) {
		std::cerr << logb(LogErr) << "fcntl(F_GETFL) failed"
			  << std::endl;

		return flags;
	}

	return 0;
}

int
EventFdSignal(int efd)
{
	uint64_t val = 1;
	int n;

	n = write(efd, &val, sizeof(val));
	if (n < 0) {
		std::cerr << logb(LogErr)
			  << "Failed to notify stopfd: " << strerror(errno)
			  << std::endl;
		return n;

	} else if (n != sizeof(val)) {
		std::cerr << logb(LogErr) << "Incomplete write to stopfd: " << n
			  << "/" << sizeof(val) << std::endl;
		return -1;
	}

	return 0;
}

int
EventFdDrain(int efd)
{
	uint64_t val;
	int n;

	n = read(efd, &val, sizeof(val));
	if (n < 0) {
		std::cerr << logb(LogErr)
			  << "Failed to drain eventfd(): " << strerror(errno)
			  << std::endl;
		return n;

	} else if (n != sizeof(val)) {
		std::cerr << logb(LogErr)
			  << "Incomplete read from stopfd: " << n << "/"
			  << sizeof(val) << std::endl;
		return -1;
	}

	return 0;
}

IPAddr::IPAddr(const std::string &_p) : repr(_p)
{
	std::string p = _p;
	std::string digit;
	size_t slash;
	int m;

	slash = p.find("/");

	if (slash != std::string::npos) {
		/* Extract the mask m in "a.b.c.d/m" */
		if (!Str2Num<int>(p.substr(slash + 1), m)) {
			throw "Invalid IP prefix";
		}
		if (m < 1 || m > 32) {
			throw "Invalid IP prefix";
		}
		netbits = m;
		p	= p.substr(0, slash);
	} else {
		/* No mask info. Assume /32. */
		netbits = 32;
	}

	/* Extract a, b, c and d. */
	std::replace(p.begin(), p.end(), '.', ' ');

	std::stringstream ss(p);

	addr = 0;
	while (ss >> digit) {
		int d;

		if (!Str2Num<int>(digit, d)) {
			throw "Invalid IP prefix";
		}

		if (d < 0 || d > 255) {
			throw "Invalid IP prefix";
		}
		addr <<= 8;
		addr |= (unsigned)d;
	}

	return;
}

IPAddr::IPAddr(uint32_t naddr, unsigned nbits)
{
	unsigned a, b, c, d;
	std::stringstream ss;

	if (!nbits || nbits > 30) {
		throw "Invalid IP prefix";
	}

	addr	= naddr;
	netbits = nbits;

	d = naddr & 0xff;
	naddr >>= 8;
	c = naddr & 0xff;
	naddr >>= 8;
	b = naddr & 0xff;
	naddr >>= 8;
	a = naddr & 0xff;

	ss << a << "." << b << "." << c << "." << d << "/" << nbits;
	repr = ss.str();
}

std::string
IPAddr::StrNoPrefix() const
{
	size_t slash = repr.find("/");

	if (slash == std::string::npos) {
		return repr;
	}

	return repr.substr(0, slash);
}

std::string
Base64Encode(const char *data, size_t in_len)
{
	static constexpr char kEncodingTable[] = {
	    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
	    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
	    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
	    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

	size_t out_len = 4 * ((in_len + 2) / 3);
	std::string ret(out_len, '\0');
	char *p = const_cast<char *>(ret.c_str());
	size_t i;

	for (i = 0; i < in_len - 2; i += 3) {
		*p++ = kEncodingTable[(data[i] >> 2) & 0x3F];
		*p++ = kEncodingTable[((data[i] & 0x3) << 4) |
				      ((int)(data[i + 1] & 0xF0) >> 4)];
		*p++ = kEncodingTable[((data[i + 1] & 0xF) << 2) |
				      ((int)(data[i + 2] & 0xC0) >> 6)];
		*p++ = kEncodingTable[data[i + 2] & 0x3F];
	}
	if (i < in_len) {
		*p++ = kEncodingTable[(data[i] >> 2) & 0x3F];
		if (i == (in_len - 1)) {
			*p++ = kEncodingTable[((data[i] & 0x3) << 4)];
			*p++ = '=';
		} else {
			*p++ = kEncodingTable[((data[i] & 0x3) << 4) |
					      ((int)(data[i + 1] & 0xF0) >> 4)];
			*p++ = kEncodingTable[((data[i + 1] & 0xF) << 2)];
		}
		*p++ = '=';
	}

	return ret;
}

int
Base64Decode(const char *input, size_t in_len, std::string &out)
{
	static constexpr unsigned char kDecodingTable[] = {
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
	    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
	    64, 0,  1,	2,  3,	4,  5,	6,  7,	8,  9,	10, 11, 12, 13, 14,
	    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
	    64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
	    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64};

	if (in_len % 4 != 0) {
		return -1; /* Input data size is not a multiple of 4 */
	}

	size_t out_len = in_len / 4 * 3;
	if (input[in_len - 1] == '=')
		out_len--;
	if (input[in_len - 2] == '=')
		out_len--;

	out.resize(out_len);

	for (size_t i = 0, j = 0; i < in_len;) {
		uint32_t a = input[i] == '='
				 ? 0 & i++
				 : kDecodingTable[static_cast<int>(input[i++])];
		uint32_t b = input[i] == '='
				 ? 0 & i++
				 : kDecodingTable[static_cast<int>(input[i++])];
		uint32_t c = input[i] == '='
				 ? 0 & i++
				 : kDecodingTable[static_cast<int>(input[i++])];
		uint32_t d = input[i] == '='
				 ? 0 & i++
				 : kDecodingTable[static_cast<int>(input[i++])];

		uint32_t triple =
		    (a << 3 * 6) + (b << 2 * 6) + (c << 1 * 6) + (d << 0 * 6);

		if (j < out_len)
			out[j++] = (triple >> 2 * 8) & 0xFF;
		if (j < out_len)
			out[j++] = (triple >> 1 * 8) & 0xFF;
		if (j < out_len)
			out[j++] = (triple >> 0 * 8) & 0xFF;
	}

	return 0;
}

std::string
Base64Encode(const std::string &src)
{
	return Base64Encode(src.c_str(), src.size());
}

int
Base64Decode(const std::string &enc, std::string &result)
{
	return Base64Decode(enc.data(), enc.size(), result);
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

} // namespace utils
