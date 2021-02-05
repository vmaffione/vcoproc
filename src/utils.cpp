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
PathJoin(const std::string &dirpath, const std::string &name)
{
	if (dirpath.empty()) {
		return name;
	}

	std::string res = dirpath;

	if (dirpath[dirpath.size() - 1] != '/') {
		res += '/';
	}

	return res + name;
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

	dstpath = FileBaseName(src);
	dstpath = PathJoin(dir, dstpath);

	if (rename(src.c_str(), dstpath.c_str())) {
		std::cerr << "Failed to rename " << src << " --> " << dstpath
			  << std::endl;
		return -1;
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

	dstpath = FileBaseName(srcname);
	dstpath = PathJoin(dir, dstpath);

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

} // namespace utils
