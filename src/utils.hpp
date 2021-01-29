/*
 * Author: Vincenzo M. (2020)
 */

#ifndef __UTILS_HPP__
#define __UTILS_HPP__

#include <arpa/inet.h>
#include <string>
#include <sstream>
#include <unistd.h>
#include <vector>

namespace utils {

enum {
	LogErr = 0,
	LogWrn,
	LogInf,
	LogDbg,
};

bool DirExists(const char *path);
bool DirExists(const std::string &s);
bool DirEmpty(const std::string &dirpath);
bool IsDir(const char *path);
bool IsDir(const std::string &path);
std::string CurrentDirectory();
bool IsFile(const char *path);
bool IsFile(const std::string &path);
long long int FileSize(const char *path);
long long int FileSize(const std::string &filename);
std::string FileBaseName(const std::string &path);
std::string FileParentDir(const std::string &path);
bool FileHasExtension(const std::string &filename, const std::string &ext);
std::string PathJoin(const std::string &dirpath, const std::string &name);
int MkdirIfNotExists(const std::string &path);
std::string JoinStrings(const std::vector<std::string> &strings,
			const std::string &delim);
int CopyFile(const std::string &dstname, const std::string &srcname);
int MoveToDir(const std::string &dir, const std::string &src);
int RemoveFile(const std::string &path);

int ExecuteCommand(std::stringstream &cmdss, bool verbose = false,
		   bool daemonize = false);
int ExecuteCommand(const std::string &cmdstring, bool verbose = false,
		   bool daemonize = false);

int FdSetBlocking(int fd, bool blocking);
int EventFdSignal(int efd);
int EventFdDrain(int efd);

std::string logb(int loglevel);

/* Helper class to manage file descriptors with RAII. */
class UniqueFd {
	int fd = -1;

    public:
	UniqueFd() : UniqueFd(-1) {}
	UniqueFd(int fd) : fd(fd) {}
	UniqueFd(const UniqueFd &) = delete;
	UniqueFd &operator=(const UniqueFd &) = delete;

	UniqueFd(UniqueFd &&o)
	{
		reset();
		fd   = o.fd;
		o.fd = -1;
	}

	UniqueFd &operator=(UniqueFd &&o)
	{
		if (&o != this) {
			reset();
			fd   = o.fd;
			o.fd = -1;
		}

		return *this;
	}

	~UniqueFd() { reset(); }

	int get() const { return fd; }
	operator int() const { return fd; }
	bool empty() const { return fd < 0; }

	void reset()
	{
		if (fd >= 0) {
			close(fd);
			fd = -1;
		}
	}
};

template <class T, bool HEX = false>
bool
Str2Num(const std::string &s, T &num)
{
	std::istringstream iss(s);

	if (HEX) {
		iss >> std::hex >> num;
	} else {
		iss >> num;
	}

	return !iss.fail();
}

template <class T, bool HEX = false>
std::string
Num2Str(T num)
{
	std::ostringstream oss;

	if (HEX) {
		oss << std::hex << num;
	} else {
		oss << num;
	}

	return oss.str();
}

struct IPAddr {
	std::string repr;
	uint32_t addr;
	unsigned netbits;

	IPAddr() : addr(0), netbits(0) {}
	IPAddr(const std::string &p);
	IPAddr(uint32_t naddr, unsigned nbits);

	bool IsEmpty() const { return netbits == 0 && addr == 0; }

	bool operator<(const IPAddr &o) const
	{
		return addr < o.addr || (addr == o.addr && netbits < o.netbits);
	}

	/* Pick an host address in belonging to the same subnet. */
	IPAddr HostAddr(uint32_t host, uint32_t new_netbits = -1) const
	{
		uint32_t hostmask = ~((1 << (32 - netbits)) - 1);

		if (host & hostmask) {
			throw "Host number out of range";
		}

		if (new_netbits == static_cast<uint32_t>(-1)) {
			new_netbits = netbits;
		}

		return IPAddr((addr & hostmask) + host, new_netbits);
	}

	std::string StrNoPrefix() const;
	operator std::string() const { return repr; }
};

} // namespace utils

#endif /* __UTILS_HPP__ */
