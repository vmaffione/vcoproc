#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "version.h"

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

} // namespace

class VCoproc {
    public:
	static std::unique_ptr<VCoproc> CreateVCoproc(
	    std::vector<std::string> input_dirs);

	int Start();
};

std::unique_ptr<VCoproc>
VCoproc::CreateVCoproc(std::vector<std::string> input_dirs)
{
	return nullptr;
}

int
VCoproc::Start()
{
	return 0;
}

int
main(int argc, char **argv)
{
	std::vector<std::string> input_dirs;
	int verbose = 0;
	int opt;

	auto vcoproc = VCoproc::CreateVCoproc(std::move(input_dirs));

	while ((opt = getopt(argc, argv, "hVvi:o:")) != -1) {
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
		}
	}

	return vcoproc->Start();
}
