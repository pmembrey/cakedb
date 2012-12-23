#ifndef RANGE_H_
# define RANGE_H_

#include <netinet/in.h>

void range(struct sockaddr_in saddr, char *stream_name, char *filename, int64_t from, int64_t to);

void range_main(char *exename, int argc, char **argv);

#endif	/* RANGE_H_ */
