#ifndef RANGE_H_
# define RANGE_H_

#include <netinet/in.h>

/**
 * Send a range (simple query) request to cakedb server
 */
void range(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename, int64_t from, int64_t to);

/**
 * Main function for range standalone
 */
void range_main(const char *exename, int argc, const char * const *argv);

#endif	/* RANGE_H_ */
