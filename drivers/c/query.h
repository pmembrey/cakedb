#ifndef QUERY_H_
# define QUERY_H_

#include <netinet/in.h>

/**
 * Send a query (all since) request to cakedb server
 */
void query(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename, int64_t timestamp);

/**
 * Main function for query standalone
 */
void query_main(const char *exename, int argc, const char * const *argv);

#endif
