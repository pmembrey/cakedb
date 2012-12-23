#ifndef STORE_H_
# define STORE_H_

#include <netinet/in.h>

/**
 * Send a store (append) request to cakedb server
 */
void store(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename);

/**
 * Main function for store standalone
 */
void store_main(const char *exename, int argc, const char * const *argv);

#endif	/* STORE_H_ */
