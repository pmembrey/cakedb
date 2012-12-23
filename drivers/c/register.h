#ifndef REGISTER_H_
# define REGISTER_H_

#include <netinet/in.h>

/**
 * Send a register stream request to cakedb server
 */
int16_t register_stream(struct sockaddr_in saddr, const char *stream_name);

/**
 * Main function for register standalone
 */
void register_main(const char *exename, int argc, const char * const *argv);

#endif	/* REGISTER_H_ */
