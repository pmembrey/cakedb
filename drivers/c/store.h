#ifndef STORE_H_
# define STORE_H_

#include <netinet/in.h>

void store(struct sockaddr_in saddr, char *stream_name, char *filename);

void store_main(char *exename, int argc, char **argv);

#endif	/* STORE_H_ */
