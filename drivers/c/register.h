#ifndef REGISTER_H_
# define REGISTER_H_

#include <netinet/in.h>

int16_t register_stream(struct sockaddr_in saddr, char *stream_name);

void register_main(char *exename, int argc, char **argv);

#endif	/* REGISTER_H_ */
