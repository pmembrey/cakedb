#ifndef QUERY_H_
# define QUERY_H_

#include <netinet/in.h>

void query(struct sockaddr_in saddr, char *stream_name, char *filename, int64_t timestamp);

void query_main(char *exename, int argc, char **argv);

#endif
