#ifndef NETWORK_H_
# define NETWORK_H_

#include <sys/types.h>
#include <netinet/in.h>

void write_util(int fd, void *data, size_t size);

void read_util(int fd, void *data, size_t size);

int create_network_link(struct sockaddr_in saddr);

/**
 * Read data from file and send data to socket
 */
void send_data(int socket, int fd, size_t size);

/**
 * Receive data from socket and store to file (descriptor)
 */
void recv_data(int socket, int fd, size_t size);

struct sockaddr_in parseIp(char *ip_port);

#endif	/* NETWORK_H_ */
