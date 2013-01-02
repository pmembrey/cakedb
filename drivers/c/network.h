#ifndef NETWORK_H_
# define NETWORK_H_

#include <sys/types.h>
#include <netinet/in.h>

/**
 * This method tries to write all size_t bytes of data to fd.
 * This method blocks until it writes all or an error occurs.
 */
void write_util(int fd, const void *data, size_t size);

/**
 * This method tries to read size_t of bytes from fd to the data buffer.
 * This method blocks until it reads all bytes or an error occurs.
 */
void read_util(int fd, void *data, size_t size);

/**
 * Connect to saddr distant server
 */
int create_network_link(struct sockaddr_in saddr);

/**
 * Read data from file and send data to socket
 */
void send_data(int socket, int fd, size_t size);

/**
 * Receive data from socket and store to file (descriptor)
 */
void recv_data(int socket, int fd, size_t size);

/**
 * parse ip_port who is of format <ipv4:port>.
 * Returns the corresponding sockaddr_in
 */
struct sockaddr_in parseIp(const char *ip_port);

#endif	/* NETWORK_H_ */
