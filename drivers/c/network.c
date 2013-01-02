#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "cakedb-driver.h"
#include "network.h"

/**
 * fct pointer of read or write used by the private function io_util
 */
typedef ssize_t (*io_fct)(int fd, void *data, size_t size);

/**
 * Handles io (io_fct) operation
 * Prevent duplication of code between read_util and write_util
 */
static void io_util(int fd, void *data, size_t size, io_fct iofct,
		    const char *errmsg)
{
  size_t i;
  size_t j;
  ssize_t sent;

  j = 0;
  i = 0;
  while (i < size)
  {
    sent = iofct(fd, ((char *) data) + i, size - i);
    if (sent == -1)
    {
      warning(strerror(errno));
      error(errmsg);
    }
    if (sent == 0)
    {
      /* Try 10 times */
      if (j < 10)
      {
	usleep(1);
	++j;
	continue;
      }
      warning(errmsg);
      error("I/O operation was interrupted");
    }
    i += sent;
    j = 0;
  }  
}

/**
 * This method tries to write all size_t bytes of data to fd.
 * This method blocks until it writes all or an error occurs.
 */
void write_util(int fd, const void *data, size_t size)
{
  /* Convert data to void * to comply with generic io_util */
  /* Convert write to generic io_fct to ignore const of second argument */
  io_util(fd, (void *) data, size, (io_fct) &write, "Failed to send data");
}

/**
 * This method tries to read size_t of bytes from fd to the data buffer.
 * This method blocks until it reads all bytes or an error occurs.
 */
void read_util(int fd, void *data, size_t size)
{
  io_util(fd, data, size, &read, "Failed to read data");
}

/**
 * Connect to saddr distant server
 */
int create_network_link(struct sockaddr_in saddr)
{
  int s;
#ifdef SO_NOSIGPIPE
  int set;
#endif	/* SO_NOSIGPIPE */

  printf("Connecting to server %s on port %d\n",
	 inet_ntoa(saddr.sin_addr), ntohs(saddr.sin_port));

  s = socket(AF_INET, SOCK_STREAM, 0);
  if (s == -1)
    error("Could not open socket");

#ifdef SO_NOSIGPIPE
  set = 1;
  if (setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &set, sizeof(set)))
    error("Could not set SIGPIPE handling");
#endif	/* SO_NOSIGPIPE */

  if (connect(s, (struct sockaddr *) &saddr, sizeof(saddr)))
    error("Could not connect");

  return (s);
}

/**
 * Read then write blocks until size is reached
 */
static void read_write_data(int readfd, int writefd, size_t size)
{
  char data[512];
  size_t i;
  int len;

  i = 0;
  while (i < size)
  {
    len = size - i;
    len = ((len < 512) ? len : 512);
    read_util(readfd, &data, len);
    write_util(writefd, &data, len);
    i += len;
  }
}

/**
 * Read data from file and send data to socket
 */
void send_data(int socket, int fd, size_t size)
{
  printf("sending data of %u bytes long\n", size);

  read_write_data(fd, socket, size);
}

/**
 * Receive data from socket and store to file (descriptor)
 */
void recv_data(int socket, int fd, size_t size)
{
  printf("receiving data of %u bytes long\n", size);

  read_write_data(socket, fd, size);
}

/**
 * parse ip_port who is of format <ipv4:port>.
 * Returns the corresponding sockaddr_in
 */
struct sockaddr_in parseIp(const char *ip_port)
{
  struct sockaddr_in saddr;
  char ip[INET_ADDRSTRLEN];
  short int port;
  int i;

  memset(ip, 0, sizeof(ip)/sizeof(*ip));

  for (i = 0; *ip_port != 0; ++i)
  {
    if (*ip_port == ':')
    {
      ++ip_port;
      break;
    }
    ip[i] = *ip_port;
    ++ip_port;
  }
  port = atoi(ip_port);

  saddr.sin_family = AF_INET;
  saddr.sin_port = htons(port);
  saddr.sin_addr.s_addr = inet_addr(ip);

  printf("ip %s\n", ip);
  printf("port %d\n", (int) port);

  return (saddr);
}
