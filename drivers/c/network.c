#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "cakedb-driver.h"
#include "network.h"

void write_util(int fd, void *data, size_t size)
{
  int i;
  int j;
  int sent;

  j = 0;
  i = 0;
  while (i < size)
  {
    sent = write(fd, ((char *) data) + i, size - i);
    if (sent == -1)
      error("Failed to send data");
    if (sent == 0)
    {
      if (j == 10)
      {
	usleep(1);
	++j;
	continue;
      }
      error("Sending of data was interrupted");
    }
    i += sent;
    j = 0;
  }
}

void read_util(int fd, void *data, size_t size)
{
  int i;
  int j;
  int len;

  j = 0;
  i = 0;
  while (i < size)
  {
    len = read(fd, ((char *) data) + i, size - i);
    if (len == -1)
      error("Failed to read data");
    if (len == 0)
    {
      if (j == 10)
      {
	usleep(1);
	++j;
	continue;
      }
      error("Socket closed while reading data");
    }
    i += len;
    j = 0;
  }
}

int create_network_link(struct sockaddr_in saddr)
{
  int s;

  printf("Connecting to server %s on port %d\n",
	 inet_ntoa(saddr.sin_addr), ntohs(saddr.sin_port));

  s = socket(AF_INET, SOCK_STREAM, 0);
  if (s == -1)
    error("Could not open socket");

  if (connect(s, (struct sockaddr *) &saddr, sizeof(saddr)))
    error("Could not connect");

  return (s);
}

/**
 * Read data from file and send data to socket
 */
void send_data(int socket, int fd, size_t size)
{
  char data[512];
  size_t i;
  int len;

  printf("sending data of %u bytes long\n", size);

  i = 0;
  while (i < size)
  {
    len = size - i;
    len = ((len < 512) ? len : 512);
    read_util(fd, &data, len);
    write_util(socket, &data, len);
    i += len;
  }
}

/**
 * Receive data from socket and store to file (descriptor)
 */
void recv_data(int socket, int fd, size_t size)
{
  char data[512];
  size_t i;
  int len;

  printf("receiving data of %u bytes long\n", size);
  i = 0;
  while (i < size)
  {
    len = size - i;
    len = ((len < 512) ? len : 512);
    read_util(socket, &data, len);
    write_util(fd, &data, len);
    i += len;
  }
}

struct sockaddr_in parseIp(char *ip_port)
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
