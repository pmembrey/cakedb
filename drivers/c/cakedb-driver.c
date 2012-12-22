#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

typedef struct s_reg
{
  int32_t length;
  int16_t cmd;
  char name[512 + 1];
} t_reg;

typedef struct s_data
{
  int32_t length;
  int16_t cmd;
  int16_t sid;
} t_data;

typedef struct s_get
{
  int32_t length;
  int16_t cmd;
  int16_t sid;
  int64_t from;
  int64_t to;
} t_get;

void error(const char *str)
{
  fprintf(stderr, "Error: %s\n", str);
  exit(EXIT_FAILURE);
}

void usage(char *exename)
{
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, "%s help\n", exename);
  fprintf(stdout, "%s register <server ip:port> <stream name>\n", exename);
  fprintf(stdout, "%s store <server ip:port> <stream name> <filename>\n", exename);
  fprintf(stdout, "%s query <server ip:port> <stream name> <filename> <timestamp>\n", exename);
  fprintf(stdout, "%s range <server ip:port> <stream name> <filename> <TS from> <TS to>\n", exename);
  exit(EXIT_SUCCESS);
}

void write_util(int fd, void *data, size_t size)
{
  int i;
  int j;
  int sent;

  j = 0;
  i = 0;
  while (i < size)
  {
    sent = write(fd, data + i, size - i);
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
    len = read(fd, data + i, size - i);
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

int create_network_link(char *ip, short int port)
{
  int s;
  struct sockaddr_in saddr;

  printf("Connecting to server %s on port %d\n", ip, (int) port);

  s = socket(AF_INET, SOCK_STREAM, 0);
  if (s == -1)
    error("Could not open socket");

  saddr.sin_family = AF_INET;
  saddr.sin_port = htons(port);
  saddr.sin_addr.s_addr = inet_addr(ip);

  if (connect(s, (struct sockaddr *) &saddr, sizeof(saddr)))
    error("Could not connect");

  return (s);
}

int16_t register_stream(char *ip, short int port, char *stream_name)
{
  t_reg reg;
  int s = sizeof(reg.name);
  int i;
  int16_t sid;

  printf("registering stream %s\n", stream_name);

  s = create_network_link(ip, port);

  reg.cmd = htons(5);
  strncpy(reg.name, stream_name, 512);
  i = strlen(stream_name);
  reg.length = htonl(i);

  i = write(s, &reg, i + 6);
  printf("Wrote %d bytes\n", (int) i);
  if (i <= 0)
    error("Failed sending stream name");

  i = read(s, &sid, sizeof(sid));
  printf("Read %d bytes\n", (int) i);
  if (i <= 0)
    error("Failed to receive stream id");

  sid = ntohs(sid); // Convert sid to host endianess
  printf("received stream id: %d\n", (int) sid);

  close(s);
  return (sid);
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

void store(char *ip, short int port, char *stream_name, char *filename)
{
  t_data data;
  int s;
  int i;
  int fd;
  int size;
  int16_t streamId = register_stream(ip, port, stream_name);
  struct stat sb;

  printf("storing data from file %s on cakedb\n", filename);

  s = create_network_link(ip, port);

  if (stat(filename, &sb))
    error("Cannot stat file");

  fd = open(filename, O_RDONLY);
  if (fd == -1)
    error("Cannot open file");

  data.cmd = htons(2);
  data.sid = htons(streamId);
  data.length = htonl(sb.st_size);
  write_util(s, &data, sizeof(data));

  send_data(s, fd, sb.st_size);

  close(s);
  close(fd);
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

void query(char *ip, short int port, char *stream_name, char *filename, int64_t timestamp)
{
  int s;
  int fd;
  t_get get;
  int32_t size;
  int16_t streamId = register_stream(ip, port, stream_name);

  printf("querying all data from timestamp %lld to file %s\n",
	 timestamp, filename);

  s = create_network_link(ip, port);

  fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0664);
  if (fd == -1)
    error("Cannot open/create file");

  // Send a query request
  size = 2 + 8; // sid + from
  get.length = htonl(size);
  get.cmd = htons(4);
  get.sid = htons(streamId);
  get.from = htobe64(timestamp);
  // to timestamp is not set, do not send it
  write_util(s, &get, sizeof(get) - sizeof(get.to));

  // Read header of cakedb's response
  read_util(s, &size, sizeof(size));
  size = htonl(size); // Convert size to host endianess

  // Retrieve and write cakedb's query response to file
  recv_data(s, fd, size);

  close(s);
  close(fd);
}

void range(char *ip, short int port, char *stream_name, char *filename, int64_t from, int64_t to)
{
  int s;
  int fd;
  t_get get;
  int32_t size;
  int16_t streamId = register_stream(ip, port, stream_name);

  printf("querying all data in range timestamp [%lld, %lld] to file %s\n",
	 from, to, filename);

  s = create_network_link(ip, port);

  fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0664);
  if (fd == -1)
    error("Cannot open/create file");

  // Send a query request
  size = 2 + 8 + 8; // sid + from + to
  get.length = htonl(size);
  get.cmd = htons(4);
  get.sid = htons(streamId);
  get.from = htobe64(from);
  get.to = htobe64(to);
  write_util(s, &get, sizeof(get));

  // Read header of cakedb's response
  read_util(s, &size, sizeof(size));
  size = htonl(size); // Convert size to host endianess

  // Retrieve and write cakedb's query response to file
  recv_data(s, fd, size);

  close(s);
  close(fd);

}

short int get_port(char *ip)
{
  short int port;
  char *ip_ptr = ip;

  while (*ip != 0 && *ip != ':')
    ++ip;
  if (*ip == ':')
  {
    *ip = 0;
    ++ip;
  }
  port = atoi(ip);

  printf("ip %s\n", ip_ptr);
  printf("port %d\n", (int) port);
  return (port);
}

int main(int argc, char **argv)
{
  char *exename = argv[0];
  char *command = argv[1];
  char *stream_name;
  char *filename;
  int64_t ts_from;
  int64_t ts_to;
  char *ip;
  short int port;

  if (argc < 2)
    usage(exename);

  command = argv[1];

  if (!strcmp("help", command))
  {
    usage(exename);
  }
    
  if (argc < 3)
    usage(exename);

  ip = argv[2];
  // This method modifies the param to leave only the ip
  port = get_port(ip);

  if (!strcmp("register", command))
  {
    if (argc < 4)
      usage(exename);

    stream_name = argv[3];
    register_stream(ip, port, stream_name);
  }
  else if (!strcmp("store", command))
  {
    if (argc < 5)
      usage(exename);

    stream_name = argv[3];
    filename = argv[4];
    store(ip, port, stream_name, filename);
  }
  else if (!strcmp("query", command))
  {
    if (argc < 6)
      usage(exename);

    stream_name = argv[3];
    filename = argv[4];
    ts_from = strtol(argv[5], 0, 10);
    query(ip, port, stream_name, filename, ts_from);
  }
  else if (!strcmp("range", command))
  {
    if (argc < 7)
      usage(exename);

    stream_name = argv[3];
    filename = argv[4];
    ts_from = strtol(argv[5], 0, 10);
    ts_to = strtol(argv[6], 0, 10);
    range(ip, port, stream_name, filename, ts_from, ts_to);
  }
  else
    usage(exename);

  return (EXIT_SUCCESS);
}
