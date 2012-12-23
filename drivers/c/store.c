#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"
#include "store.h"

/**
 * Send a store (append) request to cakedb server
 */
void store(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename)
{
  t_data data;
  int s;
  int fd;
  int32_t size;
  int16_t streamId = register_stream(saddr, stream_name);
  struct stat sb;

  printf("storing data from file %s on cakedb\n", filename);

  s = create_network_link(saddr);

  if (stat(filename, &sb))
    error("Cannot stat file");

  fd = open(filename, O_RDONLY);
  if (fd == -1)
    error("Cannot open file");

  /* sid + file's data */
  size = 2 + sb.st_size;
  data.length = htonl(size);
  /* Send an append request */
  data.cmd = htons(2);
  data.sid = htons(streamId);

  /* send data header and sid */
  write_util(s, &data, sizeof(data));

  /* Read file and send data to cakedb */
  send_data(s, fd, sb.st_size);

  close(s);
  close(fd);
}

/**
 * Main function for store standalone
 */
void store_main(const char *exename, int argc, const char * const *argv)
{
  struct sockaddr_in saddr;
  const char *stream_name;
  const char *filename;

  if (argc < 3)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];
  filename = argv[2];

  store(saddr, stream_name, filename);
}
