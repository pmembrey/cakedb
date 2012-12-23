#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"
#include "range.h"

/**
 * Send a range (simple query) request to cakedb server
 */
void range(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename, int64_t from, int64_t to)
{
  int s;
  int fd;
  t_get get;
  int32_t size;
  int16_t streamId = register_stream(saddr, stream_name);

  printf("querying all data in range timestamp [%lld, %lld] to file %s\n",
	 from, to, filename);

  s = create_network_link(saddr);

  fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0664);
  if (fd == -1)
    error("Cannot open/create file");

  /* Send a query request */
  size = 2 + 8 + 8; /* sid + from + to */
  get.length = htonl(size);
  get.cmd = htons(3);
  get.sid = htons(streamId);
  get.from = htobe64(from);
  get.to = htobe64(to);
  write_util(s, &get, sizeof(get));

  /* Read header of cakedb's response */
  read_util(s, &size, sizeof(size));
  size = htonl(size); /* Convert size to host endianess */

  /* Retrieve and write cakedb's query response to file */
  recv_data(s, fd, size);

  close(s);
  close(fd);

}

/**
 * Main function for range standalone
 */
void range_main(const char *exename, int argc, const char * const *argv)
{
  struct sockaddr_in saddr;
  const char *stream_name;
  const char *filename;
  int64_t ts_from;
  int64_t ts_to;

  if (argc < 5)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];
  filename = argv[2];
  ts_from = strtol(argv[3], 0, 10);
  ts_to = strtol(argv[4], 0, 10);

  range(saddr, stream_name, filename, ts_from, ts_to);
}
