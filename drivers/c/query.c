#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <inttypes.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"
#include "query.h"

/**
 * Send a query (all since) request to cakedb server
 */
void query(struct sockaddr_in saddr, const char *stream_name,
	   const char *filename, int64_t timestamp)
{
  int s;
  int fd;
  t_get get;
  int32_t size;
  int16_t streamId = register_stream(saddr, stream_name);

  printf("querying all data from timestamp %" PRId64 " to file %s\n",
	 timestamp, filename);

  s = create_network_link(saddr);

  fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, 0664);
  if (fd == -1)
    error("Cannot open/create file");

  /* Send a query request */
  size = 2 + 8; /* sid + from */
  get.length = htonl(size);
  get.cmd = htons(4);
  get.sid = htons(streamId);
  get.from = htobe64(timestamp);
  /* to timestamp is not set, do not send it */
  write_util(s, &get, sizeof(get) - sizeof(get.to));

  /* Read header of cakedb's response */
  read_util(s, &size, sizeof(size));
  size = htonl(size); /* Convert size to host endianess */

  /* Retrieve and write cakedb's query response to file */
  recv_data(s, fd, size);

  close(s);
  close(fd);
}

/**
 * Main function for query standalone
 */
void query_main(const char *exename, int argc, const char * const *argv)
{
  struct sockaddr_in saddr;
  const char *stream_name;
  const char *filename;
  int64_t ts_from;

  if (argc < 4)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];
  filename = argv[2];
  ts_from = strtol(argv[3], 0, 10);

  query(saddr, stream_name, filename, ts_from);
}
