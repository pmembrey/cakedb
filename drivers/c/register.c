#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"

/**
 * Send a register request to cakedb server
 */
int16_t register_stream(struct sockaddr_in saddr, const char *stream_name)
{
  t_head header;
  int s;
  int32_t i;
  int16_t sid;

  printf("registering stream %s\n", stream_name);

  s = create_network_link(saddr);

  /* Send an register stream request */
  header.cmd = htons(5);
  i = strlen(stream_name);
  header.length = htonl(i);

  /* Send header to cakedb */
  write_util(s, &header, sizeof(header.cmd) + sizeof(header.length));

  /* Send stream name to cakedb */
  write_util(s, stream_name, i);

  /* Read stream id returned by cakedb */
  read_util(s, &sid, sizeof(sid));

  /* Convert sid to host endianess */
  sid = ntohs(sid);
  printf("received stream id: %d\n", (int) sid);

  close(s);
  return (sid);
}

/**
 * Main function for register standalone
 */
void register_main(const char *exename, int argc, const char * const *argv)
{
  struct sockaddr_in saddr;
  const char *stream_name;

  if (argc < 2)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];

  register_stream(saddr, stream_name);
}
