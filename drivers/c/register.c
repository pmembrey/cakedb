#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"

int16_t register_stream(struct sockaddr_in saddr, char *stream_name)
{
  t_head reg;
  int s;
  int i;
  int16_t sid;

  printf("registering stream %s\n", stream_name);

  s = create_network_link(saddr);

  reg.cmd = htons(5);
  i = strlen(stream_name);
  reg.length = htonl(i);

  write_util(s, &reg, sizeof(reg));
  write_util(s, stream_name, i);

  read_util(s, &sid, sizeof(sid));

  sid = ntohs(sid); // Convert sid to host endianess
  printf("received stream id: %d\n", (int) sid);

  close(s);
  return (sid);
}

void register_main(char *exename, int argc, char **argv)
{
  struct sockaddr_in saddr;
  char *stream_name;

  if (argc < 2)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];

  register_stream(saddr, stream_name);
}
