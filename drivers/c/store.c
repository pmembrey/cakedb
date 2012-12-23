#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "cakedb-driver.h"
#include "network.h"
#include "register.h"
#include "store.h"

void store(struct sockaddr_in saddr, char *stream_name, char *filename)
{
  t_data data;
  int s;
  int fd;
  int16_t streamId = register_stream(saddr, stream_name);
  struct stat sb;

  printf("storing data from file %s on cakedb\n", filename);

  s = create_network_link(saddr);

  if (stat(filename, &sb))
    error("Cannot stat file");

  fd = open(filename, O_RDONLY);
  if (fd == -1)
    error("Cannot open file");

  data.header.cmd = htons(2);
  data.sid = htons(streamId);
  data.header.length = htonl(sb.st_size);
  write_util(s, &data, sizeof(data));

  send_data(s, fd, sb.st_size);

  close(s);
  close(fd);
}

void store_main(char *exename, int argc, char **argv)
{
  struct sockaddr_in saddr;
  char *stream_name;
  char *filename;

  if (argc < 3)
    usage(exename);

  saddr = parseIp(argv[0]);
  stream_name = argv[1];
  filename = argv[2];
  store(saddr, stream_name, filename);
}
