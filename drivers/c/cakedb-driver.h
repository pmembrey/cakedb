#ifndef CAKEDB_DRIVER_H_
# define CAKEDB_DRIVER_H_

#include <sys/types.h>

typedef struct s_head
{
  int32_t length;
  int16_t cmd;
} t_head;

typedef struct s_data
{
  t_head header;
  int16_t sid;
} t_data;

typedef struct s_get
{
  t_head header;
  int16_t sid;
  int64_t from;
  int64_t to;
} t_get;

void error(const char *str);

void warning(const char *str);

void usage(char *exename);

#endif	/* CAKEDB_DRIVER_H_ */
