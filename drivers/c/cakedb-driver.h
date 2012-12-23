#ifndef CAKEDB_DRIVER_H_
# define CAKEDB_DRIVER_H_

#include <sys/types.h>

/**
 * Header of cakedb's protocol
 */
typedef struct s_head
{
  int32_t length;
  int16_t cmd;
} t_head;

/**
 * Header for sending data
 */
typedef struct s_data
{
  int32_t length;
  int16_t cmd;
  int16_t sid;
} t_data;


/**
 * Header for querying data
 */
typedef struct s_get
{
  int32_t length;
  int16_t cmd;
  int16_t sid;
  int64_t from;
  int64_t to;
} t_get;

/****************************/

/**
 * Simple error handler
 * An error is important, this function will exit the application when used.
 * Use warning instead if exit is too brutal.
 * Logs message to stderr
 */
void error(const char *str);

/**
 * Simple warning handler
 * Logs message to stderr
 */
void warning(const char *str);

/**
 * Print the application's usage
 */
void usage(const char *exename);

#endif	/* CAKEDB_DRIVER_H_ */
