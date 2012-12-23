#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "cakedb-driver.h"
#include "register.h"
#include "store.h"
#include "query.h"
#include "range.h"

/**
 * Simple error handler
 * An error is important, this function will exit the application when used.
 * Use warning instead if exit is too brutal.
 * Logs message to stderr
 */
void error(const char *str)
{
  fprintf(stderr, "Error: %s\n", str);
  exit(EXIT_FAILURE);
}

/**
 * Simple warning handler
 * Logs message to stderr
 */
void warning(const char *str)
{
  fprintf(stderr, "Warning: %s\n", str);
}

/**
 * Print the application's usage
 */
void usage(const char *exename)
{
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, "%s help\n", exename);
  fprintf(stdout, "%s register <server ip:port> <stream name>\n", exename);
  fprintf(stdout, "%s store <server ip:port> <stream name> <filename>\n", exename);
  fprintf(stdout, "%s query <server ip:port> <stream name> <filename> <timestamp>\n", exename);
  fprintf(stdout, "%s range <server ip:port> <stream name> <filename> <TS from> <TS to>\n", exename);
  exit(EXIT_SUCCESS);
}


/******************************/


int main(int argc, const char * const *argv)
{
  const char *exename = argv[0];
  const char *command;
  int command_argc;
  const char * const *command_argv;

  if (argc < 2)
    usage(exename);

  command = argv[1];
  command_argc = argc - 2;
  command_argv = argv + 2;

  if (!strcmp("help", command))
    usage(exename);
    
  if (!strcmp("register", command))
    register_main(exename, command_argc, command_argv);
  else if (!strcmp("store", command))
    store_main(exename, command_argc, command_argv);
  else if (!strcmp("query", command))
    query_main(exename, command_argc, command_argv);
  else if (!strcmp("range", command))
    range_main(exename, command_argc, command_argv);
  else
    usage(exename);

  return (EXIT_SUCCESS);
}
