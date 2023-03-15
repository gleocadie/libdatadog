#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

void __attribute__((constructor)) init();

void init() {
  pid_t child_pid = fork();

  if (child_pid == 0) {
    // child
    // run the datadog-agent
    char *argv[] = {"datadog-agent", "run", NULL};
    // override the environment variables so we don't keep trying to LD_PRELOAD
    // in an infinite loop
    char *envp[] = {"HOME=/", "PATH=/bin:/usr/bin", "DD_LOG_LEVEL=debug", "DD_LOGS_ENABLED=1",
                    NULL};
    execve(argv[0], &argv[0], envp);
  } else {
    // parent
    printf("Running from parent\n");
    sleep(3);
    // return main_orig(argc, argv, envp);
  }
}
