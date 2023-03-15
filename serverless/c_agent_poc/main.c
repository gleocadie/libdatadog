#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int (*main_orig)(int, char **, char **);

int main_hook(int argc, char **argv, char **envp) {
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
    return 0;
  } else {
    // parent
    printf("Running from parent\n");
    sleep(3);
    return main_orig(argc, argv, envp);
  }
}

int __libc_start_main(int (*main)(int, char **, char **), int argc, char **argv,
                      int (*init)(int, char **, char **), void (*fini)(void),
                      void (*rtld_fini)(void), void *stack_end) {
  main_orig = main;

  typeof(&__libc_start_main) orig = dlsym(RTLD_NEXT, "__libc_start_main");

  return orig(main_hook, argc, argv, init, fini, rtld_fini, stack_end);
}
