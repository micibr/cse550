#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#define MAX_LINE 4096
#define MAX_PROGS 64
#define MAX_ARGS 64

// Split lines into argv arrays
static int parse_args(char *prog, char *argv[MAX_ARGS]) {
    int argc = 0;
    char *tok = strtok(prog, " \t");
    while (tok && argc < MAX_ARGS - 1) {
        argv[argc++] = tok;
        tok = strtok(NULL, " \t");
    }
    argv[argc] = NULL;
    return argc;
}

// Return number of programs, and fill progs with pointers to each program string.
static int parse_pipeline(char *line, char *progs[MAX_PROGS]) {
    int n = 0;
    char *tok = strtok(line, "|");
    while (tok && n < MAX_PROGS) {
        // trim whitespace from both ends of the program string
        while (*tok == ' ' || *tok == '\t') tok++;
        char *end = tok + strlen(tok) - 1;
        while (end > tok && (*end == ' ' || *end == '\t' || *end == '\n')) *end-- = '\0';
        progs[n++] = tok;
        tok = strtok(NULL, "|");
    }
    return n;
}

static void run_pipeline(char *progs[], int n) {
    // pipes[i] connects program i to program i+1
    int pipes[MAX_PROGS - 1][2];
    for (int i = 0; i < n - 1; i++) {
        if (pipe(pipes[i]) < 0) {
            perror("pipe");
            return;
        }
    }

    pid_t pids[MAX_PROGS];
    for (int i = 0; i < n; i++) {
        char *argv[MAX_ARGS];
        if (parse_args(progs[i], argv) == 0) continue;

        pids[i] = fork();
        if (pids[i] < 0) {
            perror("fork");
            return;
        }
        if (pids[i] == 0) {
            if (i > 0) {
                dup2(pipes[i - 1][0], STDIN_FILENO);
            }
            if (i < n - 1) {
                dup2(pipes[i][1], STDOUT_FILENO);
            }
            // close pipes
            for (int j = 0; j < n - 1; j++) {
                close(pipes[j][0]);
                close(pipes[j][1]);
            }
            execvp(argv[0], argv);
            perror(argv[0]);
            exit(1);
        }
    }

    // close all pipe fds in parent
    for (int i = 0; i < n - 1; i++) {
        close(pipes[i][0]);
        close(pipes[i][1]);
    }

    // wait for all children
    for (int i = 0; i < n; i++) {
        waitpid(pids[i], NULL, 0);
    }
}

int main(void) {
    char line[MAX_LINE];

    while (1) {
        if (isatty(STDIN_FILENO))
            printf("550shell> ");

        if (!fgets(line, sizeof(line), stdin))
            break;

        // strip newline and skip empty lines
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0') continue;

        char *progs[MAX_PROGS];
        int n = parse_pipeline(line, progs);
        if (n > 0)
            run_pipeline(progs, n);
    }

    return 0;
}
