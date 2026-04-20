/*
 * CSE 550 Assignment 1
 * AMTED Server
 */

#define _GNU_SOURCE
#include <stdio.h>      // Used for printf, fprintf, perror
#include <stdlib.h>     // Used for malloc, free, exit, atoi
#include <string.h>     // Used for memset, strncpy
#include <unistd.h>     // Used for read, write, close, pipe
#include <errno.h>      // Used for errno, EAGAIN, EWOULDBLOCK, EINTR
#include <fcntl.h>      // Used for fcntl, O_NONBLOCK, O_RDONLY
#include <signal.h>     // Used for sigaction, SIGPIPE, SIG_IGN
#include <pthread.h>    // Used for pthread_t, pthread_mutex_t, pthread_cond_t, pthread_create
#include <sys/epoll.h>  // Used for epoll_create1, epoll_ctl, epoll_wait, EPOLLIN, EPOLLOUT
#include <sys/socket.h> // Used for socket, bind, listen, accept, setsockopt, SO_REUSEADDR
#include <sys/types.h>  // Used for ssize_t, socklen_t
#include <netinet/in.h> // Used for sockaddr_in, htons, SOMAXCONN
#include <arpa/inet.h>  // Used for inet_pton, inet_ntoa

#define NUM_ARGS 3              // expected argc: program name, host, port
#define MAX_CLIENTS 16          // maximum simultaneous client connections
#define MAX_PATH_LEN 254        // maximum file path length in bytes
#define MAX_FILE_SIZE (2 * 1024 * 1024) // maximum file size read into memory
#define MAX_EVENTS 32           // maximum epoll events returned per wait call
#define THREADPOOL_SIZE 4       // number of worker threads for disk I/O
#define TASK_QUEUE_LEN  32      // capacity of the thread pool task queue

// File states for a client connection. Describes whether the client is still providing a file path
// or is in the process for waiting
typedef enum {
    READING_PATH,  // waiting to receive the full file path from the client
    WAITING_FILE,  // path received, worker thread is reading the file from disk
    WRITING_FILE,  // file is in memory, main thread is sending it to the client
} conn_state_t;

// Per-client connection state. The main thread will keep track of all connections and handle them.
typedef struct {
    int fd;                         // client socket file descriptor, -1 indicates slot is free
    conn_state_t state;             // current state of this connection

    char pathbuf[MAX_PATH_LEN + 2]; // buffer for the incoming file path
    int pathlen;                    // number of bytes received into path buffer so far

    char *file_data;                // buffer holding the file contents
    size_t file_size;               // number of bytes in file
    size_t bytes_sent;              // how many bytes have been sent so far
} conn_t;

// A task is the unit of work placed into the thread pool queue, and used by the worker threads.
typedef struct {
    int cfd;                    // client this task is serving
    char path[MAX_PATH_LEN + 1]; // null-terminated file path to read
} task_t;

// The threadpool mananges the set of worker threads, deploying new ones when a new client connects
typedef struct {
    pthread_t threads[THREADPOOL_SIZE]; // worker threads
    task_t queue[TASK_QUEUE_LEN];       // circular buffer of tasks provided by clients
    int head, tail, count;              // ring buffer indices and current task count
    pthread_mutex_t mu;                 // lock which protects the threadpool metadata
    pthread_cond_t cond;                // the condition on which threads sleep on
} threadpool_t;

// Message sent from a worker thread to the main thread via the pipe to indicate worker has finished
typedef struct {
    int    cfd;       // client this result belongs to
    char  *file_data; // file buffer, NULL signals an error
    size_t file_size; // number of valid bytes in file_data
} notify_t;

static int pipe_fd[2];            // pipes for communication between worker and main threads
static conn_t conns[MAX_CLIENTS]; // fixed size connection list for new clients
static threadpool_t pool;         // static threadpool for AMTED server

// Helper function to set certain file descriptors as non-blocking
static int set_nonblocking(int fd) {
    // First retrieve the flags associated with the file descriptor and update with non-block flag
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Helper function to add new file descriptors to the interest list of epoll
static void epoll_add(int fd, int epfd, uint32_t events) {
    // Take the file descriptor of the object wanting to be added with the epoll file descriptor
    // and simply add the new file descriptor.
    struct epoll_event ev;
    ev.events  = events;
    ev.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

// Helper function to modify the status of a certain file descriptor, specfically to indicate
// a certain file descriptor is ready to read(EPOLLIN) vs write(EPOLLOUT)
static void epoll_mod(int fd, int epfd, uint32_t events) {
    struct epoll_event ev;
    ev.events  = events;
    ev.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// Main worker function that all threads run through the lifecycle of the AMTED server.
static void *worker_func(void *arg) {
    (void)arg; // To ignore compiler warnings, since this is not needed, but defined by POSIX
    task_t task;
    notify_t notif;
    int file_fd;
    char *buf;
    ssize_t total;
    ssize_t bytes_read;

    // Loop indefinitely
    while (1) {
        // Initially acquire lock
        pthread_mutex_lock(&pool.mu);

        // If no tasks are available, the thread should sleep
        while (pool.count == 0) {
            // Release lock, and wait on the pool condition
            pthread_cond_wait(&pool.cond, &pool.mu);
        }

        // If the worker thread reaches this point, a new task has arrived. Dequeue the task and,
        // move the shift the pool queue to indicate this in the thread pool.
        task = pool.queue[pool.head];
        pool.head = (pool.head + 1) % TASK_QUEUE_LEN;
        pool.count--;
        // Since we are done interacting with pool metadata, we can relinquish the lock
        pthread_mutex_unlock(&pool.mu);

        // Initialize a notify message to send back to the main thread
        notif.cfd = task.cfd;
        notif.file_data = NULL;
        notif.file_size = 0;

        // Take the worker task, and process it. Start by taking the file path passed through the
        // task and open the file.
        file_fd = open(task.path, O_RDONLY);
        if (file_fd >= 0) {
            // If file was successfully opened, allocate a new buffer to use.
            buf = malloc(MAX_FILE_SIZE);
            if (!buf) {
                perror("malloc failed");
            } else {
                total = 0;
                bytes_read = 0;

                // Start reading the entire file into the buffer
                while ((size_t)total < MAX_FILE_SIZE) {
                    bytes_read = read(file_fd, buf + total, MAX_FILE_SIZE - total);
                    if (bytes_read <= 0) {
                        // Either the file is fully read, or an error was encountered
                        break;
                    }
                    // Increment total bytes read
                    total += bytes_read;
                }

                if (bytes_read < 0) {
                    // If no bytes were read as a whole or an error was recorded, free the buffer
                    // and do not add any data to the notification message to indicate an error
                    free(buf);
                } else {
                    // Otherwise do update the notification message
                    notif.file_data = buf;
                    notif.file_size = (size_t)total;
                }
            }
            // Close the file after reading
            close(file_fd);
        }

        // Indicate to the main thread that this worker has finished by writing to the write pipe
        if (write(pipe_fd[1], &notif, sizeof(notif)) < 0) {
            // If the write fails, simply free the file buffer, cannot continue task
            free(notif.file_data);
        }
    }
    // Return to indicate the worker has finished its task.
    return NULL;
}

// Threadpool function to add a new client task into the threadpool queue, so a worker thread
// can work on the provided task.
static int threadpool_dispatch(int cfd, const char *path) {
    task_t *t;

    // Acquire the lock on the threadpool
    pthread_mutex_lock(&pool.mu);
    // Check if the pool is full (should never happen as the max number of clients is less)
    if (pool.count == TASK_QUEUE_LEN) {
        // Cannot add the task so return an error
        pthread_mutex_unlock(&pool.mu);
        return -1;
    }

    // Get the slot at the tail of the pool queue, and update the fields properly
    t = &pool.queue[pool.tail];
    t->cfd = cfd;
    strncpy(t->path, path, MAX_PATH_LEN);
    // Terminate the file path (should already be terminated, but in case)
    t->path[MAX_PATH_LEN] = '\0';

    // Shift the queue pointers
    pool.tail = (pool.tail + 1) % TASK_QUEUE_LEN;
    pool.count++;

    // Signal to a thread that a new task has arrived and release the lock.
    pthread_cond_signal(&pool.cond);
    pthread_mutex_unlock(&pool.mu);
    return 0;
}

// Function to handle reading from the client. Specifically takes in the file path provided by the
// client.
static void handle_read(conn_t *conn, int epfd) {
    int fd = conn->fd;
    int prev_len;
    int k;
    ssize_t bytes_read;

    // Loop until the newline character is found in the stream
    while (1) {
        // Read however many bytes can be read
        prev_len = conn->pathlen;
        bytes_read = read(fd, &conn->pathbuf[conn->pathlen],
                          (MAX_PATH_LEN + 1) - prev_len);
 
        // If no bytes were read or a fatal error was encountered, close client and clear
        // the connection field for reuse.
        if (bytes_read == 0 ||
            (bytes_read < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
            close(fd);
            conn->fd = -1;
            return;
        }

        // A non-fatal error has occurred, return and the read will be tried again through the epoll
        // system
        if (bytes_read < 0) {
            return;
        }

        // Update the number of bytes in the file path
        conn->pathlen += bytes_read;

        // Scan the newly read bytes for the newline character
        for (k = prev_len; k < conn->pathlen; k++) {
            // Newline char was found
            if (conn->pathbuf[k] == '\n') {
                // Terminate file path
                conn->pathbuf[k] = '\0';
                // Advance the connection state into waiting
                conn->state = WAITING_FILE;
                // Add the new task to the threadpool
                if (threadpool_dispatch(conn->fd, conn->pathbuf) < 0) {
                    // If task could not be added, close client and free conn slot
                    close(fd);
                    conn->fd = -1;
                } else {
                    // Otherwise, threadpool was able add task, tell the epoll manager to not
                    // bother the main thread with events related to this file descriptor
                    epoll_mod(fd, epfd, 0);
                }
                // Return since new task was successfully was created
                return;
            }
        }

        // If newline char was not found, and the max length was exceeded, close client and free
        // conn slot.
        if (conn->pathlen >= MAX_PATH_LEN) {
            close(fd);
            conn->fd = -1;
            return;
        }
    }
}

// Handle writing to the client. Writes the requested file back to the client.
static void handle_write(conn_t *conn) {
    int fd = conn->fd;
    size_t remaining;
    ssize_t bytes_write;

    // Loop until all the file contents have been written back to the client.
    while (1) {
        // Calculate how many bytes need to transferred to client.
        remaining = conn->file_size - conn->bytes_sent;
        if (remaining == 0) {
            // If none are remaining, we can free the file data buffer, close the client, and reset
            // the conn slot.
            free(conn->file_data);
            conn->file_data = NULL;
            close(fd);
            conn->fd = -1;
            return;
        }

        // Keep writing to the client as long bytes remain.
        bytes_write = write(fd, conn->file_data + conn->bytes_sent, remaining);
        if (bytes_write < 0) {
            // An error was encountered.
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Non-fatal error, simply return, the epoll manager will handle a retry.
                return;
            }
            // Fatal error, free file data, clear conn slot, and close client.
            free(conn->file_data);
            conn->file_data = NULL;
            close(fd);
            conn->fd = -1;
            return;
        }

        // Update number of bytes written to the client.
        conn->bytes_sent += bytes_write;
    }
}

// Initializes the threadpool at the start of the server.
static void init_threadpool(void) {
    int i;

    // Initialize locks and waiting conditions.
    pthread_mutex_init(&pool.mu, NULL);
    pthread_cond_init(&pool.cond, NULL);
    // Establish all the worker threads, and start waiting.
    for (i = 0; i < THREADPOOL_SIZE; i++) {
        pthread_create(&pool.threads[i], NULL, worker_func, NULL);
    }
}

// Initializes the server socket to be used.
static int setup_server_socket(const char *host, int port) {
    int opt = 1;
    int server_fd;
    struct sockaddr_in addr;

    // Create a new file descriptor for the server.
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Set the options for the socket
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    // Set the server file descriptor as non-blocking
    set_nonblocking(server_fd);

    // Insert the relevant host and port into the address struct
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) < 1) {
        fprintf(stderr, "Invalid address: %s\n", host);
        exit(EXIT_FAILURE);
    }

    // Bind the server file descriptor to said host and port
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Start listening on said host and port
    if (listen(server_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    // Return the associated file descriptor
    return server_fd;
}

// Handles a new client connecting to the server.
static void handle_accept(int server_fd, int epfd) {
    struct sockaddr_in caddr;
    socklen_t clen;
    int cfd;
    conn_t *conn;
    int j;

    // Loop, waiting for new clients to come in
    while (1) {
        // Block and wait for clients to connect. Accept once client does connect.
        clen = sizeof(caddr);
        cfd = accept(server_fd, (struct sockaddr*)&caddr, &clen);
        if (cfd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("accept failed");
            }
            break;
        }

        // Set the newly established client file descriptor as non-blocking
        set_nonblocking(cfd);
        printf("New connection accepted: fd=%d from %s:%d\n",
               cfd, inet_ntoa(caddr.sin_addr), ntohs(caddr.sin_port));

        // Find a new slot in the conn table.
        conn = NULL;
        for (j = 0; j < MAX_CLIENTS; j++) {
            if (conns[j].fd < 0) {
                conn = &conns[j];
                break;
            }
        }

        // No slot was found, close client
        if (!conn) {
            close(cfd);
            fprintf(stderr, "exceeded %d clients\n", MAX_CLIENTS);
            continue;
        }

        // Establish the chosen conn slot with the correct fields.
        memset(conn, 0, sizeof(conn_t));
        conn->fd = cfd;
        conn->state = READING_PATH; // Indicate that the client is currently sending file path
        // Add the client to the epoll interest list
        epoll_add(cfd, epfd, EPOLLIN);
    }
}

// Handles writes from the pipe, when a worker thread finishes a task successfully.
static void handle_pipe_notification(int epfd) {
    notify_t notif;
    conn_t *conn;
    int j;

    // Keep reading from the pipe, while a new notification message exists in the pipe.
    while (read(pipe_fd[0], &notif, sizeof(notif)) == sizeof(notif)) {
        // Search for the client file descriptor, whose task was completed
        conn = NULL;
        for (j = 0; j < MAX_CLIENTS; j++) {
            if (conns[j].fd == notif.cfd) {
                conn = &conns[j];
                break;
            }
        }

        // If no client was found, or the client conn is in the wrong state, free file data
        // and continue reading
        if (!conn || conn->state != WAITING_FILE) {
            free(notif.file_data);
            continue;
        }

        // If the file data provided in the conn slot is empty, close the client and free the slot
        if (!notif.file_data) {
            close(conn->fd);
            conn->fd = -1;
            continue;
        }

        // Update the conn slot's fields from the notification message
        conn->file_data = notif.file_data;
        conn->file_size = notif.file_size;
        conn->bytes_sent = 0;
        conn->state = WRITING_FILE;
        // Indicate that this client is ready to be written to and should be added back to the 
        // interest list
        epoll_mod(conn->fd, epfd, EPOLLOUT);
    }
}

// Function associated with the epoll manager. Will handle events associated with the client.
static void handle_client_event(int fd, int epfd) {
    conn_t *conn;
    int j;

    // Find the conn slot associated with the client that had an event occur.
    conn = NULL;
    for (j = 0; j < MAX_CLIENTS; j++) {
        if (conns[j].fd == fd) {
            conn = &conns[j];
            break;
        }
    }

    // If client does not exist, return.
    if (!conn) {
        return;
    }

    // Check if the client with the event is ready to transmit path or accept file data
    if (conn->state == READING_PATH) {
        handle_read(conn, epfd);
    } else if (conn->state == WRITING_FILE) {
        handle_write(conn);
    }
}

int main(int argc, char **argv) {
    struct sigaction sa;
    struct epoll_event events[MAX_EVENTS];
    int server_fd;
    int epfd;
    int nfds;
    int fd;
    int i;

    if (argc != NUM_ARGS) {
        fprintf(stderr, "Usage: %s <address> <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Establish conn table for client slots
    for (i = 0; i < MAX_CLIENTS; i++) {
        conns[i].fd = -1;
    }

    // Initialize threadpool.
    init_threadpool();

    // Ignore SIGPIPE signals, so the client/server does crash unexpectedly, rather will gracefully
    // close
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPIPE, &sa, NULL);

    // Initialize the pipe
    if (pipe(pipe_fd) < 0) {
        perror("pipe failed");
        exit(EXIT_FAILURE);
    }
    // Set the pipe to be non-blocking
    set_nonblocking(pipe_fd[0]);

    // Setup server and start listening on host/port
    server_fd = setup_server_socket(argv[1], atoi(argv[2]));

    // Create the epoll instance
    epfd = epoll_create1(0);
    if (epfd < 0) {
        perror("epoll_create1 failed");
        exit(EXIT_FAILURE);
    }
    
    // Add both the server and read pipe to the epoll interest list
    epoll_add(server_fd, epfd, EPOLLIN);
    epoll_add(pipe_fd[0], epfd, EPOLLIN);

    printf("AMTED Server now listening on %s:%d\n", argv[1], atoi(argv[2]));

    // Loop waiting for events to occur.
    while (1) {
        // Cause the main thread to wait for epoll events.
        nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            // Error finding any events
            if (errno == EINTR) {
                // Non-fatal so continue
                continue;
            }
            // Fatal so exit
            perror("epoll_wait failed");
            break;
        }

        // For each of the file descriptors that had an event occur, process independently
        for (i = 0; i < nfds; i++) {
            fd = events[i].data.fd;
            if (fd == server_fd) {
                // Server event, new client connecting
                handle_accept(server_fd, epfd);
            } else if (fd == pipe_fd[0]) {
                // Pipe notification, so accept worker finish
                handle_pipe_notification(epfd);
            } else {
                // Client notification, either client writing file path or ready to receive data
                handle_client_event(fd, epfd);
            }
        }
    }

    // Exited the loop, server closing, so clean up conn table
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (conns[i].fd >= 0) {
            free(conns[i].file_data);
            close(conns[i].fd);
        }
    }

    // Close server related file descriptors
    close(pipe_fd[0]);
    close(pipe_fd[1]);
    close(server_fd);
    close(epfd);

    return EXIT_SUCCESS;
}