Michael Ibrahim, micibr@cs.washington.edu
Sahil Gupta sahil19@cs.washington.edu

Pipe Concurrency:

We do not have any synchronization on the pipe even though multiple threads may write to the pipe 
concurrently. POSIX provides the following guarantee, which is also listed on the `pipe()` man page.
"POSIX.1 says that writes of less than `PIPE_BUF` bytes must be atomic". On Linux, `PIPE_BUF` is defined
as 4096 bytes, which is far less than the notification message size we use, and thus writes are never 
interleaved, and synchronization is not required.