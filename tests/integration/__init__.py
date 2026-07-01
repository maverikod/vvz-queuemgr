"""
Integration test package for queuemgr.

These tests exercise queuemgr end-to-end using real child processes and, in
several cases, real TCP sockets. They are written to reproduce the casmgr
production incident: queuemgr forking a live asyncio server process (with
open listener sockets / epoll fds) corrupted the parent's HTTP request
parsing after a job ran through the queue.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""
