A transaction-safe, WAL-compatible SQLite3 database backup tool.

Requirements:

* A reasonably POSIX-like operating system with `<stdint.h>` in its C compiler
environment.

* A version of the SQLite3 library built with support for the `sqlite_dbpage`
virtual table.

* A version of zlib supporting the `Z_BLOCK` flush mode.

Only tested on macOS and Linux.
