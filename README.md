[![Haskell CI](https://img.shields.io/github/actions/workflow/status/input-output-hk/lmdb-simple/haskell.yml?label=Build&style=for-the-badge)](https://github.com/input-output-hk/lmdb-simple/actions/workflows/haskell.yml)
[![handbook](https://img.shields.io/badge/policy-Cardano%20Engineering%20Handbook-informational?style=for-the-badge)](https://input-output-hk.github.io/cardano-engineering-handbook)

# Input-Ouptut fork lmdb-simple

See [INPUT-OUTPUT-FORK.md](./INPUT-OUPUT-FORK.md) for reasons for the current
fork.

We follow processes and guidelines as established by the Consensus team at IOG.
A nice starting point for reading `ouroboros-consensus` documentation is
[ouroboros-network/ouroboros-consensus/README.md](
https://github.com/input-output-hk/ouroboros-consensus/blob/main/README.md).

# Simple Haskell API for LMDB

This package allows you to store arbitrary Haskell values in and retrieve them
from a persistent [Lightning Memory-mapped Database][LMDB] on disk.

  [LMDB]: https://symas.com/lightning-memory-mapped-database/

LMDB is a high-performance [ACID][]-compliant no-maintenance read-optimized
key-value store. Any Haskell type with a [`Serialise`][Serialise] instance can
be stored in an LMDB database, or used as a key to index one.

  [ACID]: https://en.wikipedia.org/wiki/ACID
  [Serialise]: https://hackage.haskell.org/package/serialise/docs/Codec-Serialise-Tutorial.html#g:3

This package provides a few different APIs for using LMDB:

  * The basic API provides transactional `put` and `get` functions to store
    and retrieve values from an LMDB database.

  * The extended API provides many functions similar to those in `Data.Map`,
    e.g. `lookup`, `insert`, `delete`, `foldr`, and so on.

  * The `View` API provides a read-only snapshot of an LMDB database that can
    be queried from pure code.

  * The `DBRef` API provides a mutable variable similar to `IORef` that is
    tied to a particular key in an LMDB database.

