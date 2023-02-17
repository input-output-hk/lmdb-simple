
<a id='changelog-0.5.0.0'></a>
## 0.5.0.0 — 2023-02-17

First release of `lmdb-simple` fork. See
[INPUT-OUTPUT-FORK.md](./INPUT-OUTPUT-FORK.md).

This changelog describes changes from revision
`9ffca8a30489e7f0aafd77cf73374979c27eb97d` onwards, which include all changes
introduced by the Consensus team.

### Non-Breaking

- Expose the `Database.LMDB.Simple.Internal` module.
- Add a monadic cursor API that provides fine-grained access to cursors. This
  new API lives in `Database.LDMB.Simpel.Cursor`.
- Add a blocking version of closing LMDB environments.
- Expose `marshalOutBS`, `copyLazyBS` and `pokeMDBVal` from
  `Database.LMDB.Simple.Internal`.
- Add `pokeMDBVal` counterpart to `peekMDBVal`.
- Add `putNoOverwrite` function that is similar to `put`, but disallows
  overwrites.

### Breaking

- Rename package from `lmdb-simple` to `cardano-lmdb-simple`.
- `copyLazyBS` throws an error if the copy is incomplete.
- Rename `peekVal` to `peekMDBVal`
