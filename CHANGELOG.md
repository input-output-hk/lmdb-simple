## next release — ????-??-??

### Breaking

- Restrict base to `>=4.14` and `<4.20`, which coincides with the GHC versions that
  we have tested the package with.

### Patch

- Add compatibility for GHC 9.8.

## 0.6.1.1 — 2023 08-01

### Patch

- Upgrade code to GHC 9.6.

## 0.6.1.0 — 2023-05-11

### Non-Breaking

- Add `Show` and `Eq` instances for `Limits`

## 0.6.0.0 — 2023-03-08

### Non-Breaking

- Add `TransactionHandle`s in new `Database.LMDB.Simple.TransactionHandle`
  module. These `TransactionHandle`s are a wrapper around internal LMDB
  transaction handles, which keep open a consistent view of the database as long
  as the handle is live.

- Add `copyEnvironment` and `getPathEnvironment` functions.

### Breaking

- Annotate cursor monads with transaction modes. This prevents read-write cursor
  operations from running in read-only environments.

- Represent transaction/environment modes as a `Mode` data kind instead of as
  `Type`s. As a consequence, restrict types that are parameterised by modes
  (such as `Transaction`, `Environment`, `TransactionHandle` and `CursorM`) to
  `Mode`-kinded type arguments. e.g.,
  ```haskell
  type Transaction :: Mode -> Type -> Type
  ```
- Rename `Mode` type class to `IsMode`. Its kind is restricted to `Mode`-kinded
  types.

- Remove definitions for read-write transaction handles from the exports list of the `TransactionHandle` module. See [issue 14](https://github.com/input-output-hk/lmdb-simple/issues/14#issuecomment-1446841800).

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
