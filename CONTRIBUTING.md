# Contributing

## Building

Building the project requires LMDB to be installed on your system. Either build
LMDB from source on any system, or use a package manager on Ubuntu/MacOS, or use
`ghcup` on Windows.

* **Ubuntu**:
  ```
  apt-get update
  apt-get install pkg-config liblmdb-dev
  ```
* **MacOS**:
  ```
  brew update
  brew install pkg-config lmdb
  ```
* **Windows**:
  ```
  ghcup run -- pacman --noconfirm -S `
    mingw-w64-x86_64-pkg-config `
    mingw-w64-x86_64-lmdb
  ```
  Make sure that the path to the install folder is included in your `PATH`
  variable, e.g., "C:\msys64\bin".


The project is built using `ghc` and `cabal`.

```
cabal update
cabal build all
```

## Testing

Tests are run using `cabal`.

```
cabal build all
cabal test all
```

## Code style

There is no strict code style, but try to keep the code style consistent
throughout the repository and favour readability. Code should be well-documented and well-tested.

## Formatting

We enforce no formatter for Haskell or Cabal files, but try to keep the
formatting consistent throughout the repository. We suggest using
`stylish-haskell` and `cabal-fmt` to keep formatting consistent.

## Pull requests

The following are requirements for merging a PR into `master`:
* Each commit should be small and should preferably address one thing. Commit messages should be useful.
* Document and test your changes.
* The PR should have a useful description, and it should link issues that it
  resolves (if any).
* Changes introduced by the PR should be recorded in the relevant changelog
  files.
* PRs should not bundle many unrelated changes.
* PRs should be approved by at least 1 developer.
* The PR should pass all CI checks.

## Releases

Releases follow the [Haskell Package Versioning Policy](https://pvp.haskell.org/). We use version numbers consisting of 4 parts, like `A.B.C.D`.
* `A.B` is the *major* version number. A bump indicates a breaking change.
* `C` is the *minor* version number. A bump indicates a non-breaking change.
* `D` is the *patch* version number. A bump indicates a small, non-breaking patch.