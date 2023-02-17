# Reasons for the fork

Here we expose the reasons for the Input Output Global fork `lmdb-simple`.

A dedicated Hackage instance called [Cardano Haskell Packages
(CHaP)](https://github.com/input-output-hk/cardano-haskell-packages) exists for
Haskell packages that are primarily used within the Cardano ecosystem. The
Consensus team at Input-Output Global uses forks of the `lmdb` and `lmdb-simple`
packages for developing a state-of-the-art storage component. Since it has
become policy to no longer rely on `source-repository-package` stanzas in
Cardano projects, we publish our forks to CHaP under the names `cardano-lmdb`
and `cardano-lmdb-simple`. If time and opportunity permits, we would try
upstreaming our contributions to the main repository.