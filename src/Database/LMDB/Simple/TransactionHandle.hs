{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE KindSignatures           #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE Rank2Types               #-}
{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications         #-}

{-# OPTIONS_GHC -Wno-redundant-constraints #-}

{- | Transactions handles.

  There are two downsides to regular @'Transaction'@s:
  * The current @'Transaction'@ runners (like @'readWriteTransaction'@) will
    create and destroy an internal transaction handle (@'MDB_txn'@) in a
    bracketed style. Since internal transaction handles keep open a consistent
    view of the database, this means that the transaction inside the bracket
    will not be affected by other effects. If we want to perform several
    transactions to the same consistent view, we would need to keep the
    transaction handle open for longer, but the bracketed allocate-free style
    for internal transaction handles prevents this.
  * A @'Transaction'@ will only produce results when it is run with
    @'readWriteTransaction'@ or one of the other runners. This prevents us from
    getting intermediate results or performing intermediate writes (side
    effects). Instead, all results and effects will be performed in one go.

  A @'TransactionHandle'@ will keep open an internal LMDB transaction handle
  (@'MDB_txn'@) in an @'MVar'@, such that we can submit the usual
  @'Transaction'@s to it. Each submission will provide its results and peform
  its side effects right away.

  Note on terminology: whenever we speak of a transaction handle, we refer to a
  @'TransactionHandle'@, unless we say it is an /internal (LMDB)/ transaction
  handle, in which case we refer to an @'MDB_txn'@.

  We can currently not implement the submission of read-write transactions,
  because the lower-level Haskell bindings are set up in such a way that the
  internal 'MDB_txn' should be created and completed in a single bound Haskell
  thread (see 'mdb_txn_begin' to see why this is imposed by LMDB itself). This
  is not compatible with the TransactionHandle use case, where the 'MDB_txn' is
  created separately from where it is used. As such, only 'ReadOnly'
  transactions are provided by this module.
-}
module Database.LMDB.Simple.TransactionHandle (
    TransactionHandle
  , abort
  , commit
  , newReadOnly
  , submitReadOnly
  ) where

import           Control.Concurrent.MVar
import           Control.Monad

import           Data.Kind
import           Data.Proxy

import           Database.LMDB.Raw
import           Database.LMDB.Simple.Internal

type role TransactionHandle nominal

-- | A @'TransactionHandle'@ will keep open an internal LMDB transaction handle.
type TransactionHandle :: Mode -> Type
newtype TransactionHandle mode = TransactionHandle {
    -- Note: if @'thTxn'@ is an empty @'MVar'@, the transaction handle is
    -- considered to be committed. As such, we should not use any blocking
    -- @'MVar'@ operations.
    thTxn  :: MVar MDB_txn
  }

-- | Create a new transaction handle.
--
-- The transaction handle should eventually be committed using @'commit'@ to
-- ensure that the internal transaction handle is freed, though the internal
-- transaction handle will /likely/ be automatically freed once the transaction
-- handle is garbage collected. This is not a strong guarantee, however, so
-- using @'commit'@ is preferable.
_new ::
     forall emode thmode. (IsMode thmode, SubMode emode thmode)
  => Environment emode
  -> IO (TransactionHandle thmode)
_new (Env mdbEnv) = do
    txn <- mdb_txn_begin mdbEnv Nothing (isReadOnlyMode (Proxy @thmode))
    thTxn <- newMVar txn
    void $ mkWeakMVar thTxn $ finalize thTxn
    pure TransactionHandle {thTxn}
  where
    finalize :: MVar MDB_txn -> IO ()
    finalize thTxn = do
      txnMay <- tryTakeMVar thTxn
      case txnMay of
        Nothing  -> pure ()
        Just txn -> mdb_txn_commit txn

-- | Create a new transaction handle in read-only mode.
--
-- See @'new'@ for more info about creating new transaction handles.
newReadOnly :: Environment a -> IO (TransactionHandle ReadOnly)
newReadOnly = _new

-- | Create a new transaction handle in read-write mode.
--
-- See @'new'@ for more info about creating new transaction handles.
_newReadWrite :: Environment ReadWrite -> IO (TransactionHandle ReadWrite)
_newReadWrite = _new

-- | Submit a regular @'Transaction'@ to a transaction handle. Throws an error
-- if the transaction handle was already committed.
_submit ::
     (IsMode tmode, SubMode thmode tmode)
  => TransactionHandle thmode
  -> Transaction tmode a
  -> IO a
_submit TransactionHandle{thTxn} tx@(Txn tf) = do
  txnMay <- tryReadMVar thTxn
  case txnMay of
    Nothing  -> error "submit: TransactionHandle is closed"
    Just txn ->
      if isReadOnlyTransaction tx then
        tf txn
      else
        error "submit: ReadWrite transactions not yet supported"

-- | Submit a regular read-only @'Transaction'@ to a transaction handle.
--
-- See @'submit'@ for more info about submitting regular transactions to
-- transaction handles.
submitReadOnly :: TransactionHandle mode -> Transaction ReadOnly a -> IO a
submitReadOnly = _submit

-- | Submit a regular read-write @'Transaction'@ to a transaction handle.
--
-- See @'submit'@ for more info about submitting regular transactions to
-- transaction handles.
_submitReadWrite :: TransactionHandle ReadWrite -> Transaction ReadWrite a -> IO a
_submitReadWrite = _submit

-- | Commit the transaction handle, discarding the internal transaction handle.
-- Throws an error if the internal transaction handle has already been
-- discarded.
commit :: TransactionHandle mode -> IO ()
commit TransactionHandle{thTxn} = do
  txnMay <- tryTakeMVar thTxn
  case txnMay of
    Nothing  -> error "commit: internal transaction handle aldreay discarded"
    Just txn -> mdb_txn_commit txn

-- | Abort the transaction handle, discarding the internal transaction handle.
-- Throws an error if the internal transaction handle has already been
-- discarded.
abort :: TransactionHandle mode -> IO ()
abort TransactionHandle{thTxn} = do
  txnMay <- tryTakeMVar thTxn
  case txnMay of
    Nothing  -> error "abort: internal transaction handle already discarded"
    Just txn -> mdb_txn_abort txn
