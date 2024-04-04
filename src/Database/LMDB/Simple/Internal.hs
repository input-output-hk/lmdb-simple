{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE Rank2Types #-}

module Database.LMDB.Simple.Internal
  ( Mode (..)
  , IsMode (..)
  , SubMode
  , Environment (..)
  , Transaction (..)
  , Database (..)
  , Serialise
  , isReadOnlyEnvironment
  , isReadOnlyTransaction
  , isReadWriteTransaction
  , serialiseBS
  , marshalOut
  , marshalOutBS
  , copyLazyBS
  , marshalIn
  , peekMDBVal
  , pokeMDBVal
  , forEachForward
  , forEachReverse
  , withCursor
  , defaultWriteFlags
  , overwriteFlags
  , get
  , get'
  , getBS
  , getBS'
  , put
  , putBS
  , putNoOverwrite
  , delete
  , deleteBS
  ) where

import Codec.Serialise
  ( Serialise
  , serialise
  , deserialise
  )

import Control.Exception
  ( assert
  , bracket
  )

import Control.Monad
  ( (>=>)
  , foldM
  )

import Control.Monad.IO.Class
  ( MonadIO (liftIO)
  )

import Data.ByteString
  ( packCStringLen
  )
import qualified Data.ByteString as BS

import Data.ByteString.Unsafe
  ( unsafeUseAsCStringLen
  )

import Data.ByteString.Lazy
  ( toChunks
  , toStrict
  , fromStrict
  )
import qualified Data.ByteString.Lazy as BSL

import Data.Kind (Type, Constraint)

import Data.Proxy (Proxy (..))

import Data.Word
  ( Word8
  )

import Database.LMDB.Raw
  ( MDB_env
  , MDB_txn
  , MDB_dbi'
  , MDB_val (MDB_val)
  , MDB_cursor'
  , MDB_cursor_op (MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_PREV)
  , MDB_WriteFlag (MDB_CURRENT, MDB_NOOVERWRITE)
  , MDB_WriteFlags
  , mdb_cursor_open'
  , mdb_cursor_close'
  , mdb_cursor_get'
  , mdb_get'
  , mdb_put'
  , mdb_reserve'
  , mdb_del'
  , compileWriteFlags
  )

import Foreign
  ( Ptr
  , castPtr
  , peek
  , poke
  , plusPtr
  , copyBytes
  )

-- | Modes of operation for 'Environment's and 'Transaction's.
--
-- Note: See the documentation of 'SubMode' and 'Transaction'.
data Mode = ReadWrite | ReadOnly

-- | Reflect (on the term level) whether a @'Mode'@ is read-only or read-write
type IsMode :: Mode -> Constraint
class IsMode mode where
  isReadOnlyMode :: proxy mode -> Bool

instance IsMode ReadWrite where
  isReadOnlyMode _ = False
instance IsMode ReadOnly  where
  isReadOnlyMode _ = True

-- | Both read-only and read-write transactions can run in a read-write
-- environment, but read-only transactions can only run in read-only
-- environments. @mode1@ would be the environment's mode, and @mode2@ would be
-- the transaction's mode.
type SubMode :: Mode -> Mode -> Constraint
type family SubMode mode1 mode2 where
  SubMode mode1 ReadWrite = mode1 ~ ReadWrite
  SubMode mode1 ReadOnly  = ()

-- | An LMDB environment is a directory or file on disk that contains one or
-- more databases, and has an associated (reader) lock table.
type Environment :: Mode -> Type
newtype Environment mode = Env MDB_env

isReadOnlyEnvironment :: forall mode. IsMode mode => Environment mode -> Bool
isReadOnlyEnvironment = isReadOnlyMode

-- | An LMDB transaction is an atomic unit for reading and/or changing one or
-- more LMDB databases within an environment, during which the transaction has
-- a consistent view of the database(s) and is unaffected by any other
-- transaction. The effects of a transaction can either be committed to the
-- LMDB environment atomically, or they can be rolled back with no observable
-- effect on the environment if the transaction is aborted.
--
-- Transactions may be 'ReadWrite' or 'ReadOnly', however LMDB enforces a
-- strict single-writer policy so only one top-level 'ReadWrite' transaction
-- may be active at any time.
--
-- This API models transactions using a 'Transaction' monad. This monad has a
-- 'MonadIO' instance so it is possible to perform arbitrary I/O within a
-- transaction using 'liftIO'. However, such 'IO' actions are not atomic and
-- cannot be rolled back if the transaction is aborted, so use with care.
type Transaction :: Mode -> Type -> Type
newtype Transaction mode a = Txn (MDB_txn -> IO a)

isReadOnlyTransaction :: forall mode a. IsMode mode => Transaction mode a -> Bool
isReadOnlyTransaction _ = isReadOnlyMode (Proxy @mode)

isReadWriteTransaction :: IsMode mode => Transaction mode a -> Bool
isReadWriteTransaction = not . isReadOnlyTransaction

instance Functor (Transaction mode) where
  fmap f (Txn tf) = Txn $ fmap f . tf

instance Applicative (Transaction mode) where
  pure x = Txn $ \_ -> pure x
  Txn tff <*> Txn tf = Txn $ \txn -> tff txn <*> tf txn

instance Monad (Transaction mode) where
  Txn tf >>= f = Txn $ \txn -> tf txn >>= \r -> let Txn tf' = f r in tf' txn

instance MonadIO (Transaction mode) where
  liftIO = Txn . const

-- | A database maps arbitrary keys to values. This API uses the 'Serialise'
-- class to encode and decode keys and values for LMDB to store on disk. For
-- details on creating your own instances of this class, see
-- "Codec.Serialise.Tutorial".
data Database k v = Db MDB_env MDB_dbi'

-- | @'peekMDBVal' ptr@ peeks a value of type @v@ from a pointer @ptr@.
--
-- Use this function in conjunction with @'pokeMDBVal'@ to communicate with LMDB
-- through pointers. This function will marshal in the value of type @v@ that we
-- peek from the pointer.
peekMDBVal :: Serialise v => Ptr MDB_val -> IO v
peekMDBVal = peek >=> marshalIn

-- | @'pokeMDBVal' ptr x@ communicates a value @x@ of type @v@ to LMDB through a
-- pointer @ptr@.
--
-- Use this function in conjunction with @'peekMDBVal'@ to communicate with LMDB
-- through pointers. This function will marshal out the value @x@ that we want
-- to poke the pointer with.
pokeMDBVal :: Serialise v => Ptr MDB_val -> v -> IO ()
pokeMDBVal ptr v = marshalOut v (poke ptr)

serialiseLBS :: Serialise v => v -> BSL.ByteString
serialiseLBS = serialise

serialiseBS :: Serialise v => v -> BS.ByteString
serialiseBS = toStrict . serialiseLBS

marshalIn :: Serialise v => MDB_val -> IO v
marshalIn (MDB_val len ptr) =
  deserialise . fromStrict <$> packCStringLen (castPtr ptr, fromIntegral len)

marshalOut :: Serialise v => v -> (MDB_val -> IO a) -> IO a
marshalOut = marshalOutBS . serialiseBS

marshalOutBS :: BS.ByteString -> (MDB_val -> IO a) -> IO a
marshalOutBS bs f =
  unsafeUseAsCStringLen bs $ \(ptr, len) ->
  f $ MDB_val (fromIntegral len) (castPtr ptr)

copyLazyBS :: BSL.ByteString -> Ptr Word8 -> Int -> IO ()
copyLazyBS lbs ptr rem =
  foldM copyBS (castPtr ptr, rem) (toChunks lbs) >>= \case
      (_, 0) -> return ()
      (_, i) -> error ("Incomplete lazy bytestring copying, remaining bytes: " ++ show i)

  where copyBS :: (Ptr Word8, Int) -> BS.ByteString -> IO (Ptr Word8, Int)
        copyBS (ptr, rem) bs = unsafeUseAsCStringLen bs $ \(bsp, len) ->
          assert (len <= rem) $ copyBytes ptr (castPtr bsp) len >>
          return (ptr `plusPtr` len, rem - len)

forEach :: MDB_cursor_op -> MDB_cursor_op
        -> MDB_txn -> MDB_dbi' -> Ptr MDB_val -> Ptr MDB_val
        -> a -> (IO a -> IO a) -> IO a
forEach first next txn dbi kptr vptr acc f =
  withCursor txn dbi $ cursorGet first acc

  where cursorGet op acc cursor = do
          found <- mdb_cursor_get' op cursor kptr vptr
          if found
            then f (cursorGet next acc cursor)
            else pure acc

forEachForward, forEachReverse :: MDB_txn -> MDB_dbi'
                               -> Ptr MDB_val -> Ptr MDB_val
                               -> a -> (IO a -> IO a) -> IO a
forEachForward = forEach MDB_FIRST MDB_NEXT
forEachReverse = forEach MDB_LAST  MDB_PREV

withCursor :: MDB_txn -> MDB_dbi' -> (MDB_cursor' -> IO a) -> IO a
withCursor txn dbi = bracket (mdb_cursor_open' txn dbi) mdb_cursor_close'

defaultWriteFlags, overwriteFlags :: MDB_WriteFlags
defaultWriteFlags = compileWriteFlags []
overwriteFlags    = compileWriteFlags [MDB_CURRENT]

get :: (Serialise k, Serialise v)
    => Database k v -> k -> Transaction mode (Maybe v)
get db = getBS db . serialiseBS

get' :: Serialise k => Database k v -> k -> Transaction mode (Maybe MDB_val)
get' db = getBS' db . serialiseBS

getBS :: Serialise v
      => Database k v -> BS.ByteString -> Transaction mode (Maybe v)
getBS db keyBS = getBS' db keyBS >>=
  maybe (return Nothing) (liftIO . fmap Just . marshalIn)

getBS' :: Database k v -> BS.ByteString -> Transaction mode (Maybe MDB_val)
getBS' (Db _ dbi) keyBS = Txn $ \txn -> marshalOutBS keyBS $ mdb_get' txn dbi

put :: (Serialise k, Serialise v)
    => Database k v -> k -> v -> Transaction ReadWrite ()
put db = putBS db . serialiseBS

putBS :: Serialise v
     => Database k v -> BS.ByteString -> v -> Transaction ReadWrite ()
putBS (Db _ dbi) keyBS value = Txn $ \txn ->
  marshalOutBS keyBS $ \kval -> do
  let valueLBS = serialiseLBS value
      sz = fromIntegral (BSL.length valueLBS)
  MDB_val len ptr <- mdb_reserve' defaultWriteFlags txn dbi kval sz
  let len' = fromIntegral len
  assert (len' == sz) $ copyLazyBS valueLBS (castPtr ptr) len'

delete :: Serialise k => Database k v -> k -> Transaction ReadWrite Bool
delete db = deleteBS db . serialiseBS

deleteBS :: Database k v -> BS.ByteString -> Transaction ReadWrite Bool
deleteBS (Db _ dbi) key = Txn $ \txn ->
  marshalOutBS key $ \kval -> mdb_del' txn dbi kval Nothing

putNoOverwrite :: (Serialise k, Serialise v)
               => Database k v -> k -> v -> Transaction ReadWrite Bool
putNoOverwrite (Db _ dbi) k v = Txn $ \txn ->
  marshalOut k $ \kval ->
    marshalOut v $ \vval ->
      mdb_put' (compileWriteFlags [MDB_NOOVERWRITE]) txn dbi kval vval
