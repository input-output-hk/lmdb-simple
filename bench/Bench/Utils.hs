{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Bench.Utils (
    -- * Environment types
    BenchEnv (..)
    -- * Initialisation and cleanup for environments
  , initBenchEnv
  , cleanupBenchEnv
    -- * Defaults for LMDB Limits
  , simpleLMDBLimits
    -- * Defaults for database population
  , noPopulateDb
  , populateDbUnique
    -- * Default types for keys and values
  , Key (..)
  , Value (..)
  , keySize
  , key0
  , pKey
  , pValue
  , valueSize
  ) where

import           Codec.Serialise
import           Control.DeepSeq
import           Control.Monad
import           Control.Monad.IO.Class
import qualified Data.ByteString.Short         as B (ShortByteString, pack)
import           Data.Proxy
import           Data.Word
import           GHC.Generics                  (Generic)
import           System.Directory
import           System.IO.Temp

import           Test.Tasty.QuickCheck

import           Database.LMDB.Simple
import qualified Database.LMDB.Simple.Internal as Internal (putNoOverwrite)

{-------------------------------------------------------------------------------
  Environment types
-------------------------------------------------------------------------------}

data BenchEnv k v = BenchEnv {
    dbFilePath :: FilePath
  , dbEnv      :: Environment ReadWrite
  , db         :: Database k v
  }

-- TODO: The @'dbEnv'@ and @'db'@ fields of @'BenchEnv'@ consist mainly of
-- pointers, so we can omit them here because they will be likely not to
-- contribute much to evaluation time. If we want to evaluate these fields
-- instead, we should change the lower-level Haskell bindings to expose more
-- constructors.
instance NFData (BenchEnv k v) where
  rnf :: BenchEnv k v -> ()
  rnf BenchEnv{dbFilePath} = rnf dbFilePath

{-------------------------------------------------------------------------------
  Initialisation and cleanup for environments
-------------------------------------------------------------------------------}

initBenchEnv ::
     (MonadIO m, Serialise k, Serialise v, Arbitrary k, Arbitrary v)
  => Proxy k
  -> Proxy v
  -> Limits
     -- ^ LMDB limits such as maximum database size.
  -> (Database k v -> Transaction ReadWrite ())
     -- ^ Action that populate the database
  -> m (BenchEnv k v)
initBenchEnv _ _ limits populate = do
  dbFilePath <- createDbDir
  dbEnv <- liftIO $ openEnvironment dbFilePath limits
  db <- liftIO $ readWriteTransaction dbEnv (getDatabase Nothing)
  void $ liftIO $ readWriteTransaction dbEnv $ populate db
  pure BenchEnv {dbFilePath, dbEnv, db}

createDbDir :: MonadIO m => m FilePath
createDbDir = do
  sysTmpDir <- liftIO getTemporaryDirectory
  liftIO $ createTempDirectory sysTmpDir "bench_cursor"

cleanupBenchEnv :: MonadIO m => BenchEnv k v -> m ()
cleanupBenchEnv tenv = do
  liftIO $ closeEnvironmentBlocking (dbEnv tenv)
  deleteDbDir (dbFilePath tenv)

deleteDbDir :: MonadIO m => FilePath -> m ()
deleteDbDir = liftIO . removeDirectoryRecursive

{-------------------------------------------------------------------------------
  Defaults for LMDB limits
-------------------------------------------------------------------------------}

simpleLMDBLimits :: Limits
simpleLMDBLimits = defaultLimits

{-------------------------------------------------------------------------------
  Defaults for database population
-------------------------------------------------------------------------------}

-- | @'noPopulateDb' db@ does not populate the LMDB database @db@ with any
-- entries.
noPopulateDb :: Database k v -> Transaction ReadWrite ()
noPopulateDb _ = pure ()

-- | @'populateDbUnique' n db@ populates the LMDB database @db@ with @n@ unique
-- key-value pairs.
--
-- Note: ensure that @n@ is much smaller than the range of values that are
-- generated for @'Arbitrary' k@, or this function may not terminate within
-- reasonable time, let alone terminate at all. We use a simple "retry if a
-- generated key is already present in the database (i.e., a /clash/)" strategy
-- to generate exactly @n@ unique keys, which means that clashes occur
-- inhibitively often as @n@ approaches the size of the of the range of @k@.
--
-- TODO: As a possible improvement to ensure termination, the function could
-- keep track of the smallest key that is not yet in the DB, such that we can
-- use that key if a clash occurs. This w
populateDbUnique ::
     ( Serialise k, Serialise v
     , Arbitrary k, Arbitrary v
     , Ord k
     )
  => Int
  -> Database k v
  -> Transaction ReadWrite ()
populateDbUnique n db
  | n <= 0           = pure ()
  | otherwise        = do
      k <- liftIO $ generate arbitrary
      v <- liftIO $ generate arbitrary
      b <- Internal.putNoOverwrite db k v
      if b then
        populateDbUnique (n - 1) db
      else
        populateDbUnique n db

{-------------------------------------------------------------------------------
  Default types for keys and values
-------------------------------------------------------------------------------}

newtype Key = Key B.ShortByteString
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (NFData, Serialise)

-- | Size of a @'Key'@ in number of bytes.
keySize :: Int
keySize = 32

key0 :: Key
key0 = Key . B.pack $ replicate keySize 0

newtype Value = Value B.ShortByteString
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (NFData, Serialise)

-- | Size of a @'Value'@ in number of bytes.
valueSize :: Int
valueSize = 64

-- | A value of type @'Key'@ is chosen uniformly from the entire range of the
-- type @'Key'@: any possible @'Word8'@ vector of length @'keySize'@.
instance Arbitrary Key where
  arbitrary = Key . B.pack <$> replicateM keySize uniformWord8

-- | A value of type @'Value'@ is chosen uniformly from the entire range of the
-- type @'Value'@: any possible @'Word8'@ vector of length @'valueSize'@.
instance Arbitrary Value where
  arbitrary = Value . B.pack <$> replicateM valueSize uniformWord8

-- | Generate a uniformly random @'Word8'@.
--
-- @'arbitraryBoundedIntegral'@ ensures that we pick uniformly random @'Word8'@
-- values. The @'arbitrary'@ instance for @'Word8'@ uses
-- @'arbitrarySizedBoundedIntegral'@, which also picks from the entire range of
-- the @'Word8'@ type, but smaller values are more likely to picked.
uniformWord8 :: Gen Word8
uniformWord8 = arbitraryBoundedIntegral

pKey :: Proxy Key
pKey = Proxy

pValue :: Proxy Value
pValue = Proxy
