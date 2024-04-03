{-# LANGUAGE DataKinds #-}

module Harness
  ( setup
  , tearDown
  ) where

import Database.LMDB.Simple
import System.Directory
import System.IO.Temp

setup :: String -> IO (Environment ReadWrite, Database Int String, FilePath)
setup name = do
  sysTmpDir <- getCanonicalTemporaryDirectory
  tmpDir <- createTempDirectory sysTmpDir name
  env <- openEnvironment tmpDir defaultLimits
         { mapSize      = 1024 * 1024 * 1024
         , maxDatabases = 4
         }
  db <- readOnlyTransaction env (getDatabase Nothing)
  return (env, db, tmpDir)

tearDown :: (Environment ReadWrite, a, FilePath) -> IO ()
tearDown (env, _db, fp) = closeEnvironment env >> removeDirectoryRecursive fp
