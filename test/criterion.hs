{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE StandaloneDeriving #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Main where

import Control.DeepSeq
import Criterion.Main
import Database.LMDB.Simple
import Database.LMDB.Simple.Internal (Environment (..), Database (..))
import Database.LMDB.Raw
import GHC.Generics
import Harness

import Control.Monad (forM, forM_)

main :: IO ()
main = defaultMain [
      bench ("insertion of " ++ elements) $
        perRunEnvWithCleanup (setup "criterion-insertion") tearDown $ \ ~(env, db, _) ->
          insertion env db
    , bench ("retrieval of " ++ elements)$
        perRunEnvWithCleanup
          ( setup "criterion-retrieval" >>= \(env, db, fp) ->
            insertion env db >>
            pure (env, db, fp) )
          tearDown $ \ ~(env, db, _) ->
            retrieval env db
    ]

n :: Int
n = 10000

elements :: String
elements = show n ++ " elements"

insertion :: Environment ReadWrite -> Database Int String -> IO ()
insertion env db = transaction env $ do
  clear db
  forM_ [1..n] $ \i -> put db i (Just $ show i)

retrieval :: Environment mode -> Database Int String -> IO [Maybe String]
retrieval env db = readOnlyTransaction env $ forM [1..n] $ \i -> get db i

instance NFData MDB_env where
  rnf x = x `seq` ()

-- | TODO: wrong, but we don't have access to the constructors for @MDB_dbi'@
instance NFData (Database k v) where
  rnf (Db envr dbi) = rnf envr `seq` dbi `seq` ()

deriving stock instance Generic (Environment mode)
deriving anyclass instance NFData (Environment mode)
