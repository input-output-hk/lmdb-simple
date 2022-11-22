{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}
{-# HLINT ignore "Avoid lambda" #-}

module Bench.Database.LMDB.Simple.Cursor (benchmarks) where

import           Codec.Serialise
import           Control.DeepSeq             (NFData)
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as Map
import           Text.Printf

import           Test.QuickCheck.Monadic
import           Test.Tasty
import           Test.Tasty.Bench
import           Test.Tasty.QuickCheck       hiding (mapSize)

import           Database.LMDB.Simple
import           Database.LMDB.Simple.Cursor

import           Bench.Utils

benchmarks :: Benchmark
benchmarks = bgroup "Cursor" [
    envWithCleanup
        (initBenchEnv
          pKey
          pValue
          (simpleLMDBLimits {mapSize=1024*1024*175})
          (populateDbUnique 1_000_000)
        )
        cleanupBenchEnv $ \bEnv ->
          bgroup "cgetMany" [
              bgroup "benchmarks" [
                  bench_cgetMany bEnv key0     1_000
                , bench_cgetMany bEnv key0    10_000
                , bench_cgetMany bEnv key0   100_000
                , bench_cgetMany bEnv key0 1_000_000
                ]
            , bgroup "tests" [
                  test_cgetMany bEnv key0     1_000
                , test_cgetMany bEnv key0    10_000
                , test_cgetMany bEnv key0   100_000
                , test_cgetMany bEnv key0 1_000_000
                ]
            ]
  ]

-- | Benchmark a @'cgetMany'@ cursor transaction.
bench_cgetMany ::
     (Serialise k, Serialise v, Ord k, NFData k, NFData v)
  => BenchEnv k v -> k -> Int -> Benchmark
bench_cgetMany bEnv k n =
  bench (name_cgetMany bEnv k n) $
    whnfAppIO (\m -> perform_cgetMany bEnv k m) n

-- | Property test a @'cgetMany'@ cursor transaction.
test_cgetMany ::
     (Serialise k, Serialise v, Ord k)
  => BenchEnv k v -> k -> Int -> TestTree
test_cgetMany bEnv k n =
  testProperty (name_cgetMany bEnv k n) $
    once . monadicIO $ do
      m <- run (perform_cgetMany bEnv k n)
      pure $ Map.size m == n

-- | Generate a benchmark name given the inputs to the benchmark.
name_cgetMany :: BenchEnv k v -> k -> Int -> String
name_cgetMany _bEnv _k n = printf "n=%d" n

-- | Run a @'cgetMany'@ cursor transaction.
perform_cgetMany ::
     (Serialise k, Serialise v, Ord k)
  => BenchEnv k v -> k -> Int -> IO (Map k v)
perform_cgetMany BenchEnv{dbEnv, db} k n =
  let cm = runCursorAsTransaction (cgetMany (Just (k, Inclusive)) n) db
  in  readWriteTransaction dbEnv cm
