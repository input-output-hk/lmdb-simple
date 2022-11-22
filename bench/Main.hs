module Main where

import           Test.Tasty.Bench

import           Bench.Database.LMDB.Simple.Cursor as Cursor

main :: IO ()
main = defaultMain [
    bgroup "bench" [
        bgroup "Bench" [
            bgroup "Database" [
                bgroup "LMDB" [
                    bgroup "Simple" [
                        Cursor.benchmarks
                      ]
                  ]
              ]
          ]
      ]
  ]
