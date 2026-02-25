module Main where

import Bench.Database.LMDB.Simple.Cursor as Cursor
import Test.Tasty.Bench

main :: IO ()
main =
  defaultMain
    [ bgroup
        "bench"
        [ bgroup
            "Bench"
            [ bgroup
                "Database"
                [ bgroup
                    "LMDB"
                    [ bgroup
                        "Simple"
                        [ Cursor.benchmarks
                        ]
                    ]
                ]
            ]
        ]
    ]
