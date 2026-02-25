module Main where

import qualified Test.Database.LMDB.Simple.Cursor as Test.Cursor
import qualified Test.Database.LMDB.Simple.TransactionHandle as Test.TransactionHandle
import Test.Tasty

main :: IO ()
main =
  defaultMain $
    testGroup
      "test-cursors"
      [ testGroup
          "Test"
          [ testGroup
              "Database"
              [ testGroup
                  "LMDB"
                  [ testGroup
                      "Simple"
                      [ Test.Cursor.tests
                      , Test.TransactionHandle.tests
                      ]
                  ]
              ]
          ]
      ]
