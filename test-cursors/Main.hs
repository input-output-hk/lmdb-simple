module Main where

import           Test.Tasty

import qualified Test.Database.LMDB.Simple.Cursor as Test.Cursor

main :: IO ()
main = defaultMain $ testGroup "test-cursors" [
    testGroup "Test" [
        testGroup "Database" [
            testGroup "LMDB" [
                testGroup "Simple" [
                    Test.Cursor.tests
                  ]
              ]
          ]
      ]
  ]
