module Test.Database.LMDB.Simple.Cursor  where

import           Test.Tasty

import           Test.Database.LMDB.Simple.Cursor.Lockstep as LS

tests :: TestTree
tests = testGroup "Cursor" [
    LS.tests
  ]
