module Test.Database.LMDB.Simple.Cursor where

import Test.Database.LMDB.Simple.Cursor.Lockstep as LS
import Test.Tasty

tests :: TestTree
tests =
  testGroup
    "Cursor"
    [ LS.tests
    ]
