{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Test.Database.LMDB.Simple.TransactionHandle where

import           System.IO.Temp

import           Test.QuickCheck
import           Test.QuickCheck.Monadic
import           Test.Tasty
import           Test.Tasty.QuickCheck

import           Database.LMDB.Simple
import           Database.LMDB.Simple.TransactionHandle

tests :: TestTree
tests = testGroup "TransactionHandle" [
    testProperty "prop_simpleScenario" prop_simpleScenario
  ]

-- | A simple test for performing reads through a transaction handle.
-- Transaction handles do not see the effects of writes that are comitted after
-- the transaction handle is created.
prop_simpleScenario :: Property
prop_simpleScenario = once $ monadicIO $ run $
  withSystemTempDirectory "prop_simpleScenario" $ \fp -> do
    env <- openEnvironment @ReadWrite fp defaultLimits

    -- Insert dummy entries into the database.
    readWriteTransaction env $ do
      db <- getDatabase @ReadWrite @Int @Int Nothing
      put db 1 (Just 1)
      put db 2 (Just 2)

    th <- newReadOnly env

    -- Delete the dummy entries in the database. The transaction handle should
    -- not see this effect.
    readWriteTransaction env $ do
      db <- getDatabase @ReadWrite @Int @Int Nothing
      put db 1 Nothing
      put db 2 Nothing

    (x, y) <- submitReadOnly th $ do
      db <- getDatabase @ReadOnly @Int @Int Nothing
      x <- get db 2
      y <- get db 1
      pure (x, y)
    commit th

    closeEnvironmentBlocking env

    pure (x == Just 2 && y == Just 1)
