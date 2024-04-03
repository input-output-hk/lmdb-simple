{-# LANGUAGE TupleSections #-}

module Database.LMDB.Simple.DBRefSpec
  ( spec
  ) where

import Database.LMDB.Simple.DBRef
import Harness
import Test.Hspec

spec :: Spec
spec = beforeAll (setup "DBRefSpec" >>= \(env, db, cleanup) -> (env,,cleanup) <$> newDBRef env db 0) $
       afterAll tearDown $ do

  it "starts empty" $ \(_, ref, _) ->
    readDBRef ref
    `shouldReturn` Nothing

  it "reads what is written" $ \(_, ref, _) ->
    (writeDBRef ref (Just "foo") >> readDBRef ref)
    `shouldReturn` Just "foo"

  it "reads what is modified" $ \(_, ref, _) ->
    (modifyDBRef_ ref (fmap (++ "bar")) >> readDBRef ref)
    `shouldReturn` Just "foobar"

  it "can be emptied with modifyDBRef" $ \(_, ref, _) ->
    (modifyDBRef_ ref (const Nothing) >> readDBRef ref)
    `shouldReturn` Nothing

  it "can be emptied with writeDBRef" $ \(_, ref, _) ->
    (writeDBRef ref (Just "baz") >> writeDBRef ref Nothing >> readDBRef ref)
    `shouldReturn` Nothing
