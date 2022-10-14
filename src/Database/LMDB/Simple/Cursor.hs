{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Database.LMDB.Simple.Cursor (
    -- * The Cursor monad
    CursorEnv (..)
  , CursorM (..)
  , runCursorAsTransaction
  , runCursorM
    -- * Monadic operations
  , cget
  , cget_
  , cpokeKey
  , cgetFirst
  , cgetNext
  , cgetSet
  , cgetSetKey
  , cgetSetRange
    -- * Cursor folds
  , Bound (..)
  , FoldRange (..)
  , forEach
  , cfoldM
  , cgetMany
  ) where

import           Control.Monad.Reader
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import           Foreign                       (Ptr, alloca)

import           Database.LMDB.Raw
import           Database.LMDB.Simple.Codec
import           Database.LMDB.Simple.Internal hiding (get, peekVal, put)

{-------------------------------------------------------------------------------
  The Cursor monad
-------------------------------------------------------------------------------}

-- | An environment used in the cursor monad @'CursorM'@.
data CursorEnv k v = CursorEnv {
    -- | A pointer to a lower-level cursor provided by the LMDB Haskell binding.
    cursor :: MDB_cursor'
    -- | A pointer that can hold a key.
  , kPtr   :: Ptr MDB_val
    -- | A pointer that can hold a value.
  , vPtr   :: Ptr MDB_val
    -- | A codec for encoding/decoding keys in CBOR format.
  , kCodec :: Codec k
    -- | A codec for encoding/decoding values in CBOR format.
  , vCodec :: Codec v
  }

-- | The cursor monad is a @'ReaderT'@ transformer on top of @'IO'@.
--
-- We perform lower-level cursor operations in @'IO'@, while the @'ReaderT'@
-- passes along a @'CursorEnv'@ that contains all the information we need to
-- perform the lower-level cursor operations.
newtype CursorM k v a = CursorM {unCursorM :: ReaderT (CursorEnv k v) IO a}
  deriving stock (Functor)
  deriving newtype (Applicative, Monad)

-- | Run a cursor monad in @'IO'@.
runCursorM :: CursorM k v a -> CursorEnv k v -> IO a
runCursorM cm = runReaderT (unCursorM cm)

-- | Run a cursor monad as a @'Transaction'.
runCursorAsTransaction ::
     Codec k            -- ^ A @'Codec'@ for the key type.
  -> Codec v            -- ^ A @'Codec'@ for the value type.
  -> CursorM k v a      -- ^ The cursor monad to run.
  -> Database k v       -- ^ The database to run the cursor monad in.
  -> Transaction mode a
runCursorAsTransaction kcod vcod cm (Db _ dbi) = Txn $ \txn ->
  alloca $ \kptr ->
    alloca $ \vptr ->
      withCursor txn dbi (\c -> runCursorM cm (CursorEnv c kptr vptr kcod vcod))

{-------------------------------------------------------------------------------
  Monadic operations
-------------------------------------------------------------------------------}

-- | Retrieve a key-value pair using a cursor Get operation.
--
-- The different typs of cursor Get operations are listed here:
-- http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
cget :: MDB_cursor_op -> CursorM k v (Maybe (k, v))
cget op = CursorM $ do
  r <- ask
  found <- lift $ mdb_cursor_get' op (cursor r) (kPtr r) (vPtr r)
  if found then do
    k <- lift $ peekVal (kCodec r) (kPtr r)
    v <- lift $ peekVal (vCodec r) (vPtr r)
    pure $ Just (k, v)
  else
    pure Nothing

-- | Like @'cget'@, but throws away the result.
cget_ :: MDB_cursor_op -> CursorM k v ()
cget_ = void . cget

cpokeKey :: k -> CursorM k v ()
cpokeKey k = CursorM $ do
  s <- ask
  lift $ pokeVal (kCodec s) (kPtr s) k

cgetFirst :: CursorM k v (Maybe (k, v))
cgetFirst = cget MDB_FIRST

cgetNext :: CursorM k v (Maybe (k, v))
cgetNext = cget MDB_NEXT

cgetSet :: k -> CursorM k v ()
cgetSet k = cpokeKey k >> cget_ MDB_SET

cgetSetKey :: k -> CursorM k v (Maybe (k, v))
cgetSetKey k = cpokeKey k >> cget MDB_SET_KEY

-- | @'cgetSetRange' k@ positions the cursor at the first key greater than or
-- equal to @k@. Return key + data.
cgetSetRange :: k -> CursorM k v (Maybe (k, v))
cgetSetRange k = cpokeKey k >> cget MDB_SET_RANGE

{-------------------------------------------------------------------------------
  Cursor folds
-------------------------------------------------------------------------------}

-- | Left fold over the full database.
forEach ::
     MDB_cursor_op
  -> MDB_cursor_op
  -> (a -> k -> v -> a)
  -> a
  -> CursorM k v a
forEach first next f z = do
    go first z
  where
    go op acc = do
      x <- cget op
      case x of
        Just (k, v) -> go next (f acc k v)
        Nothing     -> pure acc

data Bound = Exclusive | Inclusive

-- | A @'FoldRange'@ is defined by the inclusive/exclusive lower bound of the
-- range, and the number of keys to fold.
data FoldRange k v = FoldRange {
    lowerBound :: Maybe (k, Bound)
  , keysToFold :: Int
  }

-- | Analogous to @'foldM'@, though it only folds keys in the given
-- @'FoldRange'@.
--
-- The fold starts from the first key in the database if @lbMay == Nothing@. If
-- @lbMay == Just (k, b)@, then the fold starts from the first key greater than
-- or equal to @k@, though @b@ determines whether @k@ can be present in the
-- result or not.
--
-- Note: This is a left fold.
cfoldM ::
     forall b k v. Ord k
  => (b -> k -> v -> CursorM k v b)
  -> b
  -> FoldRange k v
  -> CursorM k v b
cfoldM f z FoldRange{lowerBound = lb, keysToFold = n}
    | n <= 0    = pure z
    | otherwise = do
        kvMay <- cgetInitial
        case kvMay of
          Nothing ->
            pure z
          Just (k, v) -> do
            z' <- f z k v
            foldM f' z' [1..n-1]
  where
    cgetInitial :: CursorM k v (Maybe (k, v))
    cgetInitial = maybe cgetFirst (uncurry cgetSetRange') lb

    -- Like @'cgetSetRange'@, but may skip the first read key if the the lower
    -- bound is exclusive and @k@ is the first key that was read from the
    -- database.
    cgetSetRange' k b = do
      k'vMay <- cgetSetRange k
      case k'vMay of
        Nothing                       -> pure Nothing
        Just (k', v) | k == k'
                     , Exclusive <- b -> cgetNext
                     | otherwise      -> pure $ Just (k', v)

    f' :: b -> Int -> CursorM k v b
    f' acc _ = do
      kvMay <- cgetNext
      case kvMay of
        Nothing     -> pure acc
        Just (k, v) -> f acc k v

-- | @'cgetMany' lbMay n@ reads a range of @n@ key-value pairs given an
-- inclusive/exclusive lower bound on keys to read.
--
-- Uses `cfoldM` to perform the range read using a monadic fold. See @'cfoldM'@
-- and @'FoldRange'@ for specifics on how ranges are folded.
cgetMany :: forall k v. Ord k => Maybe (k, Bound) -> Int -> CursorM k v (Map k v)
cgetMany lbMay n = cfoldM f Map.empty (FoldRange lbMay n)
  where
    f :: Map k v -> k -> v -> CursorM k v (Map k v)
    f m k v = pure $ Map.insert k v m
