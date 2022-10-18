{-# LANGUAGE BangPatterns               #-}
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
  , MDB_cursor_op (..)
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
  , forEachBackward
  , forEachForward
  , cfoldM
  , cgetMany
  ) where

import           Control.Monad.Reader
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import           Foreign                       (Ptr, alloca)

import           Database.LMDB.Raw
import           Database.LMDB.Simple.Codec
import           Database.LMDB.Simple.Internal hiding (forEachForward,
                                                forEachReverse, get, peekVal,
                                                put)

{-------------------------------------------------------------------------------
  The Cursor monad
-------------------------------------------------------------------------------}

-- | An environment used in the cursor monad @'CursorM'@.
--
-- The @'kPtr'@ and @'vPtr'@ pointers are used to communicate keys and values
-- respectively between the Haskell program and the LMDB software. For example,
-- if we read the key-value pair at the current cursor position, then reading
-- what these pointers reference will give us the read key and value. If we
-- write a key-value pair using the cursor, then the LMDB software will read the
-- target key and value from the pointers.
data CursorEnv k v = CursorEnv {
    -- | A pointer to a lower-level cursor provided by the LMDB Haskell binding.
    --
    -- An LMDB cursor points to an entry in the database. We can read/write on
    -- on this cursor, or move the cursor to different entries in the database.
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
  deriving newtype (MonadIO, MonadReader (CursorEnv k v))

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
-- This function will return @'Nothing'@ if what we are trying to read is not
-- found. For example, @'cget' 'MDB_NEXT'@ will return @'Nothing'@ if there is
-- no more key-value pair to read after the current cursor position.
--
-- The different types of cursor Get operations are listed here:
-- http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
cget :: MDB_cursor_op -> CursorM k v (Maybe (k, v))
cget op = do
  r <- ask
  found <- CursorM $ lift $ mdb_cursor_get' op (cursor r) (kPtr r) (vPtr r)
  if found then do
    k <- cpeekKey
    v <- cpeekValue
    pure $ Just (k, v)
  else
    pure Nothing

-- | Like @'cget'@, but throws away the result.
cget_ :: MDB_cursor_op -> CursorM k v ()
cget_ = void . cget

cpeekKey :: CursorM k v k
cpeekKey = do
  r <- ask
  CursorM $ lift $ peekMDBVal (kCodec r) (kPtr r)

cpeekValue :: CursorM k v v
cpeekValue = do
  r <- ask
  CursorM $ lift $ peekMDBVal (vCodec r) (vPtr r)

cpokeKey :: k -> CursorM k v ()
cpokeKey k = CursorM $ do
  s <- ask
  lift $ pokeMDBVal (kCodec s) (kPtr s) k

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

-- | Strict fold over the full database.
forEach ::
     forall a k v.
     MDB_cursor_op
  -> MDB_cursor_op
  -> (a -> k -> v -> a)
  -> a
  -> CursorM k v a
forEach first next f z = do
    go first z
  where
    go :: MDB_cursor_op -> a -> CursorM k v a
    go op acc = do
      x <- cget op
      case x of
        Just (k, v) -> let acc' = f acc k v
                       in  acc' `seq` go next acc'
        Nothing     -> pure acc

-- | Strict left fold over the full database.
forEachForward ::
     (a -> k -> v -> a)
  -> a
  -> CursorM k v a
forEachForward  = forEach MDB_FIRST MDB_NEXT

-- | Strict right fold over the full database.
forEachBackward ::
     (k -> v -> a -> a)
  -> a
  -> CursorM k v a
forEachBackward f = forEach MDB_LAST MDB_PREV f'
  where f' z k v = f k v z

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
-- result or not. Namely, if @b == Exclusive@, then @k@ can not be present in
-- the result because @k@ is the exclusive lower bound for this range read.
-- Conversely, if @b == Inclusive@, then @k@ /may/ be present in the result, but
-- it does not have to be if @k@ is not in the database.
--
-- Note: This is a strict left fold.
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
            !z'  <- f z k v
            !z'' <- foldM f' z' [1..n-1]
            pure z''
  where
    cgetInitial :: CursorM k v (Maybe (k, v))
    cgetInitial = maybe cgetFirst (uncurry cgetSetRange') lb

    -- Like @'cgetSetRange'@, but may skip the first read key if the the lower
    -- bound is exclusive and @k@ is the first key that was read from the
    -- database.
    cgetSetRange' :: k -> Bound -> CursorM k v (Maybe (k, v))
    cgetSetRange' k b = do
      k2vMay <- cgetSetRange k
      case k2vMay of
        Nothing                       -> pure Nothing
        Just (k2, v) | k == k2
                     , Exclusive <- b -> cgetNext
                     | otherwise      -> pure $ Just (k2, v)

    f' :: b -> Int -> CursorM k v b
    f' !acc _ = do
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
