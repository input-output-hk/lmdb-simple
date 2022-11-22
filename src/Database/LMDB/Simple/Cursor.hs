{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Database.LMDB.Simple.Cursor (
    -- * The Cursor monad
    CursorConstraints
  , CursorEnv (..)
  , CursorM (..)
  , runCursorAsTransaction
  , runCursorM
    -- * Peek and poke keys and values
  , cpeekKey
  , cpeekValue
  , cpokeKey
  , cpokeValue
    -- * Basic monadic cursor operations: get
  , MDB_cursor_op (..)
  , cgetG
  , cgetG_
  , cgetFirst
  , cgetCurrent
  , cgetLast
  , cgetNext
  , cgetNextNoDup
  , cgetPrev
  , cgetPrevNoDup
  , cgetSet
  , cgetSetKey
  , cgetSetRange
    -- * Basic monadic cursor operations: put
  , CPutFlag (..)
  , cputG
  , cput
  , cputCurrent
  , cputNoOverwrite
  , cputAppend
    -- * Basic monadic cursor operations: delete
  , CDelFlag (..)
  , cdelG
  , cdel
    -- * Cursor folds
  , Bound (..)
  , FoldRange (..)
  , forEach
  , forEachBackward
  , forEachForward
  , cfoldM
  , cgetAll
  , cgetMany
  ) where

import           Codec.Serialise
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Data.Foldable
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import           Foreign                       (Ptr, alloca, nullPtr)

import           Database.LMDB.Raw
import           Database.LMDB.Simple.Internal hiding (forEachForward,
                                                forEachReverse, get, put)

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
  }

-- | The cursor monad is a @'ReaderT'@ transformer on top of @'IO'@.
--
-- We perform lower-level cursor operations in @'IO'@, while the @'ReaderT'@
-- passes along a @'CursorEnv'@ that contains all the information we need to
-- perform the lower-level cursor operations.
newtype CursorM k v a = CursorM {unCursorM :: ReaderT (CursorEnv k v) IO a}
  deriving stock (Functor)
  deriving newtype (Applicative, Monad)
  deriving newtype (MonadIO, MonadReader (CursorEnv k v), MonadThrow, MonadCatch)

-- | Run a cursor monad in @'IO'@.
runCursorM :: CursorM k v a -> CursorEnv k v -> IO a
runCursorM cm = runReaderT (unCursorM cm)

-- | Run a cursor monad as a @'Transaction'.
runCursorAsTransaction ::
     (Serialise k, Serialise v)
  => CursorM k v a      -- ^ The cursor monad to run.
  -> Database k v       -- ^ The database to run the cursor monad in.
  -> Transaction mode a
runCursorAsTransaction cm (Db _ dbi) = Txn $ \txn ->
  alloca $ \kptr ->
    alloca $ \vptr ->
      withCursor txn dbi (\c -> runCursorM cm (CursorEnv c kptr vptr))

type CursorConstraints m k v = (
    MonadIO m
  , MonadReader (CursorEnv k v) m
  , Serialise k
  , Serialise v
  )

{-------------------------------------------------------------------------------
  Peek and poke keys and values
-------------------------------------------------------------------------------}

-- | Read the key at the key pointer.
cpeekKey :: CursorConstraints m k v => m k
cpeekKey = do
  r <- ask
  liftIO $ peekVal (kPtr r)

-- | Read the value at the value pointer.
cpeekValue :: CursorConstraints m k v => m v
cpeekValue = do
  r <- ask
  liftIO $ peekVal (vPtr r)

-- | Write a key at the key pointer.
cpokeKey :: CursorConstraints m k v => k -> m ()
cpokeKey k = do
  s <- ask
  liftIO $ pokeMDBVal (kPtr s) k

-- | Write a value at the value pointer.
cpokeValue :: CursorConstraints m k v => v -> m ()
cpokeValue v = do
  s <- ask
  liftIO $ pokeMDBVal (vPtr s) v

{-------------------------------------------------------------------------------
  Basic monadic cursor operations: get
-------------------------------------------------------------------------------}

errCursorOpNotSupported :: MDB_cursor_op -> a
errCursorOpNotSupported op =
  error $ "MDB_cursor_op not yet supported for cursor get operations: "
      <> show op

-- | General-purpose cursor getfunction.
--
-- This function will return @'Nothing'@ if what we are trying to read is not
-- found. For example, @'cget' 'MDB_NEXT'@ will return @'Nothing'@ if there is
-- no more key-value pair to read after the current cursor position.
--
-- Note: not all cursor ops are supported, so use dedicated functions like
-- @'cgetFirst'@, @'cgetNext'@ and @'cgetPrev'@ instead.
--
-- Note: functions like @'cgetSet'@, @'cgetSetKey'@ and @'cgetSetRange'@ require
-- a valid key to be set in @'kPtr'@. Using @'cgetG'@ with @'MDB_SET'@,
-- @'MDB_SET_KEY'@ or @'MDB_SET_RANGE'@ without using @'cpokeKey'@ first will
-- therefore likely result in an exception.
--
-- http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
-- http://www.lmdb.tech/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0
cgetG :: CursorConstraints m k v => MDB_cursor_op -> m (Maybe (k, v))
cgetG op = do
  r <- ask
  found <- liftIO $ mdb_cursor_get' op (cursor r) (kPtr r) (vPtr r)
  if found then do
    k <- cpeekKey
    v <- cpeekValue
    pure $ Just (k, v)
  else
    pure Nothing

-- | Like @'cgetG'@, but throws away the result.
cgetG_ :: CursorConstraints m k v => MDB_cursor_op -> m ()
cgetG_ = void . cgetG

cgetFirst :: CursorConstraints m k v => m (Maybe (k, v))
cgetFirst = cgetG MDB_FIRST

-- TODO(jdral): sorted duplicates not supported yet.
_cgetFirstDup :: CursorConstraints m k v => m (Maybe (k, v))
_cgetFirstDup  = errCursorOpNotSupported MDB_FIRST_DUP

-- TODO(jdral): sorted duplicates not supported yet.
_cgetBoth :: CursorConstraints m k v => m ()
_cgetBoth = errCursorOpNotSupported MDB_GET_BOTH

-- TODO(jdral): sorted duplicates not supported yet.
_cgetBothRange :: CursorConstraints m k v => m ()
_cgetBothRange = errCursorOpNotSupported MDB_GET_BOTH_RANGE

cgetCurrent :: CursorConstraints m k v => m (Maybe (k, v))
cgetCurrent = cgetG MDB_GET_CURRENT

-- TODO(jdral): fixed-size, sorted duplicates not supported yet.
_cgetMultiple :: CursorConstraints m k v => m ()
_cgetMultiple  = errCursorOpNotSupported MDB_GET_MULTIPLE

cgetLast :: CursorConstraints m k v => m (Maybe (k, v))
cgetLast = cgetG MDB_LAST

-- TODO(jdral): sorted duplicates not supported yet.
_cgetLastDup :: CursorConstraints m k v => m ()
_cgetLastDup = errCursorOpNotSupported MDB_LAST_DUP

cgetNext :: CursorConstraints m k v => m (Maybe (k, v))
cgetNext = cgetG MDB_NEXT

-- TODO(jdral): sorted duplicates not supported yet.
_cgetNextDup :: CursorConstraints m k v => m ()
_cgetNextDup = errCursorOpNotSupported MDB_NEXT_DUP

-- TODO(jdral): fixed-size, sorted duplicates not supported yet.
_cgetNextMultiple :: CursorConstraints m k v => m ()
_cgetNextMultiple = errCursorOpNotSupported MDB_NEXT_MULTIPLE

cgetNextNoDup :: CursorConstraints m k v => m (Maybe (k, v))
cgetNextNoDup = cgetG MDB_NEXT_NODUP

cgetPrev :: CursorConstraints m k v => m (Maybe (k, v))
cgetPrev = cgetG MDB_PREV

-- TODO(jdral): sorted duplicates not supported yet.
_cgetPrevDup :: CursorConstraints m k v => m ()
_cgetPrevDup = errCursorOpNotSupported MDB_PREV_DUP

cgetPrevNoDup :: CursorConstraints m k v => m (Maybe (k, v))
cgetPrevNoDup = cgetG MDB_PREV_NODUP

cgetSet :: CursorConstraints m k v => k -> m ()
cgetSet k = cpokeKey k >> cgetG_ MDB_SET

cgetSetKey :: CursorConstraints m k v => k -> m (Maybe (k, v))
cgetSetKey k = cpokeKey k >> cgetG MDB_SET_KEY

-- | @'cgetSetRange' k@ positions the cursor at the first key greater than or
-- equal to @k@. Return key + data.
cgetSetRange :: CursorConstraints m k v => k -> m (Maybe (k, v))
cgetSetRange k = cpokeKey k >> cgetG MDB_SET_RANGE

{-------------------------------------------------------------------------------
  Basic monadic cursor operations: put
-------------------------------------------------------------------------------}

errPutFlagNotSupported :: CPutFlag -> a
errPutFlagNotSupported pf =
  error $ "CPutFlag not yet supported for cursor put operations: "
       <> show pf

-- | Flags that control the behaviour of cursor put operations.
data CPutFlag =
    CPF_MDB_CURRENT
  | CPF_MDB_NODUPDATA
  | CPF_MDB_NOOVERWRITE
  | CPF_MDB_RESERVE
  | CPF_MDB_APPEND
  | CPF_MDB_APPENDDUP
  | CPF_MDB_MULTIPLE
  deriving (Show, Eq, Ord)

fromCPutFlag :: CPutFlag -> MDB_WriteFlag
fromCPutFlag = \case
  CPF_MDB_CURRENT     -> MDB_CURRENT
  CPF_MDB_NODUPDATA   -> errPutFlagNotSupported CPF_MDB_NODUPDATA
  CPF_MDB_NOOVERWRITE -> MDB_NOOVERWRITE
  CPF_MDB_RESERVE     -> errPutFlagNotSupported CPF_MDB_RESERVE
  CPF_MDB_APPEND      -> MDB_APPEND
  CPF_MDB_APPENDDUP   -> errPutFlagNotSupported CPF_MDB_APPENDDUP
  CPF_MDB_MULTIPLE    -> errPutFlagNotSupported CPF_MDB_MULTIPLE

compileCPutFlag :: Maybe CPutFlag -> MDB_WriteFlags
compileCPutFlag = compileWriteFlags . toList . fmap fromCPutFlag

-- | General-purpose cursor put function.
--
-- Note: not all flags are supported, so use dedicated functions like @'cput'@,
-- @'cputCurrent'@ and @'cputNoOverwrite'@ instead.
--
-- http://www.lmdb.tech/doc/group__mdb.html#ga1f83ccb40011837ff37cc32be01ad91e
cputG :: CursorConstraints m k v => Maybe CPutFlag -> k -> v -> m Bool
cputG flag k v = do
  r <- ask
  cpokeKey k
  cpokeValue v
  liftIO $
    mdb_cursor_put_ptr' (compileCPutFlag flag) (cursor r) (kPtr r) (vPtr r)

cput :: CursorConstraints m k v => k -> v -> m Bool
cput = cputG   Nothing

cputCurrent :: CursorConstraints m k v => k -> v -> m Bool
cputCurrent = cputG $ Just CPF_MDB_CURRENT

-- TODO(jdral): sorted duplicates not supported yet.
_cputNoDupData :: CursorConstraints m k v => m ()
_cputNoDupData = errPutFlagNotSupported CPF_MDB_NODUPDATA

cputNoOverwrite :: CursorConstraints m k v => k -> v -> m Bool
cputNoOverwrite = cputG $ Just CPF_MDB_NOOVERWRITE

-- TODO(jdral): not yet supported, needs special handling.
_cputReserve :: CursorConstraints m k v => m ()
_cputReserve = errPutFlagNotSupported CPF_MDB_RESERVE

cputAppend :: CursorConstraints m k v => k -> v -> m Bool
cputAppend = cputG $ Just CPF_MDB_APPEND

-- TODO(jdral): sorted duplicates not supported yet.
_cputAppendDup :: CursorConstraints m k v => m ()
_cputAppendDup = errPutFlagNotSupported CPF_MDB_APPENDDUP

-- TODO(jdral): fixed-size, sorted duplicates not supported yet.
_cputMultiple :: CursorConstraints m k v => m ()
_cputMultiple = errPutFlagNotSupported CPF_MDB_MULTIPLE

{-------------------------------------------------------------------------------
  Basic monadic cursor operations: delete
-------------------------------------------------------------------------------}

errDelFlagNotSupported :: CDelFlag -> a
errDelFlagNotSupported df =
  error $ "CDelFlag not yet supported for cursor put operations: "
       <> show df

-- | Flags that control the behaviour of cursor delete operations.
data CDelFlag = CDF_MDB_NODUPDATA
  deriving (Show, Eq, Ord)

fromCDelFlag :: CDelFlag -> MDB_WriteFlag
fromCDelFlag = \case
  CDF_MDB_NODUPDATA -> errDelFlagNotSupported CDF_MDB_NODUPDATA

compileCDelFlag :: Maybe CDelFlag -> MDB_WriteFlags
compileCDelFlag = compileWriteFlags . toList . fmap fromCDelFlag

-- General-purpose cursor delete function.
--
-- Note: not all flags are supported, so use dedicated functions like @'cdel''@
-- instead.
--
-- http://www.lmdb.tech/doc/group__mdb.html#ga26a52d3efcfd72e5bf6bd6960bf75f95
cdelG :: CursorConstraints m k v => Maybe CDelFlag -> m ()
cdelG flag = do
  r <- ask
  if kPtr r == nullPtr then error "e" else pure ()
  liftIO $ mdb_cursor_del' (compileCDelFlag flag) (cursor r)

cdel :: CursorConstraints m k v => m ()
cdel = cdelG Nothing

-- TODO(jdral): sorted duplicates not supported yet.
_cdelNoDupData :: CursorConstraints m k v => m ()
_cdelNoDupData = errDelFlagNotSupported CDF_MDB_NODUPDATA

{-------------------------------------------------------------------------------
  Cursor folds
-------------------------------------------------------------------------------}

-- | Strict fold over the full database.
forEach ::
     forall m a k v.
     CursorConstraints m k v
  => MDB_cursor_op
  -> MDB_cursor_op
  -> (a -> k -> v -> a)
  -> a
  -> m a
forEach first next f z = do
    go first z
  where
    go :: MDB_cursor_op -> a -> m a
    go op acc = do
      x <- cgetG op
      case x of
        Just (k, v) -> let acc' = f acc k v
                       in  acc' `seq` go next acc'
        Nothing     -> pure acc

-- | Strict left fold over the full database.
forEachForward ::
     CursorConstraints m k v
  => (a -> k -> v -> a)
  -> a
  -> m a
forEachForward  = forEach MDB_FIRST MDB_NEXT

-- | Strict right fold over the full database.
forEachBackward ::
     CursorConstraints m k v
  => (k -> v -> a -> a)
  -> a
  -> m a
forEachBackward f = forEach MDB_LAST MDB_PREV f'
  where f' z k v = f k v z

cgetAll ::
     (CursorConstraints m k v, Ord k)
  => m (Map k v)
cgetAll = forEachForward (\acc k v -> Map.insert k v acc) mempty

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
     forall m b k v.
     (CursorConstraints m k v, Ord k)
  => (b -> k -> v -> m b)
  -> b
  -> FoldRange k v
  -> m b
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
    cgetInitial :: m (Maybe (k, v))
    cgetInitial = maybe cgetFirst (uncurry cgetSetRange') lb

    -- Like @'cgetSetRange'@, but may skip the first read key if the the lower
    -- bound is exclusive and @k@ is the first key that was read from the
    -- database.
    cgetSetRange' :: k -> Bound -> m (Maybe (k, v))
    cgetSetRange' k b = do
      k2vMay <- cgetSetRange k
      case k2vMay of
        Nothing                       -> pure Nothing
        Just (k2, v) | k == k2
                     , Exclusive <- b -> cgetNext
                     | otherwise      -> pure $ Just (k2, v)

    f' :: b -> Int -> m b
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
cgetMany ::
     forall m k v.
     (CursorConstraints m k v, Ord k)
  => Maybe (k, Bound)
  -> Int
  -> m (Map k v)
cgetMany lbMay n = cfoldM f Map.empty (FoldRange lbMay n)
  where
    f :: Map k v -> k -> v -> m (Map k v)
    f m k v = pure $ Map.insert k v m
