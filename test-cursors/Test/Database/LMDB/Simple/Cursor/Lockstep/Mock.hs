{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE Rank2Types                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}

module Test.Database.LMDB.Simple.Cursor.Lockstep.Mock (
    -- * Types
    Mock (..)
  , emptyMock
  , COp (..)
  , Err (..)
    -- * Mocked cursor monad
  , MC (..)
  , runMC
  , unsafeEvalMC
  , MCC
    -- * Mock cursor operations: get
  , mCursorGet
  , mCursorGetSet
  , mCursorGetSetKey
  , mCursorGetSetRange
    -- * Mock cursor operations: put
  , mCursorPut
    -- * Mock cursor operations: delete
  , mCursorDel
    -- * Utility: viewing cursor positions
  , CPosView (..)
  , viewCPos
    -- * Convert observable errors to mock errors
  , fromLMDBError
  ) where

import           Control.Monad
import           Control.Monad.Error.Class
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.State.Class
import           Control.Monad.State.Strict
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as Map

import           Database.LMDB.Raw
import           Database.LMDB.Simple.Cursor

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

data Mock k v = Mock {
    -- | The key-value pairs that are stored in the database.
    store :: Map k v
    -- | Position of the cursor in the key-value store. @'Nothing'@ signals that
    -- the cursor position is a null pointer.
  , cpos  :: Maybe k
  }
  deriving (Show, Eq)

emptyMock :: Mock k v
emptyMock = Mock {
    store = Map.empty
  , cpos  = Nothing
  }

-- | Simple enumeration type for the three types of cursor oprations: get, put
-- and delete.
data COp = Get | Put | Del
  deriving (Show, Eq)

data Err =
    -- | An @'MDB_cursor_op'@ is not supported for a get operation.
    ErrCursorOpNotSupported MDB_cursor_op
    -- | A @'CPutFlag'@ is not supported for a put operation.
  | ErrPutFlagNotSupported CPutFlag
    -- | A @'CDelFlag'@ is not supported for a delete operation.
  | ErrDelFlagNotSupported CDelFlag
    -- | A cursor operation got an invalid argument, like a cursor position that
    -- is a null pointer.
  | ErrInvalidArgument COp
    -- | A specific key-value pair was not found in the database.
    --
    -- Example: deletes will delete the first key-value pair in a cursor
    -- position that is greater than or equal to the current cursor position.
    -- This means that if the cursor points to a key-value pair, then it will
    -- delete it. If it does not point to a key-value pair, then we delete the
    -- closest, successor key-value pair. However, if there no more successors,
    -- then this error is thrown.
  | ErrNotFound COp

  deriving (Show, Eq)

{-------------------------------------------------------------------------------
  Mocked cursor monad
-------------------------------------------------------------------------------}

-- | Mocked cursor monad.
newtype MC k v a = MC {unMC :: ExceptT Err (StateT (Mock k v) Identity) a}
  deriving stock Functor
  deriving newtype (Applicative, Monad, MonadState (Mock k v), MonadError Err)

runMC :: MC k v a -> Mock k v -> (Either Err a, Mock k v)
runMC = runState . runExceptT . unMC

-- | Unsafely evaluate a mocked cursor monad.
--
-- Will err if the mocked cursor monad throws an error.
unsafeEvalMC :: MC k v a -> Mock k v -> a
unsafeEvalMC mc m = unsafeFromRight $ fst $ runMC mc m
  where
    unsafeFromRight :: Either a b -> b
    unsafeFromRight (Right x) = x
    unsafeFromRight (Left _)  = error "unsafeFromRight"

-- | Common constraints for mocked cursor operations.
type MCC k v m = (
    MonadState (Mock k v) m
  , MonadError Err m
  , Ord k
  )

{-------------------------------------------------------------------------------
  Mock cursor operations: get
-------------------------------------------------------------------------------}

-- | Cursor get operation.
--
-- Delegates to specialised cursor get functions based on the @'MDB_cursor_op'@
-- argument.
mCursorGet :: MCC k v m => MDB_cursor_op -> m (Maybe (k, v))
mCursorGet op = case op of
  MDB_FIRST       -> mCursorGetFirst
  MDB_GET_CURRENT -> mCursorGetCurrent
  MDB_LAST        -> mCursorGetLast
  MDB_NEXT        -> mCursorGetNext
  MDB_NEXT_NODUP  -> mCursorGetNextNoDup
  MDB_PREV        -> mCursorGetPrev
  MDB_PREV_NODUP  -> mCursorGetPrevNoDup
  _               -> throwError $ ErrCursorOpNotSupported op

mCursorGetFirst :: MCC k v m => m (Maybe (k, v))
mCursorGetFirst = do
  Mock{store} <- get
  case Map.lookupMin store of
    Nothing     -> pure Nothing
    Just (k, v) -> moveCursor k >> pure (Just (k, v))

mCursorGetCurrent :: MCC k v m => m (Maybe (k, v))
mCursorGetCurrent = do
  Mock{store} <- get
  when (Map.null store) $
    throwError $ ErrInvalidArgument Get
  view <- viewCPos
  case view of
    NullP       -> throwError $ ErrInvalidArgument Get
    EmptyP k    -> case Map.lookupGT k store of
      Nothing       -> pure Nothing
      Just (k2, v2) -> moveCursor k2 >> pure (Just (k2, v2))
    FilledP k v -> pure (Just (k, v))

mCursorGetLast :: MCC k v m => m (Maybe (k, v))
mCursorGetLast = do
  Mock{store} <- get
  case Map.lookupMax store of
    Nothing     -> pure Nothing
    Just (k, v) -> moveCursor k >> pure (Just (k, v))

mCursorGetNext :: MCC k v m => m (Maybe (k, v))
mCursorGetNext = getWithLookup Map.lookupGT

mCursorGetNextNoDup :: MCC k v m => m (Maybe (k, v))
mCursorGetNextNoDup = getWithLookup Map.lookupGT

mCursorGetPrev :: MCC k v m => m (Maybe (k, v))
mCursorGetPrev = getWithLookup Map.lookupLT

mCursorGetPrevNoDup :: MCC k v m => m (Maybe (k, v))
mCursorGetPrevNoDup = getWithLookup Map.lookupLT

mCursorGetSet :: MCC k v m => k -> m ()
mCursorGetSet = moveCursor

mCursorGetSetKey :: MCC k v m => k -> m (Maybe (k, v))
mCursorGetSetKey k = do
  moveCursor k
  getWithLookup (\k0 m -> (k,) <$> Map.lookup k0 m)

mCursorGetSetRange :: MCC k v m => k -> m (Maybe (k, v))
mCursorGetSetRange k = do
  moveCursor k
  getWithLookup Map.lookupGE

{-------------------------------------------------------------------------------
  Mock cursor operations: put
-------------------------------------------------------------------------------}

-- | Cursor put operation.
--
-- Delegates to specialised cursor put functions based on the @'Maybe'
-- 'CPutFlag'@ argument.
mCursorPut :: MCC k v m => Maybe CPutFlag -> k -> v -> m Bool
mCursorPut pfMay = case pfMay of
  Nothing -> \k v -> insertAndMoveCursor k v >> pure True
  Just pf -> case pf of
    CPF_MDB_CURRENT     -> mCursorPutCurrent
    CPF_MDB_NOOVERWRITE -> mCursorPutNoOverwrite
    CPF_MDB_APPEND      -> mCursorPutAppend
    _otherwise          -> \_k _v -> throwError $ ErrPutFlagNotSupported pf

mCursorPutCurrent :: MCC k v m => k -> v -> m Bool
mCursorPutCurrent k v = do
  Mock{store} <- get
  when (Map.null store) $
    throwError $ ErrInvalidArgument Put
  view <- viewCPos
  case view of
    NullP           -> throwError $ ErrInvalidArgument Put
    EmptyP _k2      -> error "Undefined behaviour"
    FilledP _k2 _v2 -> insertAndMoveCursor k v >> pure True

mCursorPutNoOverwrite :: MCC k v m => k -> v -> m Bool
mCursorPutNoOverwrite k v = do
  Mock{store} <- get
  case Map.lookup k store of
    Just _  -> moveCursor k >> pure False
    Nothing -> insertAndMoveCursor k v >> pure True

mCursorPutAppend :: MCC k v m => k -> v -> m Bool
mCursorPutAppend k v = do
  Mock{store} <- get
  case Map.lookupMax store of
    Nothing       -> insertAndMoveCursor k v >> pure True
    Just (k2, _)
      | k2 < k    -> insertAndMoveCursor k v >> pure True
      | otherwise -> moveCursor k2 >> pure False

{-------------------------------------------------------------------------------
  Mock cursor operations: delete
-------------------------------------------------------------------------------}

-- | Cursor delete operation.
mCursorDel :: MCC k v m => Maybe CDelFlag -> m ()
mCursorDel dfMay = do
  Mock{store} <- get
  when (Map.null store) $
    throwError $ ErrInvalidArgument Del
  case dfMay of
    Nothing -> do
      view <- viewCPos
      case view of
        NullP       -> throwError $ ErrInvalidArgument Del
        EmptyP k    -> deleteGT k >>=
                       \b -> unless b (throwError $ ErrNotFound Del)
        FilledP k _ -> delete k
    Just df -> throwError $ ErrDelFlagNotSupported df

{-------------------------------------------------------------------------------
  Utility
-------------------------------------------------------------------------------}

-- | Given a lookup function, get a key-value pair based on the current cursor
-- position.
getWithLookup ::
     forall k v m. MCC k v m
  => (k -> Map k v -> Maybe (k, v))
  -> m (Maybe (k, v))
getWithLookup lookUp = do
    view <- viewCPos
    case view of
      NullP        -> pure Nothing
      EmptyP k     -> f k
      FilledP k _v -> f k
  where
    f :: k -> m (Maybe (k, v))
    f k = do
      store <- gets store
      case lookUp k store of
        Nothing       -> pure Nothing
        Just (k2, v2) -> moveCursor k2 >> pure (Just (k2, v2))

-- | Move the cursor position to a specific key.
moveCursor :: MCC k v m => k -> m ()
moveCursor k = modify' (\m -> m {cpos = Just k})

-- | Insert a key-value pair in the store and move the cursor position to that
-- key.
insertAndMoveCursor :: MCC k v m => k -> v -> m ()
insertAndMoveCursor k v = do
  insert k v
  moveCursor k

-- | Insert a key-value pair in the store.
insert :: MCC k v m => k -> v -> m ()
insert k v = modify' (\m@Mock{store} -> m { store = Map.insert k v store })

-- | Delete a specific key from the store.
delete :: MCC k v m => k -> m ()
delete k = modify' (\m -> m { store = Map.delete k (store m) })

-- | Delete the first key greater than the specified key from the store.
deleteGT :: MCC k v m => k -> m Bool
deleteGT k = do
  Mock{store} <- get
  case Map.lookupGT k store of
    Nothing      -> pure False
    Just (k2, _) -> delete k2 >> pure True

{-------------------------------------------------------------------------------
  Utility: viewing cursor positions
-------------------------------------------------------------------------------}

-- | A view of a cursor position that also details information about the key
-- that a cursor position points to.
data CPosView k v =
    -- | The cursor position is a null pointer. There is no key it points to.
    NullP
    -- | The cursor position points to a key that is empty. We only know the key
    -- that the cursor position points to.
  | EmptyP k
    -- | The cursor position points to a key that is filled. We know the key and
    -- the value that the cursor position points to.
  | FilledP k v
  deriving (Show, Eq)

-- | Create a @'CPosView'@ based on the current cursor position.
viewCPos :: MCC k v m => m (CPosView k v)
viewCPos = do
  m <- get
  let
    view = case m of
      Mock {cpos = Nothing}       -> NullP
      Mock {cpos = Just k, store} -> case Map.lookup k store of
        Nothing -> EmptyP k
        Just v  -> FilledP k v
  pure view

{-------------------------------------------------------------------------------
  Convert observable errors to mock errors
-------------------------------------------------------------------------------}

-- | FIXME(jdral): Matching on strings is very error-prone, but it is the only
-- way to distinguish between specific @'LMDB_Error'@s as thrown by the lower
-- level LMDB bindings. We should improve error handling in the haskell
-- bindings.
fromLMDBError :: LMDB_Error -> Maybe Err
fromLMDBError LMDB_Error{e_context, e_description, e_code} =
  case (e_context, e_description, e_code) of
    ("mdb_cursor_get", "Invalid argument", Left 22) ->
      Just $ ErrInvalidArgument Get
    ("mdb_cursor_put", "Invalid argument", Left 22) ->
      Just $ ErrInvalidArgument Put
    ("mdb_cursor_del", "Invalid argument", Left 22) ->
      Just $ ErrInvalidArgument Del
    ("mdb_cursor_del", "MDB_NOTFOUND: No matching key/data pair found", Right MDB_NOTFOUND) ->
      Just $ ErrNotFound Del
    _                                               ->
      Nothing
