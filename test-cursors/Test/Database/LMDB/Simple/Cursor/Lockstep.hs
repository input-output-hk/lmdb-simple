{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns        #-}
{-# LANGUAGE Rank2Types            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LMDB.Simple.Cursor.Lockstep (tests) where

import           Prelude                                        hiding (init)

import           Control.Exception                              ()
import           Control.Monad                                  ((<=<))
import           Control.Monad.Catch                            (MonadCatch (..),
                                                                 MonadThrow (..))
import           Control.Monad.IO.Class
import           Data.Bifunctor
import qualified Data.Map.Strict                                as Map
import           Data.Maybe
import           Data.Proxy
import           Data.Typeable
import           System.Directory
import           System.IO.Temp

import qualified Test.QuickCheck                                as QC
import           Test.QuickCheck                                (Arbitrary, Gen,
                                                                 Property)
import           Test.QuickCheck.StateModel
import           Test.QuickCheck.StateModel.Lockstep            as Lockstep
import           Test.QuickCheck.StateModel.Lockstep.Defaults   as Lockstep
import           Test.QuickCheck.StateModel.Lockstep.Op.SumProd as Lockstep
import           Test.QuickCheck.StateModel.Lockstep.Run        as Lockstep
import           Test.Tasty                                     hiding (after)
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck                          (testProperty)

import           Database.LMDB.Raw
import           Database.LMDB.Simple
import           Database.LMDB.Simple.Cursor
import           Database.LMDB.Simple.Internal

import           Test.Database.LMDB.Simple.Cursor.Lockstep.Mock

{-------------------------------------------------------------------------------
  Main test tree
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Lockstep" [
    testProperty "lockstepTest" (QC.withMaxSuccess 1000 lockstepTest)
  , testCase "labelledExamples" labelledExamples
  ]

lockstepTest :: Actions (Lockstep T) -> Property
lockstepTest =
  runActionsBracket
    pT
    (initTestEnv @IO @Word @Word)
    cleanupTestEnv
    (\cm tenv ->
        readWriteTransaction (dbEnv tenv) (runCursorAsTransaction cm (db tenv))
    )

labelledExamples :: IO ()
labelledExamples = QC.labelledExamples $
  tagActions pT

type T = CursorState Word Word

pT :: Proxy T
pT = Proxy

{-------------------------------------------------------------------------------
  Resource initialisation/cleanup
-------------------------------------------------------------------------------}

data TestEnv k v = TestEnv {
    dbFilePath :: FilePath
  , dbEnv      :: Environment ReadWrite
  , db         :: Database k v
  }

initTestEnv :: MonadIO m => m (TestEnv k v)
initTestEnv = do
  dbFilePath <- createDbDir
  dbEnv <- liftIO $ openEnvironment dbFilePath testLMDBLimits
  db <- liftIO $ readWriteTransaction dbEnv (getDatabase Nothing)
  pure TestEnv {dbFilePath, dbEnv, db}

cleanupTestEnv :: MonadIO m => TestEnv k v -> m ()
cleanupTestEnv tenv = do
  liftIO $ closeEnvironmentBlocking (dbEnv tenv)
  deleteDbDir (dbFilePath tenv)

createDbDir :: MonadIO m => m FilePath
createDbDir = do
  sysTmpDir <- liftIO getTemporaryDirectory
  liftIO $ createTempDirectory sysTmpDir "qd_cursor"

deleteDbDir :: MonadIO m => FilePath -> m ()
deleteDbDir = liftIO . removeDirectoryRecursive

testLMDBLimits :: Limits
testLMDBLimits = defaultLimits

{-------------------------------------------------------------------------------
  Model state
-------------------------------------------------------------------------------}

data CursorState k v = CursorState {
    csMock  :: Mock k v
  , csStats :: Stats k v
  }
  deriving stock (Show, Eq)

initState :: CursorState k v
initState = CursorState {
    csMock = emptyMock
  , csStats = initStats
  }

{-------------------------------------------------------------------------------
  @'StateModel'@ and @'RunModel'@ instances
-------------------------------------------------------------------------------}

type RealMonad k v mode = CursorM k v mode
type instance Realized (CursorM k v mode) a = a

type CursorAct k v a = Action (Lockstep (CursorState k v)) (Either Err a)

instance ( Show k, Show v
         , Ord k, Eq v
         , Typeable k, Typeable v
         , Arbitrary k, Arbitrary v
         ) => StateModel (Lockstep (CursorState k v)) where
  data Action (Lockstep (CursorState k v)) a where
    CursorGet         :: MDB_cursor_op            -> CursorAct k v (Maybe (k, v))
    CursorGetSet      :: k                        -> CursorAct k v ()
    CursorGetSetKey   :: k                        -> CursorAct k v (Maybe (k, v))
    CursorGetSetRange :: k                        -> CursorAct k v (Maybe (k, v))
    CursorPut         :: Maybe CPutFlag -> k -> v -> CursorAct k v Bool
    CursorDel         :: Maybe CDelFlag           -> CursorAct k v ()

  initialState        = Lockstep.initialState initState
  nextState           = Lockstep.nextState
  precondition st act = Lockstep.precondition st act
                        && modelPrecondition (getModel st) act
  arbitraryAction     = Lockstep.arbitraryAction
  shrinkAction        = Lockstep.shrinkAction

instance ( Show k, Show v
         , Ord k, Eq v
         , Typeable k, Typeable v
         , Arbitrary k, Arbitrary v
         , Serialise k, Serialise v
         ) => RunModel (Lockstep (CursorState k v)) (RealMonad k v ReadWrite) where
  perform _st    = runCM
  postcondition = Lockstep.postcondition
  monitoring    = Lockstep.monitoring (Proxy @(CursorM k v ReadWrite))

deriving stock instance (Show k, Show v)
                     => Show (LockstepAction (CursorState k v) a)
deriving stock instance (Eq k, Eq v) => Eq (LockstepAction (CursorState k v) a)

-- | Custom precondition
modelPrecondition ::
       Ord k
    => CursorState k v
    -> LockstepAction (CursorState k v) a
    -> Bool
modelPrecondition (CursorState m _stats) action = case action of
  CursorGet op
    | op == MDB_NEXT || op == MDB_NEXT_NODUP -> case unsafeEvalMC viewCPos m of
        FilledP _ _ -> True
        EmptyP _    -> False
        NullP       -> True
  CursorPut (Just CPF_MDB_CURRENT) k _v -> case unsafeEvalMC viewCPos m of
    NullP          -> True
    -- The cursor should point to a filled key.
    EmptyP _k2     -> False
    -- Given key should match the key that the cursor points to.
    FilledP k2 _v2 -> k == k2
  CursorPut (Just CPF_MDB_NOOVERWRITE) k _v -> isNothing $ Map.lookup k (store m)
  _ -> True

{-------------------------------------------------------------------------------
  @'InLockstep'@ instance
-------------------------------------------------------------------------------}

type CursorVal k v a = ModelValue (CursorState k v) a
type CursorObs k v a = Observable (CursorState k v) a
type CVal k v a = CursorVal k v a
type CObs k v a = CursorObs k v a

instance ( Show k, Show v
         , Ord k, Eq v
         , Typeable k, Typeable v
         , Arbitrary k, Arbitrary v
         ) => InLockstep (CursorState k v) where

  -- | Model values
  data instance ModelValue (CursorState k v) a where
    MKVPair  :: Maybe (k, v) -> CursorVal k v (Maybe (k, v))
    MBool    :: Bool         -> CursorVal k v Bool
    MUnit    :: ()           -> CursorVal k v ()
    MErr     :: Err          -> CursorVal k v Err

    MEither  :: Either (CVal k v a) (CVal k v b) -> CVal k v (Either a b)
    MPair    :: (CVal k v a, CVal k v b)         -> CVal k v (a, b)

  -- | Observable results
  data instance Observable (CursorState k v) a where
    OId     :: (Show a, Eq a, Typeable a) => a -> CObs k v a
    OEither :: Either (CObs k v a) (CObs k v b) -> CObs k v (Either a b)
    OPair   :: (CObs k v a, CObs k v b) -> CObs k v (a, b)

  observeModel :: CursorVal k v a -> CursorObs k v a
  observeModel = \case
    MKVPair x -> OId x
    MUnit x   -> OId x
    MBool x   -> OId x
    MErr x    -> OId x
    MEither x -> OEither $ bimap observeModel observeModel x
    MPair x   -> OPair   $ bimap observeModel observeModel x

  modelNextState ::
       forall a.
       LockstepAction (CursorState k v) a
    -> ModelLookUp (CursorState k v)
    -> CursorState k v
    -> ( CursorVal k v a
       , CursorState k v
       )
  modelNextState action lookUp (CursorState mock stats) =
      auxStats $ runMock lookUp action mock
    where
      auxStats (result, state') =
        (result, CursorState state' $ updateStats action result stats)

  usedVars ::
       LockstepAction (CursorState k v) a
    -> [AnyGVar (ModelOp (CursorState k v))]
  usedVars = const []

  arbitraryWithVars ::
       ModelFindVariables (CursorState k v)
    -> CursorState k v
    -> Gen (Any (LockstepAction (CursorState k v)))
  arbitraryWithVars = arbitraryCursorAction

  shrinkWithVars ::
       ModelFindVariables (CursorState k v)
    -> CursorState k v
    -> LockstepAction (CursorState k v) a
    -> [Any (LockstepAction (CursorState k v))]
  shrinkWithVars = shrinkCursorAction

  tagStep ::
       (CursorState k v, CursorState k v)
    -> LockstepAction (CursorState k v) a
    -> ModelValue (CursorState k v) a
    -> [String]
  tagStep (_before, CursorState _ after) action val =
    map show $ tagCursorAction after action val

deriving stock instance (Show k, Show v) => Show (CursorVal k v a)

deriving stock instance (Show k, Show v) => Show (CursorObs k v a)
deriving stock instance (Eq k, Eq v) => Eq (CursorObs k v a)

{-------------------------------------------------------------------------------
  @'RunLockstep'@ instance
-------------------------------------------------------------------------------}

instance (Show k, Show v, Eq k, Eq v, Ord k, Typeable k, Typeable v
         , Arbitrary k, Arbitrary v, Serialise k, Serialise v)
      => RunLockstep (CursorState k v) (RealMonad k v ReadWrite) where
  observeReal ::
       Proxy (RealMonad k v ReadWrite)
    -> LockstepAction (CursorState k v) a
    -> Realized (RealMonad k v ReadWrite) a
    -> Observable (CursorState k v) a
  observeReal _proxy = \case
      CursorGet{}         -> OEither . bimap OId OId
      CursorGetSet{}      -> OEither . bimap OId OId
      CursorGetSetKey{}   -> OEither . bimap OId OId
      CursorGetSetRange{} -> OEither . bimap OId OId
      CursorPut{}         -> OEither . bimap OId OId
      CursorDel{}         -> OEither . bimap OId OId

{-------------------------------------------------------------------------------
  Interpreter against the model
-------------------------------------------------------------------------------}

runMock ::
     (Ord k, Eq v)
  => ModelLookUp (CursorState k v)
  -> Action (Lockstep (CursorState k v)) a
  -> Mock k v
  -> ( CursorVal k v a
     , Mock k v
     )
runMock _lookUp = \case
    CursorGet op        -> wrap MKVPair . runMC (mCursorGet op)
    CursorGetSet k      -> wrap MUnit   . runMC (mCursorGetSet k)
    CursorGetSetKey k   -> wrap MKVPair . runMC (mCursorGetSetKey k)
    CursorGetSetRange k -> wrap MKVPair . runMC (mCursorGetSetRange k)
    CursorPut pfMay k v -> wrap MBool   . runMC (mCursorPut pfMay k v)
    CursorDel dfMay     -> wrap MUnit   . runMC (mCursorDel dfMay)
  where
    wrap ::
         (a -> CVal k v b)
      -> ( Either Err a
         , Mock k v
         )
      -> ( CVal k v (Either Err b)
         , Mock k v
         )
    wrap f = first (MEither . bimap MErr f)

{-------------------------------------------------------------------------------
  Generator
-------------------------------------------------------------------------------}

arbitraryCursorAction ::
     forall k v. (Ord k, Eq v, Typeable k, Typeable v, Arbitrary k, Arbitrary v)
  => ModelFindVariables (CursorState k v)
  -> CursorState k v
  -> Gen (Any (LockstepAction (CursorState k v)))
arbitraryCursorAction _findVars (CursorState m _stats) = QC.oneof withoutVars
  where
    withoutVars :: [Gen (Any (LockstepAction (CursorState k v)))]
    withoutVars = [
        genCursorGet
      , genCursorPut
      , genCursorDel
      ]

    genCursorGet :: Gen (Any (Action (Lockstep (CursorState k v))))
    genCursorGet = QC.oneof [
          genCursorGet' MDB_FIRST
        , genCursorGet' MDB_GET_CURRENT
        , genCursorGet' MDB_LAST
        , genCursorGet' MDB_NEXT
        , genCursorGet' MDB_NEXT_NODUP
        , genCursorGet' MDB_PREV
        , genCursorGet' MDB_PREV_NODUP
        , genCursorGetSet
        , genCursorGetSetKey
        , genCursorGetSetRange
        ]
      where
        genCursorGet' ::
              MDB_cursor_op
          -> Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorGet' op = pure (Some $ CursorGet op )

        genCursorGetSet :: Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorGetSet = Some <$> (CursorGetSet <$> genKey)

        genCursorGetSetKey :: Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorGetSetKey = Some <$> (CursorGetSetKey <$> genKey)

        genCursorGetSetRange :: Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorGetSetRange = Some <$> (CursorGetSetRange <$> genKey)

    genCursorPut :: Gen (Any (Action (Lockstep (CursorState k v))))
    genCursorPut = QC.oneof [
          genCursorPut' Nothing
        , genCursorPut' (Just CPF_MDB_NOOVERWRITE)
        , genCursorPut' (Just CPF_MDB_APPEND)
        , genCursorPutCurrent
        ]
      where
        genCursorPut' ::
              Maybe CPutFlag
          -> Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorPut' pfMay = fmap Some $
          CursorPut pfMay <$> genKey <*> genValue

        genCursorPutCurrent :: Gen (Any (Action (Lockstep (CursorState k v))))
        genCursorPutCurrent = Some <$>
          let genKey' = case unsafeEvalMC viewCPos m of
                NullP          -> genKey
                EmptyP k2      -> QC.frequency [(1, genKey), (10, pure k2)]
                FilledP k2 _v2 -> QC.frequency [(1, genKey), (10, pure k2)]
          in  CursorPut (Just CPF_MDB_CURRENT) <$> genKey' <*> genValue

    genCursorDel :: Gen (Any (Action (Lockstep (CursorState k v))))
    genCursorDel = pure $ Some . CursorDel $ Nothing

    genKey :: Gen k
    genKey = QC.arbitrary

    genValue :: Gen v
    genValue = QC.arbitrary

{-------------------------------------------------------------------------------
  Shrinker
-------------------------------------------------------------------------------}

shrinkCursorAction ::
       forall k v a. (Eq k, Eq v, Typeable k, Typeable v, Arbitrary k, Arbitrary v)
    => ModelFindVariables (CursorState k v)
    -> CursorState k v
    -> LockstepAction (CursorState k v) a
    -> [Any (LockstepAction (CursorState k v))]
shrinkCursorAction _findVars _m act = case act of
    CursorGet op        -> fmap Some $ CursorGet <$> shrinkOp op
    CursorGetSet k      -> fmap Some $ CursorGetSet <$> shrinkKey k
    CursorGetSetKey k   -> fmap Some $ CursorGetSetKey <$> shrinkKey k
    CursorGetSetRange k -> fmap Some $ CursorGetSetRange <$> shrinkKey k
    CursorPut pfMay k v -> fmap Some $
      CursorPut <$> shrinkCPutFlag pfMay <*> shrinkKey k <*> shrinkValue v
    CursorDel dfMay     -> fmap Some $ CursorDel <$> shrinkCDelFlag dfMay
  where
    shrinkOp :: MDB_cursor_op -> [MDB_cursor_op]
    shrinkOp MDB_GET_CURRENT = []
    shrinkOp _               = [MDB_GET_CURRENT]

    shrinkCPutFlag :: Maybe CPutFlag -> [Maybe CPutFlag]
    shrinkCPutFlag Nothing = []
    shrinkCPutFlag _       = [Nothing]

    shrinkCDelFlag :: Maybe CDelFlag -> [Maybe CDelFlag]
    shrinkCDelFlag Nothing = []
    shrinkCDelFlag _       = [Nothing]

    shrinkKey :: k -> [k]
    shrinkKey = QC.shrink

    shrinkValue :: v -> [v]
    shrinkValue = QC.shrink

{-------------------------------------------------------------------------------
  Interpret @'Op'@ against @'ModelValue'@
-------------------------------------------------------------------------------}

instance InterpretOp Op (ModelValue (CursorState k v)) where
  intOp OpId         = Just
  intOp OpFst        = \case MPair   x -> Just (fst x)
  intOp OpSnd        = \case MPair   x -> Just (snd x)
  intOp OpLeft       = \case MEither x -> either Just (const Nothing) x
  intOp OpRight      = \case MEither x -> either (const Nothing) Just x
  intOp (OpComp g f) = intOp g <=< intOp f

{-------------------------------------------------------------------------------
  Interpreter for @'CursorM'@
-------------------------------------------------------------------------------}

runCM ::
     (Serialise k, Serialise v)
  => LockstepAction (CursorState k v) a
  -> LookUp (RealMonad k v mode)
  -> RealMonad k v ReadWrite (Realized (RealMonad k v mode) a)
runCM act _lookUp = case act of
  CursorGet op        -> catchErr $ cgetG op
  CursorGetSet k      -> catchErr $ cgetSet k
  CursorGetSetKey k   -> catchErr $ cgetSetKey k
  CursorGetSetRange k -> catchErr $ cgetSetRange k
  CursorPut fls k v   -> catchErr $ cputG fls k v
  CursorDel fls       -> catchErr $ cdelG fls

catchErr :: RealMonad k v mode a -> RealMonad k v mode (Either Err a)
catchErr act = catch (Right <$> act) handler
  where
    handler :: LMDB_Error -> RealMonad k v mode (Either Err a)
    handler e = maybe (throwM e) (return . Left) (fromLMDBError e)

{-------------------------------------------------------------------------------
  Statistics and tagging
-------------------------------------------------------------------------------}

data Stats k v = Stats
  deriving stock (Show, Eq)

initStats :: Stats k v
initStats = Stats

updateStats ::
     Ord k
  => LockstepAction (CursorState k v) a
  -> CursorVal k v a
  -> Stats k v
  -> Stats k v
updateStats _ _ stats = stats

data Tag = Tag
  deriving (Show, Eq)

tagCursorAction ::
     (Show k, Show v, Ord k)
  => Stats k v
  -> LockstepAction (CursorState k v) a
  -> ModelValue (CursorState k v) a
  -> [Tag]
tagCursorAction _ _ _ = []
