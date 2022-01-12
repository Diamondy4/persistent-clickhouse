{-# LANGUAGE ScopedTypeVariables #-}

module Database.Persist.ClickHouse.Backend
  ( module Database.Persist.ClickHouse.Backend,
    module Database.Persist.ClickHouse.Internal.PersistFieldInstances,
  )
where

import Conduit (MonadUnliftIO (withRunInIO), ResourceT, runResourceT)
import Control.Exception (throwIO)
import qualified Control.Exception as UE
import Control.Monad.IO.Class
import Control.Monad.Logger (MonadLoggerIO (askLoggerIO), NoLoggingT (runNoLoggingT), logDebugNS, runLoggingT)
import Control.Monad.Reader (ReaderT (runReaderT), ask)
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.Coerce
import qualified Data.Conduit.List as CL
import Data.Foldable
import qualified Data.Foldable as Foldable
import Data.IORef
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Void
import Database.ClickHouse
import Database.Clickhouse.Client.HTTP.Client
import Database.Clickhouse.Client.HTTP.Types
import Database.Clickhouse.Conversion.TSV.Renderer
import Database.Clickhouse.Conversion.Types
import Database.Clickhouse.Conversion.Values.Renderer
import Database.Clickhouse.Types
import Database.Persist
import Database.Persist.ClickHouse.Internal.Backend
import Database.Persist.ClickHouse.Internal.Migrate
import Database.Persist.ClickHouse.Internal.Misc
import Database.Persist.ClickHouse.Internal.PersistFieldInstances
import Database.Persist.ClickHouse.Internal.SqlBackend
import Database.Persist.Sql
import Database.Persist.Sql.Util
import Database.Persist.SqlBackend
import GHC.Generics
import Optics ((^.))
import Optics.Generic
import PyF

data ClickhouseBackend connectionType = ClickhouseBackend
  { env :: !ClickhouseEnv,
    settings :: !(ClickhouseClientSettings connectionType),
    stmtMap :: !(IORef (Map Text Statement)),
    logFunc :: !LogFunc
  }
  deriving (Generic)

-- | Clickhouse doent use generated key, so you must provide manually.
instance PersistCore (ClickhouseBackend connectionType) where
  newtype BackendKey (ClickhouseBackend connectionType) = ClickhouseBackendKey Void
    deriving stock (Show, Read, Eq, Ord, Generic)
    deriving newtype (A.ToJSON, A.FromJSON)

instance PersistField (BackendKey (ClickhouseBackend connectionType)) where
  toPersistValue v = absurd (coerce v)
  fromPersistValue _ = Left "Clickhouse doesn't use default primary key, please specify it manually"

clickhouseBackendToSqlBackend ::
  forall connectionType.
  (ClickhouseClient connectionType) =>
  ClickhouseBackend connectionType ->
  SqlBackend
clickhouseBackendToSqlBackend clickbackend =
  mkSqlBackend
    MkSqlBackendArgs
      { connPrepare = prepare' (clickbackend ^. #settings) (clickbackend ^. #env),
        connInsertSql = insertSqlBackend',
        connStmtMap = clickbackend ^. #stmtMap,
        connClose = return (),
        connMigrateSql = migrate' $ clickbackend ^. #env,
        connBegin = \_ _ -> return (),
        connCommit = \_ -> return (),
        connRollback = \_ -> return (),
        connEscapeFieldName = T.pack . escapeF,
        connEscapeTableName = T.pack . escapeE . getEntityDBName,
        connEscapeRawName = T.pack . escapeDBName . T.unpack,
        connNoLimit = "LIMIT 18446744073709551615",
        connRDBMS = "clickhouse",
        connLimitOffset = decorateSQLWithLimitOffset "LIMIT 18446744073709551615",
        connLogFunc = clickbackend ^. #logFunc
      }

instance (ClickhouseClient connectionType) => BackendCompatible SqlBackend (ClickhouseBackend connectionType) where
  projectBackend = clickhouseBackendToSqlBackend

instance (ClickhouseClient connectionType) => HasPersistBackend (ClickhouseBackend connectionType) where
  type BaseBackend (ClickhouseBackend connectionType) = SqlBackend
  persistBackend = clickhouseBackendToSqlBackend

instance (ClickhouseClient connectionType) => PersistStoreRead (ClickhouseBackend connectionType) where
  get k = withBaseBackend $ get k
  getMany ks = withBaseBackend $ getMany ks

instance (ClickhouseClient connectionType) => PersistStoreWrite (ClickhouseBackend connectionType) where
  insert val = do
    conn <- ask
    let esql = insertSqlValues' t vals

    case esql of
      InsertClickhouseQuery sql fs -> do
        rawExecute sql vals
        case entityPrimary t of
          Nothing ->
            error $ "Primary key is not defined " ++ show sql
          Just pdef ->
            let pks = Foldable.toList $ fieldHaskell <$> compositeFields pdef
                keyvals = map snd $ filter (\(a, _) -> let ret = isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ getEntityFields t) fs
             in case keyFromValues keyvals of
                  Right k -> return k
                  Left e -> error $ "InsertClickhouseQuery: unexpected keyvals result: " `mappend` T.unpack e
    where
      t = entityDef $ Just val
      vals = mkInsertValues val

  insertMany recs = do
    conn <- ask
    let esql = insertManySql' t valsV
    case esql of
      InsertManyClickhouseQuery sql _ -> do
        runLoggingT
          (logDebugNS "SQL" $ T.append sql "; " <> T.pack (show vals))
          (logFunc conn)
        let preparedQuery = PreparedQuery . BSC.fromStrict $ TE.encodeUtf8 sql
        let renderedQuery = renderRows preparedQuery clickhouseTypeVals
        send (conn ^. #settings) (conn ^. #env) renderedQuery
        case entityPrimary t of
          Nothing ->
            error $ "Primary key is not defined " ++ show sql
          Just pdef ->
            let pks = Foldable.toList $ fieldHaskell <$> compositeFields pdef
                getKeyvals fs = map snd $ filter (\(a, _) -> let ret = isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ getEntityFields t) fs
                processKeyvals keyvals = case keyFromValues keyvals of
                  Right k -> k
                  Left e -> error $ "InsertClickhouseQuery: unexpected keyvals result: " `mappend` T.unpack e
             in pure $ processKeyvals . getKeyvals <$> vals
    where
      vals = mkInsertValues <$> recs
      valsV = V.fromList $ mkInsertValues <$> recs
      clickhouseTypeVals = rowsPersistValueToClickhouseType vals
      t = entityDef recs

  -- withBaseBackend $ insertMany vals
  insertKey = error "Clickhouse doesn't support insertion by key, please use insert with whole record"
  repsert = error "Repsert functionality is not implemented for Clickhouse as it dangerous to be called"
  replace = error "Replace functionality is not implemented for Clickhouse as it dangerous to be called"
  delete = error "Delete functionality is not implemented for Clickhouse as it dangerous to be called"
  update = error "Update functionality is not implemented for Clickhouse as it dangerous to be called"

instance (ClickhouseClient connectionType) => PersistQueryRead (ClickhouseBackend connectionType) where
  selectSourceRes f opts = withBaseBackend $ selectSourceRes f opts
  selectKeysRes f opts = withBaseBackend $ selectKeysRes f opts
  count f = withBaseBackend $ count f
  exists f = withBaseBackend $ exists f

openClickhouseEnv ::
  forall connectionType.
  (ClickhouseClient connectionType) =>
  ClickhouseClientSettings connectionType ->
  ClickhouseEnv ->
  LogFunc ->
  IO (ClickhouseBackend connectionType)
openClickhouseEnv settings env logFunc = do
  stmtMap <- newIORef Map.empty
  return $
    ClickhouseBackend
      { env,
        settings,
        logFunc,
        stmtMap
      }

withClickhouseConn ::
  forall m a connectionType.
  (MonadUnliftIO m, MonadLoggerIO m, ClickhouseClient connectionType) =>
  (LogFunc -> IO (ClickhouseBackend connectionType)) ->
  (ClickhouseBackend connectionType -> m a) ->
  m a
withClickhouseConn open f = do
  logFunc <- askLoggerIO
  withRunInIO $ \run ->
    UE.bracket
      (open logFunc)
      (\_ -> return ())
      (run . f)

withClickHouse ::
  (MonadUnliftIO m, MonadLoggerIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseEnv ->
  (ClickhouseBackend client -> m a) ->
  m a
withClickHouse settings env = withClickhouseConn toOpen
  where
    toOpen = openClickhouseEnv settings env

runClickhouse ::
  (MonadUnliftIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseEnv ->
  -- | database action
  ReaderT (ClickhouseBackend client) (NoLoggingT (ResourceT m)) a ->
  m a
runClickhouse settings env =
  runResourceT
    . runNoLoggingT
    . withClickHouse settings env
    . runReaderT