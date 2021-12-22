{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Persist.ClickHouse where

import Conduit
import Control.Exception (Exception, throw)
import Control.Monad (when)
import Control.Monad.Catch (MonadThrow)
import Control.Monad.Logger (MonadLoggerIO, NoLoggingT (runNoLoggingT))
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), ask, mapReaderT, withReaderT)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.Data (Typeable)
import Data.Functor (void, ($>))
import Data.IORef (newIORef)
import Data.Int (Int64)
import qualified Data.Map as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import qualified Data.UUID as UUID
import qualified Data.Vector as V
import Data.Word (Word64)
import Database.ClickHouse.From (bsToClickhouseType, decodeToClickhouseType)
import Database.Clickhouse.Client
import Database.Clickhouse.Render
import Database.Clickhouse.Types
import Database.Persist.ClickHouse.Conversion
import Database.Persist.ClickHouse.Migrate
import Database.Persist.Sql
import Database.Persist.Sql.Types.Internal (makeIsolationLevelStatement)
import qualified Database.Persist.Sql.Util as Util
import Database.Persist.SqlBackend
import Foreign (fromBool)
import GHC.Real (Ratio ((:%)))
import Network.HTTP.Req (MonadHttp, Req, defaultHttpConfig, runReq)
import PyF

type WithClickhouseEnv m a = ReaderT ClickhouseEnv m a

-- | Prepare a query.  We don't support prepared statements, but
-- we'll do some client-side preprocessing here.
-- prepare' :: ClickhouseSettings -> Text -> IO Statement
prepare' :: ClickhouseEnv -> Text -> IO Statement
prepare' connection sql = do
  let query = Query . TLE.encodeUtf8 $ TL.fromStrict sql
  return
    Statement
      { stmtFinalize = return (),
        stmtReset = return (),
        stmtExecute = execute' connection query,
        stmtQuery = withStmt' connection query
      }

executeWithPersistValue :: (MonadHttp m, MonadThrow m) => ClickhouseEnv -> Query -> [PersistValue] -> m ByteString
executeWithPersistValue connection query vals = do
  execute connection query (persistValueToClickhouseType <$> vals)

-- | Execute an statement that doesn't return any results.
-- TODO: Float out runReq from here
-- TODO: Somehow extract meaningful error code from ClickHouse response bytestring
-- execute' :: ClickhouseSettings -> Query -> [PersistValue] -> IO Int64
execute' :: ClickhouseEnv -> Query -> [PersistValue] -> IO Int64
execute' connection query params =
  0 <$ runReq defaultHttpConfig (executeWithPersistValue connection query params)

-- | Execute an statement that does return results.  The results
-- are fetched all at once and stored into memory.
withStmt' ::
  MonadIO m =>
  ClickhouseEnv ->
  Query ->
  [PersistValue] ->
  Acquire (ConduitM () [PersistValue] m ())
withStmt' !connection !query !params = do
  let wtf = runReq defaultHttpConfig $ executeWithPersistValue connection query params
  result <- mkAcquire wtf (const $ return ())
  let (_, !colData) = decodeToClickhouseType result
      colDataPV = V.map (V.toList . V.map clickhouseTypeToPersistValue) colData
  liftIO $ BS.putStrLn "\n"
  liftIO $ BS.putStrLn $ BS.pack $ show colDataPV
  liftIO $ BS.putStrLn "\n"
  return $ yieldMany colDataPV

insertSql' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSql' ent vals =
  case getEntityId ent of
    EntityIdNaturalKey _ ->
      ISRManyKeys sql vals
    EntityIdField _ ->
      ISRInsertGet sql "SELECT LAST_INSERT_ID()"
  where
    (fieldNames, placeholders) = unzip (Util.mkInsertPlaceholders ent escapeFT)
    sql =
      [fmt|\
INSERT INTO {tableName} ( {T.intercalate ", " fieldNames} )
VALUES ( {T.intercalate "," placeholders} )
      |]
    tableName = escapeET $ getEntityDBName ent

withClickHouse ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  ClickhouseEnv ->
  (SqlBackend -> m a) ->
  m a
withClickHouse = withSqlConn . openClickhouseEnv

runClickhouse ::
  (MonadUnliftIO m) =>
  ClickhouseEnv ->
  -- | database action
  ReaderT SqlBackend (NoLoggingT (ResourceT m)) a ->
  m a
runClickhouse connection =
  runResourceT
    . runNoLoggingT
    . withClickHouse connection
    . runSqlConn

openClickhouseEnv :: ClickhouseEnv -> LogFunc -> IO SqlBackend
openClickhouseEnv env@ClickhouseEnv {..} logFunc = do
  smap <- newIORef Map.empty
  return $
    mkSqlBackend
      MkSqlBackendArgs
        { connPrepare = prepare' env,
          connInsertSql = insertSql',
          connStmtMap = smap,
          connClose = return (),
          connMigrateSql = migrate' env,
          connBegin = \_ _ -> return (),
          connCommit = \_ -> return (),
          connRollback = \_ -> return (),
          connEscapeFieldName = T.pack . escapeF,
          connEscapeTableName = T.pack . escapeE . getEntityDBName,
          connEscapeRawName = T.pack . escapeDBName . T.unpack,
          connNoLimit = "LIMIT 18446744073709551615",
          connRDBMS = "clickhouse",
          connLimitOffset = decorateSQLWithLimitOffset "LIMIT 18446744073709551615",
          connLogFunc = logFunc
        }
