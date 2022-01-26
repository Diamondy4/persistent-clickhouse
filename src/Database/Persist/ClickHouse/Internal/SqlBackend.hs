{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Persist.ClickHouse.Internal.SqlBackend where

import Conduit
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import qualified Data.Vector as V
import Database.ClickHouse
import Database.Clickhouse.Conversion.TSV.From
import Database.Clickhouse.Conversion.Types
import Database.Clickhouse.Conversion.Values.Renderer (PreparedQuery (PreparedQuery))
import Database.Clickhouse.Types
import Database.Persist.ClickHouse.Internal.Conversion
import Database.Persist.ClickHouse.Internal.Misc
import Database.Persist.ClickHouse.Internal.SQL
import Database.Persist.Sql

-- | Prepare a query.  We don't support prepared statements, but
-- we'll do some client-side preprocessing here.
-- prepare' :: ClickhouseHTTPEnv -> Text -> IO Statement
prepare' ::
  (ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  Text ->
  IO Statement
prepare' settings connection sql = do
  let query = PreparedQuery . TLE.encodeUtf8 $ TL.fromStrict sql
  return
    Statement
      { stmtFinalize = return (),
        stmtReset = return (),
        stmtExecute = execute' settings connection query,
        stmtQuery = withStmt' settings connection query
      }

--executeWithPersistValue :: ClickhouseHTTPEnv -> PreparedQuery -> [PersistValue] -> IO ByteString
executeWithPersistValue ::
  ( ClickhouseClient client,
    QueryRenderer renderer,
    Foldable f,
    Functor f
  ) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  RenderQueryType renderer ->
  f PersistValue ->
  IO ByteString
executeWithPersistValue settings connection query vals = do
  executePrepared settings connection query (persistValueToClickhouseType <$> vals)

-- | Execute an statement that doesn't return any results.
-- TODO: Float out runReq from here
-- TODO: Somehow extract meaningful error code from ClickHouse response bytestring

{- execute' :: ClickhouseHTTPEnv -> PreparedQuery -> [PersistValue] -> IO Int64 -}
execute' ::
  ( ClickhouseClient client,
    QueryRenderer renderer,
    Foldable f,
    Functor f
  ) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  RenderQueryType renderer ->
  f PersistValue ->
  IO Int64
execute' settings connection query params =
  0 <$ executeWithPersistValue settings connection query params

-- | Execute an statement that does return results.  The results
-- are fetched all at once and stored into memory.

{- withStmt' ::
  MonadIO m =>
  ClickhouseHTTPEnv ->
  PreparedQuery ->
  [PersistValue] ->
  Acquire (ConduitM () [PersistValue] m ()) -}
withStmt' ::
  ( ClickhouseClient client,
    QueryRenderer renderer,
    Foldable f,
    Monad m,
    Functor f
  ) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  RenderQueryType renderer ->
  f PersistValue ->
  Acquire (ConduitT () [PersistValue] m ())
withStmt' settings !connection !query !params = do
  let action = executeWithPersistValue settings connection query params
  result <- mkAcquire action (const $ return ())
  let (_, !colData) = decodeToClickhouseRows result
      colDataPV = V.map (V.toList . V.map clickhouseTypeToPersistValue) colData
  return $ yieldMany colDataPV

insertSqlBackend' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSqlBackend' ent vals =
  case insertSqlValues' ent vals of
    InsertClickhouseQuery sql vals' -> ISRManyKeys sql vals'

{- withClickHouse ::
  (MonadUnliftIO m, MonadLoggerIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  (SqlBackend -> m a) ->
  m a
withClickHouse settings env = withSqlConn toOpen
  where
    toOpen = openClickhouseConnectionSettings settings env

runClickhouse ::
  (MonadUnliftIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  -- | database action
  ReaderT SqlBackend (NoLoggingT (ResourceT m)) a ->
  m a
runClickhouse settings env =
  runResourceT
    . runNoLoggingT
    . withClickHouse settings env
    . runSqlConn -}

{- openClickhouseConnectionSettings ::
  forall connectionType.
  (ClickhouseClient connectionType) =>
  ClickhouseClientSettings connectionType ->
  ClickhouseConnectionSettings ->
  LogFunc ->
  IO SqlBackend
openClickhouseConnectionSettings settings env@ClickhouseConnectionSettings {..} logFunc = do
  smap <- newIORef Map.empty
  return $
    mkSqlBackend
      MkSqlBackendArgs
        { connPrepare = prepare' settings env,
          connInsertSql = insertSqlBackend',
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
 -}