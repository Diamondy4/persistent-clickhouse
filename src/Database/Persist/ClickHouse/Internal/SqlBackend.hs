{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Persist.ClickHouse.Internal.SqlBackend where

import Conduit
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Encoding qualified as TLE
import Data.Vector qualified as V
import Database.ClickHouse
import Database.Clickhouse.Client.Types
import Database.Clickhouse.Conversion.CSV.Decode
import Database.Clickhouse.Conversion.PreparedQuery
import Database.Clickhouse.Conversion.Types
import Database.Persist.ClickHouse.Internal.Conversion
import Database.Persist.Sql

{- | Prepare a query. ClickHouse does not support prepared statements, but
 we'll do some client-side preprocessing here.
-}
prepare' ::
  forall client.
  (ClickhouseClientAcquire client) =>
  ClickhouseConnectionSettings client ->
  Text ->
  IO Statement
prepare' settings sql = do
  let query = PreparedQuery . TLE.encodeUtf8 $ TL.fromStrict sql
  return
    Statement
      { stmtFinalize = return ()
      , stmtReset = return ()
      , stmtExecute = execute' settings query
      , stmtQuery = withStmt' settings query
      }

executeWithPersistValueSource ::
  ( ClickhouseClientAcquire client
  , QueryRenderer renderer
  , Foldable f
  , Functor f
  , MonadIO m
  ) =>
  ClickhouseConnectionSettings client ->
  RenderQueryType renderer ->
  f PersistValue ->
  Acquire (ConduitM i ByteString m ())
executeWithPersistValueSource settings query vals = do
  executePreparedAcquire settings query (persistValueToClickhouseType <$> vals)

{- | Execute an statement that doesn't return any results.
 TODO: Somehow extract meaningful error code from ClickHouse response string (likely impossible).
-}

{- execute' :: ClickhouseHTTPEnv -> PreparedQuery -> [PersistValue] -> IO Int64 -}
execute' ::
  forall renderer client f m.
  ( QueryRenderer renderer
  , Foldable f
  , Functor f
  , MonadIO m
  , ClickhouseClientAcquire client
  , m ~ IO
  ) =>
  ClickhouseConnectionSettings client ->
  RenderQueryType renderer ->
  f PersistValue ->
  IO Int64
execute' settings query params =
  withAcquire (executeWithPersistValueSource @client @_ @_ @IO settings query params) (const $ pure 0)

{- | Execute an statement that does return results.  The results
 are fetched all at once and stored into memory.
-}
withStmt' ::
  ( QueryRenderer renderer
  , Foldable f
  , Monad m
  , Functor f
  , ClickhouseClientAcquire client
  , MonadIO m
  ) =>
  ClickhouseConnectionSettings client ->
  RenderQueryType renderer ->
  f PersistValue ->
  Acquire (ConduitT () [PersistValue] m ())
withStmt' !settings !query !params = do
  source <- executeWithPersistValueSource settings query params
  pure $
    source
      .| decodeToClickhouseRowsC
      .| mapC V.toList
      .| mapCE clickhouseTypeToPersistValue

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
  ClickhouseConnectionSettings connectionType ->
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