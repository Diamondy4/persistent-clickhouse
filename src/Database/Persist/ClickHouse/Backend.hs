{-# LANGUAGE ScopedTypeVariables #-}

module Database.Persist.ClickHouse.Backend
  ( module Database.Persist.ClickHouse.Internal.Backend,
    module Database.Persist.ClickHouse.Backend,
    module Database.Persist.ClickHouse.Internal.TH,
  )
where

import Conduit
import qualified Control.Exception as E
import Control.Monad.Logger
import Control.Monad.Reader
import Data.IORef
import qualified Data.Map as Map
import Database.Clickhouse.Types
import Database.Persist.ClickHouse.Internal.Backend
import Database.Persist.ClickHouse.Internal.TH
import Database.Persist.Sql

-- | Create new connection using client settings and logger
openClickhouseConnectionSettings ::
  forall connectionType.
  (ClickhouseClient connectionType) =>
  ClickhouseClientSettings connectionType ->
  ClickhouseConnectionSettings ->
  LogFunc ->
  IO (ClickhouseBackend connectionType)
openClickhouseConnectionSettings settings env logFunc = do
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
    E.bracket
      (open logFunc)
      (\_ -> return ())
      (run . f)

-- | Run db action in ClickHouse, providing logging monad manually.
withClickHouse ::
  (MonadUnliftIO m, MonadLoggerIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  (ClickhouseBackend client -> m a) ->
  m a
withClickHouse settings env = withClickhouseConn toOpen
  where
    toOpen = openClickhouseConnectionSettings settings env

-- | Run db action in ClickHouse, without logging.
runClickhouse ::
  (MonadUnliftIO m, ClickhouseClient client) =>
  ClickhouseClientSettings client ->
  ClickhouseConnectionSettings ->
  -- | database action
  ReaderT (ClickhouseBackend client) (NoLoggingT (ResourceT m)) a ->
  m a
runClickhouse settings env =
  runResourceT
    . runNoLoggingT
    . withClickHouse settings env
    . runReaderT
