{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Persist.ClickHouse.Backend (
  module Database.Persist.ClickHouse.Internal.Backend,
  module Database.Persist.ClickHouse.Backend,
  module Database.Persist.ClickHouse.Internal.TH,
) where

import Conduit
import Control.Exception qualified as E
import Control.Monad.Logger
import Control.Monad.Reader
import Data.IORef
import Data.Map qualified as Map
import Database.Clickhouse.Client.Types
import Database.Persist.ClickHouse.Internal.Backend
import Database.Persist.ClickHouse.Internal.TH
import Database.Persist.Sql

-- | Create new connection using client settings and logger
openClickhouseConnectionSettings ::
  forall connectionType.
  (ClickhouseClientAcquire connectionType, ClickhouseClient connectionType) =>
  ClickhouseConnectionSettings connectionType ->
  LogFunc ->
  IO ClickhouseBackend
openClickhouseConnectionSettings settings logFunc = do
  stmtMap <- newIORef Map.empty
  return $ ClickhouseBackend settings stmtMap logFunc

withClickhouseConn ::
  forall m a.
  ( MonadUnliftIO m
  , MonadLoggerIO m
  ) =>
  (LogFunc -> IO ClickhouseBackend) ->
  (ClickhouseBackend -> m a) ->
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
  forall client m a.
  ( MonadUnliftIO m
  , MonadLoggerIO m
  , ClickhouseClientAcquire client
  , ClickhouseClient client
  ) =>
  ClickhouseConnectionSettings client ->
  (ClickhouseBackend -> m a) ->
  m a
withClickHouse settings = withClickhouseConn toOpen
 where
  toOpen = openClickhouseConnectionSettings settings

-- | Run db action in ClickHouse, without logging.
runClickhouse ::
  ( MonadUnliftIO m
  , ClickhouseClientAcquire client
  , ClickhouseClient client
  ) =>
  ClickhouseConnectionSettings client ->
  -- | database action
  ReaderT ClickhouseBackend (NoLoggingT (ResourceT m)) a ->
  m a
runClickhouse settings =
  runResourceT
    . runNoLoggingT
    . withClickHouse settings
    . runReaderT