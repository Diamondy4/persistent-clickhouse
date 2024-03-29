{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Persist.ClickHouse.Internal.Backend (ClickhouseBackend (..)) where

import Conduit
import Control.Monad.Reader (ask)
import Data.Aeson qualified as A
import Data.Coerce
import Data.Foldable
import Data.Foldable qualified as Foldable
import Data.IORef
import Data.Map (Map)
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Void
import Database.Clickhouse.Client.Types
import Database.Persist
import Database.Persist.ClickHouse.Internal.Migrate
import Database.Persist.ClickHouse.Internal.Misc
import Database.Persist.ClickHouse.Internal.SQL
import Database.Persist.ClickHouse.Internal.SqlBackend
import Database.Persist.Sql
import Database.Persist.Sql.Util
import Database.Persist.SqlBackend
import GHC.Generics

{- | Clickhouse connection. Note that it is static - ClickHouse
 doesn't need to keep it open, every action is atomic.
-}
data ClickhouseBackend where
  ClickhouseBackend ::
    forall connectionType.
    ( ClickhouseClient connectionType
    , ClickhouseClientAcquire connectionType
    ) =>
    { settings :: !(ClickhouseConnectionSettings connectionType)
    , -- | Used in conversion to SqlBackend
      stmtMap :: !(IORef (Map Text Statement))
    , logFunc :: !LogFunc
    } ->
    ClickhouseBackend

-- | Clickhouse doesn't use autogenerated keys, key must be provided manually for table.
instance PersistCore ClickhouseBackend where
  newtype BackendKey ClickhouseBackend = ClickhouseBackendKey Void
    deriving stock (Show, Read, Eq, Ord, Generic)
    deriving newtype (A.ToJSON, A.FromJSON)

instance PersistField (BackendKey ClickhouseBackend) where
  toPersistValue = absurd . coerce
  fromPersistValue _ = Left "Clickhouse doesn't use default primary key, please specify it manually"

clickhouseBackendToSqlBackend ::
  ClickhouseBackend ->
  SqlBackend
clickhouseBackendToSqlBackend backend = case backend of
  (ClickhouseBackend (settings :: ClickhouseConnectionSettings connectionType) stmtMap logFunc) ->
    mkSqlBackend
      MkSqlBackendArgs
        { connPrepare = prepare' settings
        , connInsertSql = \ent vals -> insertManySql' ent [vals]
        , connStmtMap = stmtMap
        , connClose = return ()
        , connMigrateSql = migrate' settings
        , connBegin = \_ _ -> return ()
        , connCommit = \_ -> return ()
        , connRollback = \_ -> return ()
        , connEscapeFieldName = T.pack . escapeF
        , connEscapeTableName = T.pack . escapeE . getEntityDBName
        , connEscapeRawName = T.pack . escapeDBName . T.unpack
        , connNoLimit = "LIMIT 18446744073709551615"
        , connRDBMS = "clickhouse"
        , connLimitOffset = decorateSQLWithLimitOffset "LIMIT 18446744073709551615"
        , connLogFunc = logFunc
        }

instance BackendCompatible SqlBackend ClickhouseBackend where
  projectBackend = clickhouseBackendToSqlBackend

instance HasPersistBackend ClickhouseBackend where
  type BaseBackend ClickhouseBackend = SqlBackend
  persistBackend = clickhouseBackendToSqlBackend

instance PersistStoreRead ClickhouseBackend where
  get k = withBaseBackend $ get k
  getMany ks = withBaseBackend $ getMany ks

instance PersistUniqueRead ClickhouseBackend where
  getBy k = withBaseBackend $ getBy k

instance PersistStoreWrite ClickhouseBackend where
  insert = fmap head . insertMany @ClickhouseBackend . pure

  insertMany recs = do
    (ClickhouseBackend settings _ _) <- ask
    let query = insertManySql t vals
    liftIO . runConduitRes $
      sendSource settings query
        .| sinkNull

    case entityPrimary t of
      Nothing ->
        error "Primary key is not defined "
      Just pdef ->
        let pks = Foldable.toList $ fieldHaskell <$> compositeFields pdef
            getKeyvals fs = map snd $ filter (\(a, _) -> let ret = isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ getEntityFields t) fs
            processKeyvals keyvals = case keyFromValues keyvals of
              Right k -> k
              Left e -> error $ "InsertClickhouseQuery: unexpected keyvals result: " `mappend` T.unpack e
         in pure $ processKeyvals . getKeyvals <$> vals
   where
    vals = mkInsertValues <$> recs
    t = entityDef recs

  insertKey = error "Clickhouse doesn't support insertion by key, please use insert with whole record"

  -- TODO: Think what to do with mutation statements. Currently they are unsupported but must be defined for
  -- PersistStoreWrite. Maybe split the  typeclass in persistent package?
  repsert = error "Repsert functionality is not implemented for Clickhouse as it dangerous to be called"
  replace = error "Replace functionality is not implemented for Clickhouse as it dangerous to be called"
  delete = error "Delete functionality is not implemented for Clickhouse as it dangerous to be called"
  update = error "Update functionality is not implemented for Clickhouse as it dangerous to be called"

instance PersistQueryRead ClickhouseBackend where
  selectSourceRes f opts = withBaseBackend $ selectSourceRes f opts
  selectKeysRes f opts = withBaseBackend $ selectKeysRes f opts
  count f = withBaseBackend $ count f
  exists f = withBaseBackend $ exists f