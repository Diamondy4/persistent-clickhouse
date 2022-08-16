{-# LANGUAGE QuasiQuotes #-}

module Database.Persist.ClickHouse.Internal.SQL where

import Control.Monad
import Data.Text qualified as T
import Database.Clickhouse.Client.Types (ClickhouseType, Query)
import Database.Clickhouse.Conversion.CSV.Renderer
import Database.Clickhouse.Conversion.Types
import Database.Persist
import Database.Persist.ClickHouse.Internal.Conversion
import Database.Persist.ClickHouse.Internal.Misc
import Database.Persist.Sql
import Database.Persist.Sql.Util qualified as Util
import PyF

insertManySql :: EntityDef -> [[PersistValue]] -> Query
insertManySql ent = renderRows sql . rowsPersistValueToClickhouseType
 where
  --ISRInsertGet sql "SELECT LAST_INSERT_ID()"
  (fieldNames, _placeholderForFields) = unzip (Util.mkInsertPlaceholders ent escapeFT)
  sql =
    CSVQuery
      [fmt|\
INSERT INTO {tableName} ( {T.intercalate ", " fieldNames} )
FORMAT CSV
|]
  !tableName = escapeET $ getEntityDBName ent

rowsPersistValueToClickhouseType :: (Functor f, Functor row) => f (row PersistValue) -> f (row ClickhouseType)
rowsPersistValueToClickhouseType pvs = fmap persistValueToClickhouseType <$> pvs

-- | Fallback for SqlBackend
insertManySql' :: EntityDef -> [[PersistValue]] -> InsertSqlResult
insertManySql' ent vals = ISRManyKeys sql (join vals)
 where
  --ISRInsertGet sql "SELECT LAST_INSERT_ID()"
  (fieldNames, placeholderForFields) = unzip (Util.mkInsertPlaceholders ent escapeFT)
  sql =
    [fmt|\
INSERT INTO {tableName} ( {T.intercalate ", " fieldNames} )
VALUES {placeholders}|]
  !rowsCount = length vals
  placeholders = T.intercalate "," (replicate rowsCount placeholderText)
  !placeholderText = [fmt|( {T.intercalate "," placeholderForFields} )|]
  !tableName = escapeET $ getEntityDBName ent