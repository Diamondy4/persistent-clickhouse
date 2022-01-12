{-# LANGUAGE QuasiQuotes #-}

module Database.Persist.ClickHouse.Internal.Backend where

import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Database.Clickhouse.Types (ClickhouseType)
import Database.Persist
import Database.Persist.ClickHouse.Internal.Conversion
import Database.Persist.ClickHouse.Internal.Misc
import qualified Database.Persist.Sql.Util as Util
import PyF

insertSqlValues' :: EntityDef -> [PersistValue] -> InsertClickhouseQuery
insertSqlValues' ent vals =
  case getEntityId ent of
    EntityIdNaturalKey _ ->
      InsertClickhouseQuery sql vals
    EntityIdField _ ->
      error "Generated ID column not supported, please specify primary key"
  where
    --ISRInsertGet sql "SELECT LAST_INSERT_ID()"
    (fieldNames, placeholders) = unzip (Util.mkInsertPlaceholders ent escapeFT)
    sql =
      [fmt|\
INSERT INTO {tableName} ( {T.intercalate ", " fieldNames} )
VALUES ( {T.intercalate "," placeholders} )
      |]
    tableName = escapeET $ getEntityDBName ent

insertManySql' :: EntityDef -> Vector [PersistValue] -> InsertManyClickhouseQuery
insertManySql' ent vals =
  case getEntityId ent of
    EntityIdNaturalKey _ ->
      InsertManyClickhouseQuery sql vals
    EntityIdField _ ->
      error "Generated ID column not supported, please specify primary key"
  where
    --ISRInsertGet sql "SELECT LAST_INSERT_ID()"
    (fieldNames, placeholderForFields) = unzip (Util.mkInsertPlaceholders ent escapeFT)
    sql =
      [fmt|\
INSERT INTO {tableName} ( {T.intercalate ", " fieldNames} )
VALUES {placeholders}|]
    !rowsCount = V.length vals
    placeholders = T.intercalate "," (replicate rowsCount placeholderText)
    !placeholderText = [fmt|( {T.intercalate "," placeholderForFields} )|]
    !tableName = escapeET $ getEntityDBName ent

rowsPersistValueToClickhouseType :: (Functor f, Functor row) => f (row PersistValue) -> f (row ClickhouseType)
rowsPersistValueToClickhouseType pvs = fmap persistValueToClickhouseType <$> pvs