{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Database.Persist.ClickHouse.Migrate where

import Conduit
import Control.Monad
import Control.Monad.Reader
import qualified Data.Conduit.List as CL
import Data.Either (partitionEithers)
import qualified Data.List as List
import qualified Data.List.NonEmpty as NEL
import qualified Data.Map as Map
import Data.Text (Text)
import qualified Data.Text as T
import Database.Clickhouse.Types
import Database.Persist.Sql
import qualified Database.Persist.Sql.Util as Util
import GHC.Stack (HasCallStack)
import Optics (Magnify (magnify), Zoom (zoom), (%), (&), (<&>), (^.))
import Optics.Generic
import PyF

type WithClickhouseEnvT m a = ReaderT ClickhouseEnv m a

type WithClickhouseEnv a = Reader ClickhouseEnv a

data SchemaStatus = Matched | Unmatched [Sql]
  deriving (Show)

type SafeToRemove = Bool

data AlterColumn
  = ChangeType Column SqlType Text
  | IsNull Column
  | NotNull Column
  | Add' Column
  | Drop Column SafeToRemove
  | Update' Column Text
  deriving (Show)

data AlterDB
  = AddTable Text
  | AlterColumn EntityNameDB AlterColumn
  deriving (Show)

doesTableExist ::
  (Text -> IO Statement) ->
  EntityNameDB ->
  WithClickhouseEnvT IO Bool
doesTableExist getter (EntityNameDB name) = do
  dbScheme <- asks (^. #dbScheme)
  let sql = "SELECT count(*) FROM system.tables WHERE database = ? AND name = ?"
      vals =
        [ PersistText dbScheme,
          PersistText name
        ]
  stmt <- liftIO $ getter sql
  withAcquire (stmtQuery stmt vals) (\src -> runConduit $ src .| start)
  where
    start = await >>= maybe (error "No results when checking doesTableExist") start'
    start' [PersistInt64 0] = finish False
    start' [PersistInt64 1] = finish True
    start' res = error $ "doesTableExist returned unexpected result: " ++ show res
    finish x = await >>= maybe (return x) (error "Too many rows returned in doesTableExist")

checkSchema :: (HasCallStack, MonadIO m) => Migration -> ReaderT SqlBackend m SchemaStatus
checkSchema mig =
  getMigration mig <&> \case
    [] -> Matched
    migrations -> Unmatched migrations

migrate' ::
  ClickhouseEnv ->
  [EntityDef] ->
  (Text -> IO Statement) ->
  EntityDef ->
  IO (Either [Text] [(Bool, Text)])
migrate' env@ClickhouseEnv {..} allDefs getter entity = (fmap . fmap . fmap $ showAlterDb settings) . (`runReaderT` env) $ do
  liveColumns <- getColumns getter entity newcols'
  case partitionEithers liveColumns of
    -- Live table columns successfully parsed
    ([], old') -> do
      exists' <-
        if null liveColumns
          then -- Check if table exists without columns
            doesTableExist getter name
          else -- Table exists
            return True
      return . Right $ migrationText exists' old'
    (errs, _) -> return $ Left errs
  where
    name = getEntityDBName entity
    newcols' = chMkColumns allDefs entity
    migrationText exists' old'
      -- Create new tabe if not exists
      | not exists' = createText newcols
      -- Modify existing table if exists
      | otherwise =
        let acs = getAlters allDefs entity newcols old'
         in map (AlterColumn name) acs
      where
        newcols = filter (not . safeToRemove entity . cName) newcols'
    createText newcols = pure $ addTable newcols entity

addTable :: [Column] -> EntityDef -> AlterDB
addTable cols entity =
  AddTable
    [fmt|\
CREATE TABLE {dbTableName}
( {primaryKeyTxt} {T.intercalate "," $ map showColumn nonIdCols} )
ENGINE = MergeTree
|]
  where
    dbTableName = T.pack (escapeE $ getEntityDBName entity)
    nonIdCols =
      case entityPrimary entity of
        Just _ -> cols
        _ -> filter keepField cols
      where
        keepField c =
          Just (cName c) /= fmap fieldDB (getEntityIdField entity)
            && not (safeToRemove entity (cName c))
    primaryKeyTxt :: Text =
      case getEntityId entity of
        EntityIdNaturalKey pdef ->
          let definedPrimKeyTxt = T.intercalate "," $ T.pack . escapeF . fieldDB <$> NEL.toList (compositeFields pdef)
           in [fmt|PRIMARY KEY ( {definedPrimKeyTxt} ),|]
        EntityIdField field -> error "Surrogate keys not supported. Please defined primary key fields."

safeToRemove :: EntityDef -> FieldNameDB -> Bool
safeToRemove def (FieldNameDB colName) =
  any (elem FieldAttrSafeToRemove . fieldAttrs) $
    filter ((== FieldNameDB colName) . fieldDB) allEntityFields
  where
    allEntityFields =
      getEntityFieldsDatabase def <> case getEntityId def of
        EntityIdField fdef ->
          [fdef]
        _ ->
          []

getAlters ::
  [EntityDef] ->
  EntityDef ->
  [Column] ->
  [Column] ->
  [AlterColumn]
getAlters defs def = getAltersC
  where
    getAltersC [] old =
      map (\x -> Drop x $ safeToRemove def $ cName x) old
    getAltersC (new : news) old =
      let (alters, old') = findAlters defs def new old
       in alters ++ getAltersC news old'

findAlters ::
  -- | The list of all entity definitions that persistent is aware of.
  [EntityDef] ->
  -- | The entity definition for the entity that we're working on.
  EntityDef ->
  -- | The column that we're searching for potential alterations for.
  Column ->
  [Column] ->
  ([AlterColumn], [Column])
findAlters defs edef col@(Column name isNull sqltype def _ _ _ _) cols =
  case List.find (\c -> cName c == name) cols of
    Nothing ->
      ([Add' col], cols)
    Just (Column _oldName isNull' sqltype' _ _ _ _ _) ->
      let modNull = case (isNull, isNull') of
            (True, False) -> do
              guard $ Just name /= fmap fieldDB (getEntityIdField edef)
              pure (IsNull col)
            (False, True) ->
              let up = case def of
                    Nothing -> id
                    Just s -> (:) (Update' col s)
               in up [NotNull col]
            _ -> []
          modType
            | sqlTypeEq sqltype sqltype' = []
            | otherwise = [ChangeType col sqltype ""]
          dropSafe =
            if safeToRemove edef name
              then error "wtf" ([Drop col True] :: [AlterColumn])
              else []
       in ( modNull ++ modType ++ dropSafe,
            filter (\c -> cName c /= name) cols
          )

getColumns ::
  HasCallStack =>
  (Text -> IO Statement) ->
  EntityDef ->
  [Column] ->
  WithClickhouseEnvT IO [Either Text Column]
getColumns getter def cols = do
  -- Find out all columns.
  stmtClmns <-
    liftIO . getter $
      [fmt|\
SELECT name AS column_name,
if(startsWith(type, 'Nullable'), substring(type, 10, length(type) - 10), type) AS column_type,
startsWith(type, 'Nullable') AS is_nullable,
is_in_primary_key AS primary_key,
numeric_precision,
numeric_scale
FROM system.columns
WHERE database = ?
AND table = ?;|]
  dbScheme <- asks (^. #dbScheme)
  let params =
        [ PersistText dbScheme,
          PersistText $ unEntityNameDB $ getEntityDBName def
        ]
  dbColumns <- withAcquire (stmtQuery stmtClmns params) (\src -> runConduitRes $ src .| CL.consume)
  return $ getColumn <$> dbColumns

-- | Get the information about a column in a table.
getColumn ::
  HasCallStack =>
  [PersistValue] ->
  Either Text Column
getColumn
  [ PersistText cname,
    PersistText colType,
    PersistInt64 null_,
    colPrecision,
    colScale,
    primKey
    ] = do
    return
      Column
        { cName = FieldNameDB cname,
          cNull = null_ == 1,
          cSqlType = textToSqlType colType,
          cDefault = Nothing,
          cGenerated = Nothing,
          cDefaultConstraintName = Nothing,
          cMaxLen = Nothing,
          cReference = Nothing
        }
getColumn x =
  Left $ T.pack $ "Invalid result from SYSTEM schema: " ++ show x

-- TODO: Cover all types
textToSqlType :: Text -> SqlType
textToSqlType "Int32" = SqlInt32
textToSqlType "Int64" = SqlInt64
textToSqlType "String" = SqlString
textToSqlType "Date" = SqlDay
textToSqlType "Bool" = SqlBool
textToSqlType "Float64" = SqlReal
textToSqlType "DateTime" = SqlTime
textToSqlType a
  | "DateTime" `T.isPrefixOf` a = SqlTime
  | otherwise = SqlOther a

--- | Create column definitions from EntityDef
-- Clickhouse doenst have Unique constraints or foreign keys
chMkColumns :: [EntityDef] -> EntityDef -> [Column]
chMkColumns allDefs t = let (columns, _, _) = mkColumns allDefs t emptyBackendSpecificOverrides in columns

showColumn :: Column -> Text
showColumn (Column n isNull sqlType' def gen _defConstraintName _maxLen _ref) =
  [fmt|\
{colName} {colType} {colDefault}
  |]
  where
    colName = T.pack $ escapeF n
    colBaseType = showSqlType sqlType'
    colType :: Text =
      if isNull
        then "Nullable(" <> colType <> ")"
        else colBaseType
    colDefault =
      case def of
        Nothing -> ""
        Just s -> " DEFAULT " <> s

-- | Persistent sql builder's SqlType to Clickhouse type (as in clickhouse)
showSqlType :: SqlType -> Text
showSqlType (SqlOther t) = t
showSqlType SqlString = "String"
showSqlType SqlInt32 = "Int32"
showSqlType SqlInt64 = "Int64"
showSqlType SqlReal = "Float64"
showSqlType (SqlNumeric s prec) = [fmt|Decimal( {(show s)}, {(show prec)} )|]
showSqlType SqlDay = "Date"
showSqlType SqlDayTime = "DateTime"
showSqlType SqlTime = "DateTime"
showSqlType SqlBool = "UInt8"
showSqlType SqlBlob = "String"

showAlterDb :: ClickhouseSettings -> AlterDB -> (Bool, Text)
showAlterDb settings (AddTable s) = (False, s)
showAlterDb settings (AlterColumn t ac) =
  (isUnsafe ac, showAlter settings t ac)
  where
    isUnsafe Drop {} = True
    isUnsafe _ = False

showAlter :: ClickhouseSettings -> EntityNameDB -> AlterColumn -> Text
showAlter settings table alterType = case alterType of
  (ChangeType c t extra) ->
    [fmt|\
ALTER TABLE {dbTableName}
ALTER COLUMN {escapeF (cName c)} TYPE {showSqlType t}{extra}
    |]
  (IsNull c) ->
    [fmt|\
ALTER TABLE {dbTableName}
ALTER COLUMN {escapeF (cName c)} TYPE Nullable( {showSqlType . cSqlType $ c} )
    |]
  (NotNull c) ->
    [fmt|\
ALTER TABLE {dbTableName}
ALTER COLUMN {escapeF (cName c)} TYPE {showSqlType . cSqlType $ c}
    |]
  (Add' col) ->
    [fmt|\
ALTER TABLE {dbTableName}
ADD COLUMN {showColumn col}
    |]
  (Drop c _) ->
    [fmt|\
ALTER TABLE {dbTableName}
DROP COLUMN {escapeF (cName c)}
    |]
  (Update' c s) ->
    [fmt|\
ALTER TABLE {dbTableName}
UPDATE {escapeF (cName c)} = {s} WHERE {escapeF (cName c)} IS NULL
    |]
  where
    dbTableName = escapeE table

sqlTypeEq :: SqlType -> SqlType -> Bool
sqlTypeEq x y =
  T.toCaseFold (showSqlType x) == T.toCaseFold (showSqlType y)

escapeC :: ConstraintNameDB -> String
escapeC = escapeWith (escapeDBName . T.unpack)

escapeE :: EntityNameDB -> String
escapeE = escapeWith (escapeDBName . T.unpack)

escapeF :: FieldNameDB -> String
escapeF = escapeWith (escapeDBName . T.unpack)

escapeET :: EntityNameDB -> Text
escapeET = escapeWith (T.pack . escapeDBName . T.unpack)

escapeFT :: FieldNameDB -> Text
escapeFT = escapeWith (T.pack . escapeDBName . T.unpack)

escapeDBNameT :: Text -> Text
escapeDBNameT = T.pack . escapeDBName . T.unpack

escapeDBName :: String -> String
escapeDBName str = '"' : go str
  where
    go ('"' : xs) = '"' : '"' : go xs
    go (x : xs) = x : go xs
    go "" = "\""
