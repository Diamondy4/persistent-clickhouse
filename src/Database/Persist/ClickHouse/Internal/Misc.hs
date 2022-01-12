module Database.Persist.ClickHouse.Internal.Misc where

import Data.Text
import qualified Data.Text as T
import Data.Vector
import Database.Persist

data InsertClickhouseQuery = InsertClickhouseQuery !Text ![PersistValue]

data InsertManyClickhouseQuery = InsertManyClickhouseQuery !Text !(Vector [PersistValue])

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