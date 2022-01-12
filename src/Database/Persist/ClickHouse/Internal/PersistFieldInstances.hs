{-# LANGUAGE OverloadedStrings #-}

module Database.Persist.ClickHouse.Internal.PersistFieldInstances where

import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text as T
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Database.Persist.Sql

instance PersistFieldSql UUID where
  sqlType = const $ SqlOther "UUID"

instance PersistField UUID where
  toPersistValue uuid = PersistLiteral_ DbSpecific . BSC.pack . UUID.toString $ uuid
  fromPersistValue pv =
    case pv of
      PersistLiteral_ DbSpecific bs ->
        maybe
          (Left $ "Error while parsing " <> T.pack (show bs) <> " into UUID")
          Right
          (UUID.fromString . BSC.unpack $ bs)
      _ -> Left $ "Expected PersistLiteral_ DbSpecific, got " <> T.pack (show pv)
