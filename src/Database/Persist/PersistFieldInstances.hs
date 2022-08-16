{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- TODO: Maybe split instances in another package?
module Database.Persist.PersistFieldInstances where

import Data.ByteString.Char8 qualified as BSC
import Data.CaseInsensitive
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import Database.Persist.Sql

instance PersistFieldSql UUID where
  sqlType = const $ SqlOther "UUID"

instance PersistField UUID where
  toPersistValue = PersistLiteral_ DbSpecific . UUID.toASCIIBytes
  fromPersistValue pv =
    case pv of
      PersistLiteral_ DbSpecific bs ->
        maybe
          (Left $ "Error while parsing " <> T.pack (show bs) <> " into UUID")
          Right
          (UUID.fromString . BSC.unpack $ bs)
      _ -> Left $ "Expected PersistLiteral_ DbSpecific, got " <> T.pack (show pv)

instance PersistFieldSql (CI Text) where
  sqlType _ = SqlString

instance PersistField (CI Text) where
  toPersistValue = PersistText . foldedCase
  fromPersistValue pv =
    case pv of
      PersistText text -> Right . mk $ text
      _ -> Left $ "Expected PersistText, got " <> T.pack (show pv)