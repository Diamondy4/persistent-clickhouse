{-# LANGUAGE OverloadedLists #-}

module Database.Persist.ClickHouse.Conversion where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text.Encoding as TE
import qualified Data.UUID as UUID
import qualified Data.Vector as V
import Database.Clickhouse.Types
import Database.Persist
import Foreign (fromBool)
import GHC.Real (Ratio ((:%)))

-- | Convert PersistValue to ClickHouseType
persistValueToClickhouseType :: PersistValue -> ClickhouseType
persistValueToClickhouseType pv =
  case pv of
    PersistText txt -> textToClickText txt
    PersistByteString txt -> ClickString txt
    PersistInt64 int64 -> ClickInt64 int64
    PersistDouble double -> ClickDecimal64 double
    PersistRational (nom :% denom) ->
      toClickTuple (ClickInt128 $ fromIntegral nom) (ClickInt128 $ fromIntegral denom)
    PersistBool bool -> ClickInt8 $ fromBool bool
    PersistDay day -> ClickDate day
    PersistTimeOfDay tod -> error "Not implemented" -- TODO: Think how to represent time of day
    PersistUTCTime utcTime -> ClickDateTime utcTime
    PersistNull -> ClickNull
    PersistList values -> listToClickArray values
    PersistMap map ->
      let entryToTuple (key, value) =
            toClickTuple (textToClickText key) (persistValueToClickhouseType value)
       in ClickArray . V.map entryToTuple $ V.fromList map
    PersistArray values -> listToClickArray values
    PersistLiteral_ DbSpecific s -> ClickString s
    PersistLiteral_ Unescaped l -> ClickString l
    PersistLiteral_ Escaped e -> ClickString e
    PersistObjectId _ -> error "Refusing to serialize a PersistObjectId to a ClickHouse value"
  where
    toClickTuple x y = ClickTuple [x, y]
    textToClickText txt = ClickString $ TE.encodeUtf8 txt
    listToClickArray values = ClickArray . V.map persistValueToClickhouseType $ V.fromList values

-- TODO: Cover conversion of all types
clickhouseTypeToPersistValue :: ClickhouseType -> PersistValue
clickhouseTypeToPersistValue ch =
  case ch of
    ClickInt8 int -> PersistInt64 $ fromIntegral int
    ClickInt16 int -> PersistInt64 $ fromIntegral int
    ClickInt32 int -> PersistInt64 $ fromIntegral int
    ClickInt64 int -> PersistInt64 $ fromIntegral int
    ClickInt128 int -> PersistInt64 $ fromIntegral int
    ClickUInt8 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt16 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt32 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt64 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt128 uint -> PersistInt64 $ fromIntegral uint
    ClickString str -> PersistText $ TE.decodeUtf8 str
    ClickFloat32 flt -> PersistDouble $ realToFrac flt
    ClickFloat64 dbl -> PersistDouble dbl
    ClickDate day -> PersistDay day
    ClickDateTime dt -> PersistUTCTime dt
    ClickUUID uuid -> PersistLiteral_ DbSpecific $ BSC.pack . UUID.toString $ uuid
    ClickNull -> PersistNull
    _ -> error ("Not implemented for: " ++ show ch)