{-# LANGUAGE OverloadedLists #-}

module Database.Persist.ClickHouse.Internal.Conversion where

import Data.Aeson qualified as Aeson
import Data.ByteString.Char8 qualified as BC8
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.Text.Encoding qualified as TE
import Data.UUID qualified as UUID
import Data.Vector qualified as V
import Database.Clickhouse.Client.Types
import Database.Persist
import Foreign (fromBool)
import GHC.Float (float2Double)
import GHC.Real (Ratio ((:%)))

-- | Convert PersistValue to ClickHouseType
persistValueToClickhouseType :: PersistValue -> ClickhouseType
persistValueToClickhouseType pv =
  case pv of
    -- Strings
    PersistText txt -> textToClickText txt
    PersistByteString txt -> ClickString txt
    --Numbers
    PersistInt64 int64 -> ClickInt64 int64
    PersistDouble double -> ClickFloat64 double
    PersistRational (nom :% denom) ->
      toClickTuple (ClickInt128 $ fromIntegral nom) (ClickInt128 $ fromIntegral denom)
    -- Bool
    PersistBool bool -> ClickInt8 $ fromBool bool
    -- Times
    PersistDay day -> ClickDate day
    PersistTimeOfDay tod ->
      -- FIXME: Think how to better represent time of day in ClickHouse
      -- that way it represented as string "10:11:12"
      ClickString $ BSC.pack $ show tod
    PersistUTCTime utcTime -> ClickDateTime utcTime
    -- Containers
    PersistList values -> listToClickArray values
    PersistMap pMap ->
      let entryToTuple (key, value) =
            toClickTuple (textToClickText key) (persistValueToClickhouseType value)
       in ClickArray . V.map entryToTuple $ V.fromList pMap
    PersistArray values -> listToClickArray values
    -- Arbitrary specific data (for types that need special encoding)
    PersistLiteral_ DbSpecific s -> ClickString s
    PersistLiteral_ Unescaped l -> ClickString l
    PersistLiteral_ Escaped e -> ClickString e
    -- Other
    PersistNull -> ClickNullable Nothing
    PersistObjectId _ ->
      -- Value specific for Redis
      error "Refusing to serialize a PersistObjectId to a ClickHouse value"
 where
  toClickTuple x y = ClickTuple [x, y]
  textToClickText = ClickString . TE.encodeUtf8
  listToClickArray values = ClickArray . V.map persistValueToClickhouseType $ V.fromList values

clickhouseTypeToPersistValue :: ClickhouseType -> PersistValue
clickhouseTypeToPersistValue ch =
  case ch of
    -- Integers
    ClickInt8 int -> PersistInt64 $ fromIntegral int
    ClickInt16 int -> PersistInt64 $ fromIntegral int
    ClickInt32 int -> PersistInt64 $ fromIntegral int
    ClickInt64 int -> PersistInt64 $ fromIntegral int
    ClickInt128 int -> PersistInt64 $ fromIntegral int
    -- Unsigned integers
    ClickUInt8 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt16 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt32 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt64 uint -> PersistInt64 $ fromIntegral uint
    ClickUInt128 uint -> PersistInt64 $ fromIntegral uint
    -- Floats/Doubles
    ClickFloat32 flt -> PersistDouble $ float2Double flt
    ClickFloat64 dbl -> PersistDouble dbl
    -- Decimals
    ClickDecimal32 dec -> PersistLiteral_ DbSpecific . BC8.pack $ show dec
    ClickDecimal64 dec -> PersistLiteral_ DbSpecific . BC8.pack $ show dec
    ClickDecimal128 dec -> PersistLiteral_ DbSpecific . BC8.pack $ show dec
    -- Date
    ClickDate day -> PersistDay day
    ClickDateTime dt -> PersistUTCTime dt
    -- IP
    ClickIPv4 ip -> PersistLiteral_ DbSpecific . BC8.pack $ show ip
    ClickIPv6 ip -> PersistLiteral_ DbSpecific . BC8.pack $ show ip
    -- Arrays
    ClickArray arr -> PersistList . V.toList $ clickhouseTypeToPersistValue <$> arr
    ClickTuple tup -> PersistList . V.toList $ clickhouseTypeToPersistValue <$> tup
    -- Other
    ClickUUID uuid -> PersistLiteral_ DbSpecific . UUID.toASCIIBytes $ uuid
    ClickString str -> PersistText $ TE.decodeUtf8 str
    ClickNullable inner -> maybe PersistNull clickhouseTypeToPersistValue inner
    ClickBool b -> PersistBool b
    ClickJSON json -> PersistLiteral_ DbSpecific . BSL.toStrict . Aeson.encode $ json
