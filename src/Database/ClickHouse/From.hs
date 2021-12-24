{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Database.ClickHouse.From where

import Conduit
import Control.Monad.Error.Class (MonadError)
import Data.ByteString
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as LBS
import Data.Char (ord)
import Data.Csv
import qualified Data.Csv as CSV
import Data.Time
import Data.Time.Zones.All (fromTZName, tzByName)
import Data.UUID
import Data.Vector (Vector)
import qualified Data.Vector as V
import Database.Clickhouse.Client
import Database.Clickhouse.Render (Query (Query))
import Database.Clickhouse.Types
import Debug.Trace
import GHC.Int
import GHC.Word
import Network.HTTP.Req (Scheme (Http), defaultHttpConfig, runReq)

-- | ClickHouse use tab-separated csv.
decOpts :: DecodeOptions
decOpts =
  defaultDecodeOptions
    { decDelimiter = fromIntegral (ord '\t')
    }

decodeToClickhouseType :: ByteString -> (Vector LBS.ByteString, Vector (Vector ClickhouseType))
decodeToClickhouseType bs =
  traceShow bs $
    case decoded of
      Left s -> error "Failed to decode"
      Right vec -> do
        -- First result rows are column names and types must exist.
        if V.length vec < 2
          then error "Failed to decode"
          else
            let colNames = vec V.! 0
                colTypes = vec V.! 1
                colData = V.map (V.map LBS.toStrict) . V.drop 2 $ vec
                converters = V.map (bsToClickhouseType . LBS.toStrict) colTypes
                convertedData = V.map (V.zipWith ($) converters) colData
             in (colNames, convertedData)
  where
    decoded = decodeWith decOpts NoHeader (LBS.fromStrict bs)

bsDropEnd :: Int -> ByteString -> ByteString
bsDropEnd n xs = BS.take (BS.length xs - n) xs

readNullable :: ByteString -> ByteString -> ClickhouseType
readNullable spec bs = if bs == "\\N" then ClickNull else converter bs
  where
    l = BS.length spec
    -- Remove beginning "Nullable(" and trailing ")"
    cktype = BS.drop 9 . bsDropEnd 1 $ spec
    converter = bsToClickhouseType cktype

readString :: ByteString -> ClickhouseType
readString = ClickString

readUUID :: ByteString -> ClickhouseType
readUUID bs = ClickUUID . read @UUID $ C8.unpack bs

-- TODO: Better handling of errors
readDate :: ByteString -> ClickhouseType
readDate bs = ClickDate $ parseTimeOrError True defaultTimeLocale format $ C8.unpack bs
  where
    -- like "2021-12-17"
    format = "%Y-%0m-%0d"

readDateTime :: ByteString -> ByteString -> ClickhouseType
readDateTime spec = readDateTimeUTC
  where
    -- Extract inner time zone from string like DateTime(\'Europe/Moscow\')
    innerSpec = BS.drop 11 . bsDropEnd 3 $ spec
    timeZone = tzByName innerSpec

-- TODO: Handle timezone correctly
readDateTimeUTC :: ByteString -> ClickhouseType
readDateTimeUTC bs = ClickDateTime $ parseTimeOrError @UTCTime True defaultTimeLocale format $ C8.unpack bs
  where
    -- like "2021-12-03 15:03:45.302880179"
    format = "%Y-%0m-%0d %H:%M:%S%Q"

readIntColumn :: ByteString -> ByteString -> ClickhouseType
readIntColumn "Int8" bs = ClickInt8 . read @Int8 $ C8.unpack bs
readIntColumn "Int16" bs = ClickInt16 . read @Int16 $ C8.unpack bs
readIntColumn "Int32" bs = ClickInt32 . read @Int32 $ C8.unpack bs
readIntColumn "Int64" bs = ClickInt64 . read @Int64 $ C8.unpack bs
readIntColumn "UInt8" bs = ClickUInt8 . read @Word8 $ C8.unpack bs
readIntColumn "UInt16" bs = ClickUInt16 . read @Word16 $ C8.unpack bs
readIntColumn "UInt32" bs = ClickUInt32 . read @Word32 $ C8.unpack bs
readIntColumn "UInt64" bs = ClickUInt64 . read @Word64 $ C8.unpack bs
readIntColumn spec bs = error ("expect an integer but got: " ++ show bs ++ " with type " ++ show spec)

readFloatColumn :: ByteString -> ByteString -> ClickhouseType
readFloatColumn "Float32" bs = ClickFloat32 . read @Float $ C8.unpack bs
readFloatColumn "Float64" bs = ClickFloat64 . read @Double $ C8.unpack bs
readFloatColumn spec bs = error ("expect an integer but got: " ++ show bs ++ " with type " ++ show spec)

-- TODO: Cover conversion of all types
bsToClickhouseType :: BS.ByteString -> (ByteString -> ClickhouseType)
bsToClickhouseType spec
  | "DateTime" `isPrefixOf` spec = readDateTime spec
  | "Nullable" `isPrefixOf` spec = readNullable spec
  | "String" `isPrefixOf` spec = readString
  | "Date" `isPrefixOf` spec = readDate
  | "Int" `isPrefixOf` spec = readIntColumn spec
  | "UInt" `isPrefixOf` spec = readIntColumn spec
  | "UUID" `isPrefixOf` spec = readUUID
  | "Float" `isPrefixOf` spec = readFloatColumn spec
  | otherwise = error ("Unknown Type (please implement conversion from bytestring to ClickhouseType): " ++ C8.unpack spec)
