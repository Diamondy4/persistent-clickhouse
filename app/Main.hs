{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Main where

import Control.Monad.IO.Class (liftIO)
import Data.Maybe
import Data.Text (Text)
import Data.Time.Calendar
import Data.Time.Clock
import Data.UUID
import Database.Clickhouse.Types
import Database.Persist
import Database.Persist.ClickHouse
import Database.Persist.ClickHouse.Migrate
import Database.Persist.ClickHouse.PersistField
import Database.Persist.ClickHouse.TH
import Database.Persist.Sql
import Database.Persist.TH
import Network.HTTP.Req (Req, Scheme (Http))
import Text.Pretty.Simple

{- share
  [mkPersist sqlSettings, mkMigrate "migrateAll"]
  [persistLowerCase|
Person
  name String
  age Int
  iid UUID Maybe
  iida UUID
  Primary name iida
  deriving Show
|] -}

share
  [mkPersist sqlSettings, mkMigrate "migrateAll"]
  [persistLowerCase|
Avod_events_v2
  client_id String
  nsc_sid String Maybe
  event_id UUID
  event_type String
  date Day
  time UTCTime
  ad_id String Maybe
  track_url String Maybe
  track_title String Maybe
  contractor_id String Maybe
  content_owner String Maybe
  content_title String Maybe
  content_category String Maybe
  content_is_child Bool Maybe
  ad_position String Maybe
  ad_place_type String Maybe
  clientLocation String Maybe
  client_san String Maybe
  client_uid String Maybe
  client_host String Maybe
  client_user_agent String Maybe
  client_xforwarded_for String Maybe
  client_xforwarded_proto String Maybe
  client_xreal_ip String Maybe
  client_version String Maybe
  client_device_type String Maybe
  client_broadcast_type String Maybe
  client_web_client_type String Maybe
  ad_source String Maybe
  ad_owner String Maybe
  alive_channel_id String Maybe
  alive_channel_name String Maybe
  alive_channel_number String Maybe
  alive_epg_id String Maybe
  alive_epg_name String Maybe
  alive_epg_genre String Maybe
  adfox_puid9_channel String Maybe
  Primary date time
  deriving Show
|]

main :: IO ()
main = runClickhouse chEnv $ do
  check <- checkSchema migrateAll
  liftIO $ print check

  liftIO $ print "\n\n"

  records <- selectList @Avod_events_v2 [] []
  liftIO $ pPrint  $ entityVal <$> records

--runMigration migrateAll
{- insertMany
  [ Person "John Doe" (Just 35) (fromJust $ fromString "55a8fd90-a267-4aa3-84c1-64cfdea51e33"),
    Person "Somebody" Nothing (fromJust $ fromString "a308be65-d433-40be-94aa-625fda7b2dc0")
  ]
users <- selectList @Person [] []
liftIO $ print users -}

{- main :: IO ()
main = return () -}

chEnv :: ClickhouseEnv
chEnv =
  ClickhouseEnv
    { settings =
        ClickhouseSettings
          { scheme = Http,
            username = "default",
            host = "localhost",
            port = 8123,
            password = "password"
          },
      dbScheme = "getshoptv"
    }