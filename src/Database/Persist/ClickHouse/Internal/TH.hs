{-# LANGUAGE TemplateHaskell #-}

module Database.Persist.ClickHouse.Internal.TH where

import Database.Clickhouse.Client.HTTP.Client
import Database.Persist.ClickHouse.Internal.Backend
import Database.Persist.TH
import Language.Haskell.TH

type ClickhouseHTTPBackend = ClickhouseBackend ClientHTTP

-- | PersistSettings for Clickhouse HTTP backend. Disables field prefix autogeneration. 
-- Use generics for lens generation.
clickhouseSettings :: MkPersistSettings
clickhouseSettings = def {mpsPrefixFields = False}
  where
    def = mkPersistSettings $ ConT ''ClickhouseHTTPBackend