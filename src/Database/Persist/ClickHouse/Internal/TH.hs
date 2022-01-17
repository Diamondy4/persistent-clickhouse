module Database.Persist.ClickHouse.Internal.TH where

import Database.Persist.TH

-- | PersistSettings that disables field prefix autogeneration.
-- Use generics for lens generation.
sqlSettingsNoPrefix :: MkPersistSettings
sqlSettingsNoPrefix = sqlSettings {mpsFieldLabelModifier = ignoreFieldPrefix}
  where
    ignoreFieldPrefix _ field = field