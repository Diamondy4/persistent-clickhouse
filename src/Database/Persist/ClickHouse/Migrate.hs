{-# LANGUAGE PartialTypeSignatures #-}
module Database.Persist.ClickHouse.Migrate
  ( module Database.Persist.ClickHouse.Migrate
  , SchemaStatus
  ) where

import           Conduit                        ( MonadUnliftIO )
import           Control.Monad.IO.Class
import           Control.Monad.Reader
import           Data.Maybe                     ( catMaybes )
import           Data.Text                      ( Text )
import           Data.Traversable
import qualified Database.Persist.ClickHouse.Internal.Migrate
                                               as IM
import           Database.Persist.ClickHouse.Internal.Migrate
                                                ( SchemaStatus(..)
                                                , UnmatchedSchema
                                                )
import           Database.Persist.Sql           ( HasPersistBackend(..)
                                                , Migration
                                                , Sql
                                                , SqlBackend
                                                , withBaseBackend
                                                )
import qualified Database.Persist.Sql.Migration
                                               as PM

getUnmatchedSchemas ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  [Migration] ->
  ReaderT backend m [UnmatchedSchema]
getUnmatchedSchemas migs = fmap catMaybes $ for migs $ \mig -> do
  schemaStatus <- checkSchema mig
  pure $ check schemaStatus
 where
  check (Unmatched schema) = Just schema
  check Matched            = Nothing

checkSchema ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m SchemaStatus
checkSchema mig = withBaseBackend $ IM.checkSchema mig

runMigration ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m ()
runMigration mig = withBaseBackend $ PM.runMigration mig

showMigration ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m ()
showMigration mig = withBaseBackend $ PM.runMigration mig

parseMigration ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m (Either [Text] PM.CautiousMigration)
parseMigration mig = withBaseBackend $ PM.parseMigration mig

parseMigration' ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m PM.CautiousMigration
parseMigration' mig = withBaseBackend $ PM.parseMigration' mig

printMigration ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m ()
printMigration mig = withBaseBackend $ PM.printMigration mig

getMigration ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m [Sql]
getMigration mig = withBaseBackend $ PM.getMigration mig

runMigrationQuiet ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m [Text]
runMigrationQuiet mig = withBaseBackend $ PM.runMigrationQuiet mig

runMigrationSilent ::
  ( HasPersistBackend backend,
    MonadIO m,
    MonadUnliftIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m [Text]
runMigrationSilent mig = withBaseBackend $ PM.runMigrationSilent mig

runMigrationUnsafe ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m ()
runMigrationUnsafe mig = withBaseBackend $ PM.runMigrationUnsafe mig

runMigrationUnsafeQuiet ::
  ( HasPersistBackend backend,
    MonadIO m,
    BaseBackend backend ~ SqlBackend
  ) =>
  Migration ->
  ReaderT backend m [Text]
runMigrationUnsafeQuiet mig = withBaseBackend $ PM.runMigrationUnsafeQuiet mig
