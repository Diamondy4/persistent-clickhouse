module Database.Persist.ClickHouse.Migrate where

import Conduit (MonadUnliftIO)
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.Functor
import Data.Text (Text)
import Database.Persist.ClickHouse.Internal.Migrate as IM
import Database.Persist.Sql
  ( HasPersistBackend (BaseBackend),
    Migration,
    Sql,
    SqlBackend,
    withBaseBackend,
  )
import qualified Database.Persist.Sql.Migration as PM
import GHC.Stack

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