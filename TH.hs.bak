{-# LANGUAGE OverloadedStrings #-}

module Database.Persist.ClickHouse.TH where

import Data.Text (Text)
import Database.Persist.Quasi
import Database.Persist.Quasi.Internal
import Database.Persist.TH
import Language.Haskell.TH
import Language.Haskell.TH.Quote
import Language.Haskell.TH.Syntax (lift)

--persistWithSchema :: PersistSettings -> Text -> Q Exp
persistWithSchema :: PersistSettings -> Text -> String -> Q Exp
persistWithSchema settings schema = quoteExp $ persistWith $ settings {psToDBName = psToDBNameNew}
  where
    psToDBNameOld = psToDBName settings
    psToDBNameNew ps = schema <> "." <> psToDBNameOld ps

--persistLowerCaseWithSchema :: Text -> QuasiQuoter
persistLowerCaseWithSchema :: Text -> String -> Q Exp
persistLowerCaseWithSchema = persistWithSchema lowerCaseSettings

persistSchema :: QuasiQuoter
persistSchema =
  QuasiQuoter
    { quoteExp = lift,
      quotePat = error "persistSchema can't be used as pattern",
      quoteType = error "persistSchema can't be used as pattern",
      quoteDec = error "persistSchema can't be used as pattern"
    }