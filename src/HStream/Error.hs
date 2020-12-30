{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Error
  ( HStreamError (..),
  )
where

import RIO

data HStreamError
  = TaskTopologyBuildError Text
  | UnSupportedMessageStoreError Text
  | UnSupportedStateStoreError Text
  | TypeCastError Text
  | UnExpectedStateStoreType Text
  | UnknownError Text
  deriving (Show)

instance Exception HStreamError
