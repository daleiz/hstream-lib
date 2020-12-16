{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Table
  ( Table (..),
    toStream,
  )
where

import HStream.Encoding
import HStream.Stream.Internal
import RIO
import qualified RIO.Text as T

data Table k v = Table
  { tableKeySerde :: Maybe (Serde k),
    tableValueSerde :: Maybe (Serde v),
    tableProcessorName :: T.Text,
    tableInternalBuilder :: InternalStreamBuilder
  }

toStream ::
  (Typeable k, Typeable v) =>
  Table k v ->
  IO (Stream k v)
toStream Table {..} =
  return $
    mkStream tableKeySerde tableValueSerde tableProcessorName tableInternalBuilder
