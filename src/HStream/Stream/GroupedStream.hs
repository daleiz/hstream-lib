{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Stream.GroupedStream
  ( GroupedStream (..),
    aggregate,
    count,
    timeWindowedBy,
  )
where

import Data.Maybe
import HStream.Encoding
import HStream.Processor
import HStream.Store
import HStream.Stream.Internal
import HStream.Stream.TimeWindowedStream (TimeWindowedStream (..))
import HStream.Stream.TimeWindows
import HStream.Table
import RIO
import qualified RIO.Text as T

data GroupedStream k v = GroupedStream
  { gsKeySerde :: Maybe (Serde k),
    gsValueSerde :: Maybe (Serde v),
    gsProcessorName :: T.Text,
    gsInternalBuilder :: InternalStreamBuilder
  }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a, KVStore s) =>
  a ->
  (a -> Record k v -> a) ->
  Materialized k a s ->
  GroupedStream k v ->
  IO (Table k a)
aggregate initialValue aggF Materialized {..} GroupedStream {..} = do
  processorName <- mkInternalProcessorName "STREAM-AGGREGATE-" gsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF mKeySerde mValueSerde
  builder' <- addProcessorInternal processorName p [gsProcessorName] gsInternalBuilder
  newBuilder <- addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = Just mKeySerde,
        tableValueSerde = Just mValueSerde
      }

count ::
  (Typeable k, Typeable v, Ord k, KVStore s) =>
  Materialized k Int s ->
  GroupedStream k v ->
  IO (Table k Int)
count materialized groupedStream = aggregate 0 aggF materialized groupedStream
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  Serde k ->
  Serde a ->
  Processor k v
aggregateProcessor storeName initialValue aggF keySerde accSerde = Processor $ \r -> do
  store <- getStateStore storeName
  let key = runSer (serializer keySerde) (fromJust $ recordKey r)
  ma <- liftIO $ ksGet key store
  let acc = maybe initialValue (runDeser $ deserializer accSerde) ma
  let newAcc = aggF acc r
  let sNewAcc = runSer (serializer accSerde) newAcc
  liftIO $ ksPut key sNewAcc store
  forward r {recordValue = newAcc}

timeWindowedBy ::
  (Typeable k, Typeable v) =>
  TimeWindows ->
  GroupedStream k v ->
  IO (TimeWindowedStream k v)
timeWindowedBy timeWindows GroupedStream {..} =
  return $
    TimeWindowedStream
      { twsKeySerde = gsKeySerde,
        twsValueSerde = gsValueSerde,
        twsProcessorName = gsProcessorName,
        twsTimeWindows = timeWindows,
        twsInternalBuilder = gsInternalBuilder
      }
