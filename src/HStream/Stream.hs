{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Stream
  ( mkStreamBuilder,
    mkStream,
    HStream.Stream.stream,
    HStream.Stream.build,
    HStream.Stream.to,
    HStream.Stream.filter,
    HStream.Stream.map,
    HStream.Stream.groupBy,
    HStream.Stream.joinStream,
    Stream,
    StreamBuilder,
    StreamSourceConfig (..),
    StreamSinkConfig (..),
    GroupedStream,
    Materialized (..),
    StreamJoined (..),
  )
where

import Data.Maybe
import HStream.Encoding
import HStream.Processor
import HStream.Processor.Internal
import HStream.Store
import HStream.Stream.GroupedStream
import HStream.Stream.Internal
import HStream.Stream.JoinWindows
import HStream.Topic
import HStream.Type
import RIO
import qualified RIO.ByteString.Lazy as BL
import qualified RIO.Text as T

data StreamBuilder = StreamBuilder
  { sbInternalBuilder :: InternalStreamBuilder
  }

mkStreamBuilder :: T.Text -> IO StreamBuilder
mkStreamBuilder taskName = do
  internalStreamBuilder <- mkInternalStreamBuilder $ buildTask taskName
  return StreamBuilder {sbInternalBuilder = internalStreamBuilder}

data StreamSourceConfig k v = StreamSourceConfig
  { sscTopicName :: TopicName,
    sscKeySerde :: Serde k,
    sscValueSerde :: Serde v
  }

data StreamSinkConfig k v = StreamSinkConfig
  { sicTopicName :: TopicName,
    sicKeySerde :: Serde k,
    sicValueSerde :: Serde v
  }

stream ::
  (Typeable k, Typeable v) =>
  StreamSourceConfig k v ->
  StreamBuilder ->
  IO (Stream k v)
stream StreamSourceConfig {..} StreamBuilder {..} = do
  sourceProcessorName <- mkInternalProcessorName (sscTopicName `T.append` "-SOURCE-") sbInternalBuilder
  let sourceCfg =
        SourceConfig
          { sourceName = sourceProcessorName,
            sourceTopicName = sscTopicName,
            keyDeserializer = Just $ deserializer sscKeySerde,
            valueDeserializer = deserializer sscValueSerde
          }
  let newBuilder = addSourceInternal sourceCfg sbInternalBuilder
  return
    Stream
      { streamKeySerde = Just sscKeySerde,
        streamValueSerde = Just sscValueSerde,
        streamProcessorName = sourceProcessorName,
        streamInternalBuilder = newBuilder
      }

to ::
  (Typeable k, Typeable v) =>
  StreamSinkConfig k v ->
  Stream k v ->
  IO StreamBuilder
to StreamSinkConfig {..} Stream {..} = do
  sinkProcessorName <- mkInternalProcessorName (sicTopicName `T.append` "-SINK-") streamInternalBuilder
  let sinkCfg =
        SinkConfig
          { sinkName = sinkProcessorName,
            sinkTopicName = sicTopicName,
            keySerializer = Just $ serializer sicKeySerde,
            valueSerializer = serializer sicValueSerde
          }
  let newBuilder = addSinkInternal sinkCfg [streamProcessorName] streamInternalBuilder
  return $ StreamBuilder {sbInternalBuilder = newBuilder}

build :: StreamBuilder -> Task
build StreamBuilder {..} = buildInternal sbInternalBuilder

filter ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Stream k v ->
  IO (Stream k v)
filter f s@Stream {..} = do
  name <- mkInternalProcessorName "FILTER-" streamInternalBuilder
  let p = filterProcessor f
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    s
      { streamInternalBuilder = newBuilder,
        streamProcessorName = name
      }

filterProcessor ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Processor k v
filterProcessor f = Processor $ \r ->
  when (f r) $ forward r

mapProcessor ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2) =>
  (Record k1 v1 -> Record k2 v2) ->
  Processor k1 v1
mapProcessor f = Processor $ forward . f

map ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2) =>
  (Record k1 v1 -> Record k2 v2) ->
  Stream k1 v1 ->
  IO (Stream k2 v2)
map f s@Stream {..} = do
  name <- mkInternalProcessorName "MAP-" streamInternalBuilder
  let p = mapProcessor f
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    s
      { streamInternalBuilder = newBuilder,
        streamProcessorName = name,
        streamKeySerde = Nothing,
        streamValueSerde = Nothing
      }

groupBy ::
  (Typeable k1, Typeable v1, Typeable k2) =>
  (Record k1 v1 -> k2) ->
  Stream k1 v1 ->
  IO (GroupedStream k2 v1)
groupBy f Stream {..} = do
  name <- mkInternalProcessorName "GROUP-BY-" streamInternalBuilder
  let p = mapProcessor (\r -> r {recordKey = Just $ f r})
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    GroupedStream
      { gsInternalBuilder = newBuilder,
        gsProcessorName = name,
        gsKeySerde = Nothing,
        gsValueSerde = Nothing
      }

data StreamJoined k v1 v2 = StreamJoined
  { sjKeySerde :: Serde k,
    sjV1Serde :: Serde v1,
    sjV2Serde :: Serde v2,
    sjThisStore :: StateStore BL.ByteString BL.ByteString,
    sjOtherStore :: StateStore BL.ByteString BL.ByteString
  }

joinStream ::
  (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
  Stream k v2 ->
  (v1 -> v2 -> v3) ->
  JoinWindows ->
  StreamJoined k v1 v2 ->
  Stream k v1 ->
  IO (Stream k v3)
joinStream otherStream joiner JoinWindows {..} StreamJoined {..} thisStream = do
  let mergedStreamBuilder = mergeInternalStreamBuilder (streamInternalBuilder thisStream) (streamInternalBuilder otherStream)
  thisJoinProcessorName <- mkInternalProcessorName "STREAM-JOIN-STREAM-THIS-" mergedStreamBuilder
  let thisJoinStoreName = mkInternalStoreName thisJoinProcessorName
  otherJoinProcessorName <- mkInternalProcessorName "STREAM-JOIN-STREAM-Other-" mergedStreamBuilder
  let otherJoinStoreName = mkInternalStoreName otherJoinProcessorName

  -- ts1 - beforeMs <= ts2 <= ts1 + afterMs
  -- ts2 - afterMs <= ts1 <= ts2 + beforeMs
  let thisJoinProcessor = joinStreamProcessor joiner jwBeforeMs jwAfterMs thisJoinStoreName otherJoinStoreName sjKeySerde sjV1Serde sjV2Serde
  let otherJoinProcessor = joinStreamProcessor (flip joiner) jwAfterMs jwBeforeMs otherJoinStoreName thisJoinStoreName sjKeySerde sjV2Serde sjV1Serde
  mergeProcessorName <- mkInternalProcessorName "PASSTHROUGH-" mergedStreamBuilder
  let mergeProcessor = passThroughProcessor thisStream joiner

  let newTaskBuilder =
        isbTaskBuilder mergedStreamBuilder
          <> addProcessor thisJoinProcessorName thisJoinProcessor [streamProcessorName thisStream]
          <> addProcessor otherJoinProcessorName otherJoinProcessor [streamProcessorName otherStream]
          <> addProcessor mergeProcessorName mergeProcessor [thisJoinProcessorName, otherJoinProcessorName]
          <> addStateStore thisJoinStoreName sjThisStore [thisJoinProcessorName, otherJoinProcessorName]
          <> addStateStore otherJoinStoreName sjOtherStore [thisJoinProcessorName, otherJoinProcessorName]

  return
    Stream
      { streamKeySerde = Just sjKeySerde,
        streamValueSerde = Nothing,
        streamProcessorName = mergeProcessorName,
        streamInternalBuilder = mergedStreamBuilder {isbTaskBuilder = newTaskBuilder}
      }
  where
    passThroughProcessor ::
      (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
      Stream k v1 ->
      (v1 -> v2 -> v3) ->
      Processor k v3
    passThroughProcessor _ _ = Processor $ \r ->
      forward r

joinStreamProcessor ::
  (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
  (v1 -> v2 -> v3) ->
  Int64 ->
  Int64 ->
  Text ->
  Text ->
  Serde k ->
  Serde v1 ->
  Serde v2 ->
  Processor k v1
joinStreamProcessor joiner beforeMs afterMs storeName1 storeName2 keySerde v1Serde v2Serde = Processor $ \r@Record {..} -> do
  store1 <- getTimestampedKVStateStore storeName1
  let key = fromJust recordKey
  let keyBytes = runSer (serializer keySerde) key
  let v1Bytes = runSer (serializer v1Serde) recordValue
  liftIO $ tksPut (mkTimestampedKey keyBytes recordTimestamp) v1Bytes store1

  store2 <- getTimestampedKVStateStore storeName2
  candinates <- liftIO $ tksRange (mkTimestampedKey keyBytes $ recordTimestamp - beforeMs) (mkTimestampedKey keyBytes $ recordTimestamp + afterMs) store2
  forM_
    candinates
    ( \(timestampedKey, v2Bytes) -> do
        let v2 = runDeser (deserializer v2Serde) v2Bytes
        let v3 = joiner recordValue v2
        let ts2 = tkTimestamp timestampedKey
        forward $ r {recordValue = v3, recordTimestamp = max recordTimestamp ts2}
    )
