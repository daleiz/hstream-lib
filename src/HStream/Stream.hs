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
    Stream,
    StreamBuilder,
    StreamSourceConfig (..),
    StreamSinkConfig (..),
    GroupedStream,
    Materialized (..),
  )
where

import HStream.Encoding
import HStream.Processor
import HStream.Processor.Internal
import HStream.Stream.GroupedStream
import HStream.Stream.Internal
import HStream.Topic
import RIO
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
  newBuilder <- addSourceInternal sourceCfg sbInternalBuilder
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
  newBuilder <- addSinkInternal sinkCfg [streamProcessorName] streamInternalBuilder
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
  newBuilder <- addProcessorInternal name p [streamProcessorName] streamInternalBuilder
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
  newBuilder <- addProcessorInternal name p [streamProcessorName] streamInternalBuilder
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
  newBuilder <- addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    GroupedStream
      { gsInternalBuilder = newBuilder,
        gsProcessorName = name,
        gsKeySerde = Nothing,
        gsValueSerde = Nothing
      }
