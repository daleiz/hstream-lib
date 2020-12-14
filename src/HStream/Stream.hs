{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Stream
  ( mkStreamBuilder,
    from,
    HStream.Stream.to,
    HStream.Stream.filter,
    HStream.Stream.map,
    Stream,
    StreamBuilder,
    StreamSourceConfig (..),
    StreamSinkConfig (..),
  )
where

import Control.Comonad.Traced
import Data.Dynamic
import HStream.Encoding
import HStream.Processor
import HStream.Processor.Internal
import HStream.Topic
import RIO
import qualified RIO.Text as T

data Stream k v = Stream
  { streamKeySerde :: Maybe (Serde k),
    streamValueSerde :: Maybe (Serde v),
    streamProcessorName :: T.Text,
    streamStreamBuilder :: StreamBuilder
  }

data StreamBuilder = StreamBuilder
  { sbTaskBuilder :: TaskBuilder,
    sbProcessorIndex :: IORef Int
  }

mkStreamBuilder :: T.Text -> IO StreamBuilder
mkStreamBuilder taskName = do
  index <- newIORef 0
  return $
    StreamBuilder
      { sbTaskBuilder = buildTask taskName,
        sbProcessorIndex = index
      }

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

from ::
  (Typeable k, Typeable v) =>
  StreamSourceConfig k v ->
  StreamBuilder ->
  IO (Stream k v)
from StreamSourceConfig {..} sb@StreamBuilder {..} = do
  sourceProcessorName <- mkInternalProcessorName (sscTopicName `T.append` "-SOURCE-") sbProcessorIndex
  let sourceCfg =
        SourceConfig
          { sourceName = sourceProcessorName,
            sourceTopicName = sscTopicName,
            keyDeserializer = Just $ deserializer sscKeySerde,
            valueDeserializer = deserializer sscValueSerde
          }

  let taskBuilder = sbTaskBuilder =>> addSource sourceCfg
  return
    Stream
      { streamKeySerde = Just sscKeySerde,
        streamValueSerde = Just sscValueSerde,
        streamProcessorName = sourceProcessorName,
        streamStreamBuilder =
          sb
            { sbTaskBuilder = taskBuilder
            }
      }

to ::
  (Typeable k, Typeable v) =>
  StreamSinkConfig k v ->
  Stream k v ->
  IO Task
to StreamSinkConfig {..} Stream {..} = do
  let sb = streamStreamBuilder
  sinkProcessorName <- mkInternalProcessorName (sicTopicName `T.append` "-SINK-") (sbProcessorIndex sb)
  let sinkCfg =
        SinkConfig
          { sinkName = sinkProcessorName,
            sinkTopicName = sicTopicName,
            keySerializer = Just $ serializer sicKeySerde,
            valueSerializer = serializer sicValueSerde
          }
  let taskBuilder = sbTaskBuilder streamStreamBuilder =>> addSink sinkCfg [streamProcessorName]
  return $ build taskBuilder

mkInternalProcessorName :: T.Text -> IORef Int -> IO T.Text
mkInternalProcessorName namePrefix indexRef = do
  index <- readIORef indexRef
  writeIORef indexRef (index + 1)
  return $ namePrefix `T.append` T.pack (show index)

filter ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Stream k v ->
  IO (Stream k v)
filter f stream@Stream {..} = do
  name <- mkInternalProcessorName "FILTER-" (sbProcessorIndex streamStreamBuilder)
  let p = filterProcessor f
  let taskBuilder' = sbTaskBuilder streamStreamBuilder
  let taskBuilder = taskBuilder' =>> addProcessor name p [streamProcessorName]
  return
    stream
      { streamStreamBuilder = streamStreamBuilder {sbTaskBuilder = taskBuilder},
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
map f stream@Stream {..} = do
  name <- mkInternalProcessorName "MAP-" (sbProcessorIndex streamStreamBuilder)
  let p = mapProcessor f
  let taskBuilder' = sbTaskBuilder streamStreamBuilder
  let taskBuilder = taskBuilder' =>> addProcessor name p [streamProcessorName]
  return
    stream
      { streamStreamBuilder = streamStreamBuilder {sbTaskBuilder = taskBuilder},
        streamProcessorName = name,
        streamKeySerde = Nothing,
        streamValueSerde = Nothing
      }
