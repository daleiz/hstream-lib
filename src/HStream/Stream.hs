{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Stream where

import Control.Comonad.Traced
import Data.Dynamic
import HStream.Processor
import HStream.Processor.Internal
import HStream.Topic
import RIO
import qualified RIO.Text as T

-- Stream type supply a high level api for topology build.
-- the user of Stream API do not need to use low level
-- toplolgy functions, such as "addProcessor", "addSink",...etc.,
-- they can build stream task on a high level semantic abstraction,
-- we will build topolgy implicityly for them.
--
-- so how we build topolgy for stream api?
-- we need generate processor names,
-- the parent relation is obvious,
-- input stream is parent,
-- output stream is downstream.
-- And we need to generate special processors for stream purpose.
--
-- so how to generate a processor name?
-- the name can used for debug,
-- and show toplogy description.
--
-- how to generate common processors?
-- for stateless, it is simple.
-- for stateful, it contains:
-- 1. join
-- 2. groupBy ... agg
-- 3. window ... agg
--
-- logic view by stream:
-- Stream a : stands for a stream of record of a type
-- it need supply serde for key and values.
--
-- because the kv type will change,
-- we need the serde for each new type.
--
-- what is the data in the Stream data type ?
-- it should contains a taskBuilder ?
-- but a taskBuilder is just a function.
-- we need TaskBuilder -> Task,
-- and they can be connected by (=>>)
--
-- for Stream Data type,
-- Serde is not nesscessory,
-- only stateful processor need it.
-- and they can get by parameters,
--
-- we just use phethomenond
data Stream k v = Stream
  { streamKeySerde :: Maybe (Serde k),
    streamValueSerde :: Maybe (Serde v),
    -- streamInternalBuilder :: TaskBuilder,
    streamProcessorName :: T.Text,
    streamStreamBuilder :: StreamBuilder
  }

-- global variable for stream construction
-- need a buildTask as start.
-- but a buildTask need a taskName,
-- but we can not provide taskName for streamBuilder,
-- because it not stand for a complete task,
-- it is partial,
-- in the contrary,
-- we can use the same task name stand for
-- the same task.
--
-- the invariant is that
-- from the same StreamBuilder,
-- derving a standalone Task.
--
-- In other words,
-- a StreamBuilder map to only one Task,
-- for build different Tasks,
-- need recreate different StreamBuilders
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

-- s1 = from sb sc
-- s2 = from sb sc
-- the processorIndex of s1 and s2 should be different.
-- how about builder?
-- should be immutable.
--
-- nopure?
-- sb <- mkStreamBuilder
-- source <- from sb cfg
--
-- mkStreamBuilder
--   >>= from cfg
--   >>= filter f
--   >>= to sinkCfg
--
-- to 的 API 不好实现，
-- 继续用 comonad 来做，
-- 中间的数据结构是什么？
--
-- buildStream
--   =>> from sourceCfg
--   =>> filter f
--   =>> to sinkCfg
--
-- 用 comonad 的思路来做的话，
-- 要让 from, filter.. 这些 combinator
-- 返回 internalConfig 或者说 TaskBuilder,
-- 这里复杂的地方在于命名机制，
-- 为了简单起见，
-- 直接用 UUID 给 Processor 命名，
-- 杜绝自己维护 ProcessorIndex 的情况，
-- 这样的情况下，
-- 直接返回 TaskBuilder 好像可行？
-- 那 Stream 的概念体现在哪里呢？
--
-- StreamBuilder :: Stream -> Task
--
-- 关键问题是看待 to 的方式不一样，
-- to 要区别对待，
-- to :: Stream -> Task
-- 让 to 的时候直接产生 Task 合适吗？
-- 好像没啥问题.
--
-- 问题在于多 source 的情况下需要额外融合一步？
-- 对的，
-- 是要额外融合一步.
--
-- 这种 API 只能线性组合？
--
-- builder.stream("source0").filter().to("sink0")
-- builder.stream("source1").filter().to("sink1")
--
-- 也就是说从同一个 builder 出发一次可以确定一条从 source 到 sink 的 path,
-- 一个 builder 可以产生多条这样的 path,
-- 前提是对应多个 source 的情况.
--
-- 这里就有分歧了，
-- 如果要用 comonad 模式来实现的话，
-- 内隐的状态是什么？
-- 就是 TaskBuilder, 以及 ProcessorIndex,
-- 如果只是线性的单条路径呢？
--
-- 这里的关键是命名机制，
-- 因为需要给 Task 里面每个 Processor 提供一个唯一的名字,
-- 这就需要保存状态.
--
--
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
            keyDeserializer = deserializer sscKeySerde,
            valueDeserializer = deserializer sscValueSerde
          }

  let taskBuilder = sbTaskBuilder =>> addSource sourceCfg
  return
    Stream
      { streamKeySerde = Just sscKeySerde,
        streamValueSerde = Just sscValueSerde,
        -- streamInternalBuilder :: TaskBuilder,
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
            keySerializer = serializer sicKeySerde,
            valueSerializer = serializer sicValueSerde
          }
  let taskBuilder = sbTaskBuilder streamStreamBuilder =>> addSink sinkCfg [streamProcessorName]
  return $ build taskBuilder

-- 全局 id 分配器，
-- 其实只需要给每个 processor 一个独特的名字就好了，
--
--
mkInternalProcessorName :: T.Text -> IORef Int -> IO T.Text
mkInternalProcessorName namePrefix indexRef = do
  index <- readIORef indexRef
  writeIORef indexRef (index + 1)
  return $ namePrefix `T.append` T.pack (show index)

-- we also need a StreamBuilder to build Stream.
--
-- fromTopic :: TopicName -> Stream k v
--
-- filter :: (Record k v -> Bool) -> Stream k v -> Stream k v

-- stateless api (record-by-record process):
-- - branch
-- - filter
-- - map
-- - flatMap
-- - forEach
-- - groupBy
--

-- the stream api will be built on public Processor API,
-- include: addSource, addProcessor, addSink, forward.
filter ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Stream k v ->
  IO (Stream k v)
filter f stream@Stream {..} = do
  name <- mkInternalProcessorName "FILTER-" (sbProcessorIndex streamStreamBuilder)
  let p = filterProcessor f
  -- need parent name.
  -- need processor name.
  --
  -- how to find parent name?
  -- store to Stream data
  --
  -- how to generate processor name?
  -- processorNamePrefix + indexId
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

-- stateful api :
-- - agg
-- - window
-- - join

-- Window
--
-- tumbling time windows:
--   fixed-size, non-overlapping, gap-less.
--   a data record will belong to one and only one window.
--   the advance interval is idnentical to the window size.
--   tumbling time window are aligned to the epoch,
--   with the lower interval bound being inclusive and
--   upper bound being exclusive.
--   "Align to the epoch" means that the first window starts at
--   timestamp zero.
--
-- hopping time windows
--   windows based on time intervals.
--   fixed-sized, overlapping windows.
--   the windows size and its advance iterval(aka "hop").
--   a data record may belong to more than one such windows.
--   hopping windows are aligned to the epoch.
--
-- session windows
--   session windows are used to aggregate key-based events
--   into so-called sessions, the process of which is refferd
--   to as sessionization.
--   Sessions represent a period of activity separated by a
--   defined gap of inactivity(or "idleness").
--   Any events processed that fall within the inactivity gap
--   of any existing sessions are merged into the existing
--   sessions.
--   If an event falls outside of the session gap,
--   then a new session will be created.
--
-- how to implement fixed-sized time window?
-- for, hopping window.
-- they are aligned window,
-- so according the event time of a record,
-- we can assign it to particular windows.
-- then trigger window computing.
-- add state to processors.
--
--

-- 假定 key 是可选的，
-- 那么在 sourceConfig 里面 keyDeserializer
-- 就是可选的，
-- 那么如果不设置 valueDeserializer 呢，
-- 是否可以默认为是 Text ?
-- 这个可以没有，
-- 显式大于隐式.
--
-- 如果 keyDeserializer 是 Nothing,
-- 会有什么影响吗？
-- 在 runTask 的时候，
-- 从 sourceTopic poll 数据回来，
-- 这时候如果数据里面没有 key,
-- 那么设置不设置 keySerializer 都没关系，
-- 如果数据里面包含 key,
-- 同时也提供了 keySerializer,
-- 那么就可以执行正常的反序列化操作，
-- 如果这时候 keyDeserializer 也是 Nothing,
-- 这时候默认无法执行反序列化的操作，
-- 结果也是 Nothing.
-- 这就是一个 Applicative ?
-- Maybe function 作用在 Maybe data 上面
--
--
