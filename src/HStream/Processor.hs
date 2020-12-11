{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Processor
  ( buildTask,
    build,
    addSource,
    addProcessor,
    addSink,
    runTask,
    forward,
    mkMockTopicStore,
    mkMockTopicConsumer,
    mkMockTopicProducer,
    Record (..),
    Processor (..),
    SourceConfig (..),
    SinkConfig (..),
    TaskConfig (..),
    MessageStoreType (..),
    MockTopicStore (..),
    MockMessage (..),
  )
where

import Control.Comonad.Traced
import Control.Exception (throw)
import Data.Maybe
import Data.Typeable
import HStream.Encoding
import HStream.Error (HStreamError (..))
import HStream.Processor.Internal
import HStream.Topic
import RIO
import qualified RIO.ByteString.Lazy as BL
import qualified RIO.HashMap as HM
import RIO.HashMap.Partial as HM'
import qualified RIO.HashSet as HS
import qualified RIO.List as L
import qualified RIO.Text as T

-- import qualified Prelude as P

build :: TaskBuilder -> Task
build = extract

buildTask ::
  T.Text ->
  TaskBuilder
buildTask = traced . buildTask'
  where
    buildTask' taskName tp@TaskTopologyConfig {..} =
      let _ = validateTopology tp
          topologyForward =
            HM.foldlWithKey'
              ( \acc k v ->
                  let childName = k
                      (curP, parentsName) = v
                      nacc =
                        if HM.member childName acc
                          then acc
                          else HM.insert childName (curP, []) acc
                   in foldl'
                        ( \acc' parent ->
                            if HM.member parent acc'
                              then
                                let (p, cs) = acc' HM'.! parent
                                 in HM.insert parent (p, cs ++ [childName]) acc'
                              else
                                let (p, _) = topology HM'.! parent
                                 in HM.insert parent (p, [childName]) acc'
                        )
                        nacc
                        parentsName
              )
              (HM.empty :: HM.HashMap T.Text (EProcessor, [T.Text]))
              topology
       in Task
            { taskName = taskName,
              taskSourceConfig = sourceCfgs,
              taskSinkConfig = sinkCfgs,
              taskTopologyReversed = topology,
              taskTopologyForward = topologyForward
            }

validateTopology :: TaskTopologyConfig -> ()
validateTopology TaskTopologyConfig {..} =
  if L.null sourceCfgs
    then throw $ TaskTopologyBuildError "task build error: no valid source config"
    else
      if L.null sinkCfgs
        then throw $ TaskTopologyBuildError "task build error: no valid sink config"
        else ()

data SourceConfig k v = SourceConfig
  { sourceName :: T.Text,
    sourceTopicName :: T.Text,
    keyDeserializer :: Maybe (Deserializer k),
    valueDeserializer :: Deserializer v
  }

data SinkConfig k v = SinkConfig
  { sinkName :: T.Text,
    sinkTopicName :: T.Text,
    keySerializer :: Maybe (Serializer k),
    valueSerializer :: Serializer v
  }

addSource ::
  (Typeable k, Typeable v) =>
  SourceConfig k v ->
  TaskBuilder ->
  Task
addSource cfg@SourceConfig {..} builder =
  runTraced builder $
    mempty
      { sourceCfgs =
          HM.singleton
            sourceTopicName
            InternalSourceConfig
              { iSourceName = sourceName,
                iSourceTopicName = sourceTopicName
              },
        topology =
          HM.singleton
            sourceName
            (mkEProcessor $ buildSourceProcessor cfg, [])
      }

buildSourceProcessor ::
  (Typeable k, Typeable v) =>
  SourceConfig k v ->
  Processor BL.ByteString BL.ByteString
buildSourceProcessor SourceConfig {..} = Processor $ \Record {..} -> do
  -- deserialize and forward
  logDebug "enter source processor"
  ctx <- ask
  writeIORef (curProcessor ctx) sourceName
  let rk = fmap runDeser keyDeserializer <*> recordKey
  let rv = runDeser valueDeserializer recordValue
  let rr =
        Record
          { recordKey = rk,
            recordValue = rv
          }
  forward rr

addProcessor ::
  (Typeable kin, Typeable vin) =>
  T.Text ->
  Processor kin vin ->
  [T.Text] ->
  TaskBuilder ->
  Task
addProcessor name processor parentNames builder =
  runTraced builder $
    mempty
      { topology = HM.singleton name (mkEProcessor processor, parentNames)
      }

buildSinkProcessor ::
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  Processor k v
buildSinkProcessor SinkConfig {..} = Processor $ \Record {..} -> do
  logDebug "enter sink processor"
  let rk = liftA2 runSer keySerializer recordKey
  let rv = runSer valueSerializer recordValue
  forward Record {recordKey = rk, recordValue = rv}

buildInternalSinkProcessor ::
  TopicProducer p =>
  p ->
  InternalSinkConfig ->
  Processor BL.ByteString BL.ByteString
buildInternalSinkProcessor producer InternalSinkConfig {..} = Processor $ \Record {..} -> do
  liftIO $
    send
      producer
      RawProducerRecord
        { rprTopic = iSinkTopicName,
          rprKey = recordKey,
          rprValue = recordValue
        }

-- why this not work?
-- can not deduce k v,
-- 这没有理由啊，
-- record 上肯定能执行 cast 操作的,
-- 这里是否能 cast 成任意的值？
-- 关键是下面也没有出现能推断 k v 类型的信息，
-- 所有后半段没法成立.
-- 反正要给后半段一个能推断出来的类型.
-- 它这个类型信息其实就在 serializer 里面.
--
-- 所以必须留下未包装的 serailizer,
-- 也就是在开始的时候就构建
--
--
-- case cast record of
--   Just Record {..} -> do
--     -- serialize and write to topic
--     logDebug "enter sink processor"
--     let rk = liftA2 runESer iKeySerializer (fmap mkEV recordKey)
--     let rv = runESer iValueSerializer (mkEV recordValue)
--     liftIO $
--       send
--         producer
--         RawProducerRecord
--           { rprTopic = iSinkTopicName,
--             rprKey = rk,
--             rprValue = rv
--           }

addSink ::
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  [T.Text] ->
  TaskBuilder ->
  Task
addSink cfg@SinkConfig {..} parentNames builder =
  runTraced builder $
    mempty
      { topology =
          HM.singleton
            sinkName
            (mkEProcessor $ buildSinkProcessor cfg, parentNames),
        sinkCfgs =
          HM.singleton
            sinkName
            InternalSinkConfig
              { iSinkName = sinkName,
                iSinkTopicName = sinkTopicName
              }
      }

runTask ::
  TaskConfig ->
  Task ->
  IO ()
runTask TaskConfig {..} task@Task {..} = do
  topicConsumer <-
    case tcMessageStoreType of
      Mock mockStore -> mkMockTopicConsumer mockStore
      LogDevice -> throwIO $ UnSupportedMessageStoreError "LogDevice is not supported!"
      Kafka -> throwIO $ UnSupportedMessageStoreError "Kafka is not supported!"
  topicProducer <-
    case tcMessageStoreType of
      Mock mockStore -> mkMockTopicProducer mockStore
      LogDevice -> throwIO $ UnSupportedMessageStoreError "LogDevice is not supported!"
      Kafka -> throwIO $ UnSupportedMessageStoreError "Kafka is not supported!"

  -- add InternalSink Node
  let newTaskTopologyForward =
        HM.foldlWithKey'
          ( \a k v@InternalSinkConfig {..} ->
              let internalSinkProcessor = buildInternalSinkProcessor topicProducer v
                  ep = mkEProcessor internalSinkProcessor
                  (sinkProcessor, children) = taskTopologyForward HM'.! k
                  name = T.append iSinkTopicName "-INTERNAL-SINK"
                  tp = HM.insert k (sinkProcessor, children ++ [name]) a
               in HM.insert name (ep, []) tp
          )
          taskTopologyForward
          taskSinkConfig

  ctx <- buildTaskContext task {taskTopologyForward = newTaskTopologyForward} tcLogFunc

  let sourceTopicNames = HM.keys taskSourceConfig
  topicConsumer' <- subscribe topicConsumer sourceTopicNames
  forever $
    runRIO ctx $ do
      logDebug "start iteration..."
      rawRecords <- liftIO $ pollRecords topicConsumer' 2000000
      logDebug $ "polled " <> display (length rawRecords) <> " records"
      forM_
        rawRecords
        ( \RawConsumerRecord {..} -> do
            let acSourceName = iSourceName (taskSourceConfig HM'.! rcrTopic)
            let (sourceEProcessor, _) = newTaskTopologyForward HM'.! acSourceName
            runEP sourceEProcessor (mkERecord Record {recordKey = rcrKey, recordValue = rcrValue})
        )

data TaskConfig = TaskConfig
  { tcMessageStoreType :: MessageStoreType,
    tcLogFunc :: LogFunc
  }

data MessageStoreType
  = Mock MockTopicStore
  | LogDevice
  | Kafka

mkMockTopicStore :: IO MockTopicStore
mkMockTopicStore = do
  s <- newTVarIO (HM.empty :: HM.HashMap T.Text [MockMessage])
  return $
    MockTopicStore
      { mtsData = s
      }

forward ::
  (Typeable k, Typeable v) =>
  Record k v ->
  RIO TaskContext ()
forward record = do
  ctx <- ask
  curProcessorName <- readIORef $ curProcessor ctx
  logDebug $ "enter forward, curProcessor is " <> display curProcessorName
  let taskInfo = taskConfig ctx
  let tplgy = taskTopologyForward taskInfo
  let (_, children) = tplgy HM'.! curProcessorName
  for_ children $ \cname -> do
    logDebug $ "forward to child: " <> display cname
    writeIORef (curProcessor ctx) cname
    let (eProcessor, _) = tplgy HM'.! cname
    runEP eProcessor (mkERecord record)

-- if isSink (taskSinkConfig taskInfo) cname
--   then do
--     let dumbSinkProcessor = (\_ -> return ()) :: Record Dynamic Dynamic -> RIO TaskContext ()
--     let proc = fromDyn dynProcessor dumbSinkProcessor
--     proc
--       Record
--         { recordKey = toDyn <$> recordKey record,
--           recordValue = toDyn $ recordValue record
--         }
--   else do
--     let dr = dynProcessor `dynApp` toDyn record
--     fromDyn dr (return () :: RIO TaskContext ())

-- dumbProcessor :: EProcessor
-- dumbProcessor = mkEProcessor $ return ()

data MockMessage = MockMessage
  { mmTimestamp :: Timestamp,
    mmKey :: Maybe BL.ByteString,
    mmValue :: BL.ByteString
  }

data MockTopicStore = MockTopicStore
  { mtsData :: TVar (HM.HashMap T.Text [MockMessage])
  }

data MockTopicConsumer = MockTopicConsumer
  { mtcSubscribedTopics :: HS.HashSet TopicName,
    mtcTopicOffsets :: HM.HashMap T.Text Offset,
    mtcStore :: MockTopicStore
  }

instance TopicConsumer MockTopicConsumer where
  subscribe tc topicNames = return $ tc {mtcSubscribedTopics = HS.fromList topicNames}

  pollRecords MockTopicConsumer {..} pollDuration = do
    threadDelay pollDuration
    atomically $ do
      dataStore <- readTVar $ mtsData mtcStore
      let r =
            HM.foldlWithKey'
              ( \a k v ->
                  if HS.member k mtcSubscribedTopics
                    then
                      a
                        ++ map
                          ( \MockMessage {..} ->
                              RawConsumerRecord
                                { rcrTopic = k,
                                  rcrOffset = 0,
                                  rcrTimestamp = mmTimestamp,
                                  rcrKey = mmKey,
                                  rcrValue = mmValue
                                }
                          )
                          v
                    else a
              )
              []
              dataStore
      let newDataStore =
            HM.mapWithKey
              ( \k v ->
                  if HS.member k mtcSubscribedTopics
                    then []
                    else v
              )
              dataStore
      writeTVar (mtsData mtcStore) newDataStore
      return r

mkMockTopicConsumer :: MockTopicStore -> IO MockTopicConsumer
mkMockTopicConsumer topicStore =
  return
    MockTopicConsumer
      { mtcSubscribedTopics = HS.empty,
        mtcTopicOffsets = HM.empty,
        mtcStore = topicStore
      }

data MockTopicProducer = MockTopicProducer
  { mtpStore :: MockTopicStore
  }

mkMockTopicProducer ::
  MockTopicStore ->
  IO MockTopicProducer
mkMockTopicProducer store =
  return
    MockTopicProducer
      { mtpStore = store
      }

instance TopicProducer MockTopicProducer where
  send MockTopicProducer {..} RawProducerRecord {..} =
    atomically $ do
      let record =
            MockMessage
              { mmTimestamp = 0,
                mmKey = rprKey,
                mmValue = rprValue
              }
      dataStore <- readTVar $ mtsData mtpStore
      if HM.member rprTopic dataStore
        then do
          let td = dataStore HM'.! rprTopic
          let newDataStore = HM.insert rprTopic (td ++ [record]) dataStore
          writeTVar (mtsData mtpStore) newDataStore
        else do
          let newDataStore = HM.insert rprTopic [record] dataStore
          writeTVar (mtsData mtpStore) newDataStore
