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
    voidSerializer,
    voidDeserializer,
    Record (..),
    Processor (..),
    SourceConfig (..),
    SinkConfig (..),
    Deserializer (..),
    Serializer (..),
    Serde (..),
    TaskConfig (..),
    MessageStoreType (..),
    MockTopicStore (..),
    MockMessage (..),
  )
where

import Control.Comonad.Traced
import Control.Exception (throw)
import Data.Dynamic
import Data.Maybe
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

newtype Processor kin vin = Processor {runP :: Record kin vin -> RIO TaskContext ()}

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
              (HM.empty :: HM.HashMap T.Text (Dynamic, [T.Text]))
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
                iSourceTopicName = sourceTopicName,
                iKeyDeserializer = fmap (toDyn . runDeser) keyDeserializer,
                iValueDeserializer = toDyn $ runDeser valueDeserializer
              },
        topology =
          HM.singleton
            sourceName
            (toDyn $ runP $ buildSourceProcessor cfg, [])
            -- (toDyn dumbProcessor, [])
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
      { topology = HM.singleton name (toDyn $ runP processor, parentNames)
      }

buildSinkProcessor ::
  TopicProducer p =>
  p ->
  InternalSinkConfig ->
  Processor Dynamic Dynamic
buildSinkProcessor producer InternalSinkConfig {..} = Processor $ \Record {..} -> do
  -- serialize and write to topic
  logDebug "enter sink processor"
  let mrk = liftA2 dynApp iKeySerializer recordKey
  let rk = fmap (`fromDyn` BL.empty) mrk
  let rv = fromDyn (iValueSerializer `dynApp` recordValue) BL.empty
  liftIO $
    send
      producer
      RawProducerRecord
        { rprTopic = iSinkTopicName,
          rprKey = rk,
          rprValue = rv
        }

addSink ::
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  [T.Text] ->
  TaskBuilder ->
  Task
addSink SinkConfig {..} parentNames builder =
  runTraced builder $
    mempty
      { topology =
          HM.singleton
            sinkName
            (toDyn dumbProcessor, parentNames),
        sinkCfgs =
          HM.singleton
            sinkName
            InternalSinkConfig
              { iSinkName = sinkName,
                iSinkTopicName = sinkTopicName,
                iKeySerializer = fmap (toDyn . runSer) keySerializer,
                iValueSerializer = toDyn $ runSer valueSerializer
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

  -- build Sink Node
  let newTaskTopologyForward =
        HM.foldlWithKey'
          ( \a k v ->
              let sp = buildSinkProcessor topicProducer v
                  (_, parents) = taskTopologyForward HM'.! k
               in HM.insert k (toDyn $ runP sp, parents) a
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
            let (ds, _) = newTaskTopologyForward HM'.! acSourceName
            let dr = ds `dynApp` toDyn Record {recordKey = rcrKey, recordValue = rcrValue}
            fromDyn dr (return () :: RIO TaskContext ())
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

data Record k v = Record
  { recordKey :: Maybe k,
    recordValue :: v
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
    let (dynProcessor, _) = tplgy HM'.! cname
    if isSink (taskSinkConfig taskInfo) cname
      then do
        let dumbSinkProcessor = (\_ -> return ()) :: Record Dynamic Dynamic -> RIO TaskContext ()
        let proc = fromDyn dynProcessor dumbSinkProcessor
        proc
          Record
            { recordKey = toDyn <$> recordKey record,
              recordValue = toDyn $ recordValue record
            }
      else do
        let dr = dynProcessor `dynApp` toDyn record
        fromDyn dr (return () :: RIO TaskContext ())

dumbProcessor :: RIO TaskContext ()
dumbProcessor = return ()

newtype Deserializer a = Deserializer {runDeser :: BL.ByteString -> a}

newtype Serializer a = Serializer {runSer :: a -> BL.ByteString}

voidDeserializer :: Maybe (Deserializer Void)
voidDeserializer = Nothing

voidSerializer :: Maybe (Serializer Void)
voidSerializer = Nothing

data Serde a = Serde
  { serializer :: Serializer a,
    deserializer :: Deserializer a
  }

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
