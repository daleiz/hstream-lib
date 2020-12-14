{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

import Control.Comonad ((=>>))
import Data.Aeson
import qualified Data.Binary as B
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import HStream.Encoding
import HStream.Processor
import HStream.Store
import HStream.Topic
import RIO
import qualified RIO.ByteString.Lazy as BL
import System.Random
import qualified Prelude as P

data R = R
  { temperature :: Int,
    humidity :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

main :: IO ()
main = do
  let sourceConfig =
        SourceConfig
          { sourceName = "source",
            sourceTopicName = "demo-source",
            keyDeserializer = Just $ Deserializer TLE.decodeUtf8,
            valueDeserializer = Deserializer (\s -> (fromJust $ decode s) :: R)
          }
  let sinkConfig =
        SinkConfig
          { sinkName = "sink",
            sinkTopicName = "demo-sink",
            keySerializer = Just $ Serializer TLE.encodeUtf8,
            valueSerializer = Serializer (B.encode :: Int -> BL.ByteString)
          }

  memoryStore <- mkInMemoryKVStore :: IO (InMemoryKVStore TL.Text Int)
  let task =
        build $
          buildTask "demo"
            =>> addSource sourceConfig
            =>> addProcessor
              "filter"
              (filterProcessor filterR)
              ["source"]
            =>> addProcessor
              "count"
              (aggProcessor "demo-store" 0 countR)
              ["filter"]
            =>> addSink sinkConfig ["count"]
            =>> addStateStore "demo-store" memoryStore ["count"]

  mockStore <- mkMockTopicStore
  mp <- mkMockTopicProducer mockStore
  mc' <- mkMockTopicConsumer mockStore

  async $
    forever $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = "demo-source",
            rprKey = mmKey,
            rprValue = mmValue
          }

  mc <- subscribe mc' ["demo-sink"]
  async $
    forever $ do
      records <- pollRecords mc 1000000
      forM_ records $ \RawConsumerRecord {..} -> do
        let k = fromJust rcrKey
        P.putStrLn $
          ">>> count: key: "
            ++ show k
            ++ " , value: "
            ++ show (B.decode rcrValue :: Int)

  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = Mock mockStore,
              tcLogFunc = lf
            }
    runTask taskConfig task

filterR :: Record TL.Text R -> Bool
filterR Record {..} =
  temperature recordValue >= 0
    && humidity recordValue >= 0

countR :: Int -> Record TL.Text R -> Int
countR a _ = a + 1

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
  t <- getStdRandom (randomR (0, 100))
  h <- getStdRandom (randomR (0, 100))
  let r = R {temperature = t, humidity = h}
  let idk = TL.append "id-" $ TL.pack $ show k
  P.putStrLn $ "gen data: " ++ "key: " ++ TL.unpack idk ++ ", value: " ++ show r
  return
    MockMessage
      { mmTimestamp = 0,
        mmKey = Just $ TLE.encodeUtf8 idk,
        mmValue = encode $ R {temperature = t, humidity = h}
      }

filterProcessor :: (Typeable k, Typeable v) => (Record k v -> Bool) -> Processor k v
filterProcessor f = Processor $ \r ->
  when (f r) $ forward r

aggProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  Processor k v
aggProcessor storeName initialValue aggF = Processor $ \r -> do
  store <- getStateStore storeName
  let key = fromJust $ recordKey r
  ma <- liftIO $ ksGet key store
  let acc = fromMaybe initialValue ma
  let newAcc = aggF acc r
  liftIO $ ksPut key newAcc store
  forward r {recordValue = newAcc}
