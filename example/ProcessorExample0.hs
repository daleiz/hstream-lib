{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

import Control.Comonad ((=>>))
import Data.Aeson
import Data.Maybe
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import HStream.Encoding
import HStream.Processor
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
            keyDeserializer = voidDeserializer,
            valueDeserializer = Deserializer (\s -> (fromJust $ decode s) :: R)
          }
  let sinkConfig =
        SinkConfig
          { sinkName = "sink",
            sinkTopicName = "demo-sink",
            keySerializer = voidSerializer,
            valueSerializer = Serializer (encode :: R -> BL.ByteString)
          }
  let task =
        build $
          buildTask "demo"
            =>> addSource sourceConfig
            =>> addProcessor
              "filter"
              (filterProcessor filterR)
              ["source"]
            =>> addSink sinkConfig ["filter"]

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
      forM_ records $ \RawConsumerRecord {..} ->
        P.putStr "detect abnormal data: " >> BL.putStrLn rcrValue

  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = Mock mockStore,
              tcLogFunc = lf
            }
    runTask taskConfig task

filterR :: Record Void R -> Bool
filterR Record {..} =
  temperature recordValue >= 50
    && humidity recordValue >= 0

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 10)) :: IO Int
  t <- getStdRandom (randomR (0, 100))
  h <- getStdRandom (randomR (0, 100))
  let r = R {temperature = t, humidity = h}
  P.putStrLn $ "gen data: " ++ show r
  return
    MockMessage
      { mmTimestamp = 0,
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode $ R {temperature = t, humidity = h}
      }

filterProcessor :: (Typeable k, Typeable v) => (Record k v -> Bool) -> Processor k v
filterProcessor f = Processor $ \r ->
  when (f r) $ forward r
