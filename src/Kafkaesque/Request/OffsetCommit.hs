module Kafkaesque.Request.OffsetCommit
  ( offsetCommitRequestV0
  ) where

import Control.Monad (forM)
import qualified Crypto.Hash.SHA256 as SHA256 (hash)
import Data.Attoparsec.ByteString (Parser)
import Data.ByteString (ByteString)
import qualified Data.ByteString (last)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import Data.Serialize.Put (runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Message (messageV0)
import Kafkaesque.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.Log (writeMessageSet)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        OffsetCommitResponseV0(..), putInt32be, putInt64be, putKafkaString)

type PartitionData = (Int32, Int64, String)

type TopicData = (String, [PartitionData])

data OffsetCommitRequestV0 =
  OffsetCommitRequestV0 String
                        [TopicData]

offsetCommitPartition :: Parser PartitionData
offsetCommitPartition =
  (\partitionId offset metadata -> (partitionId, offset, metadata)) <$>
  signedInt32be <*>
  signedInt64be <*>
  kafkaString

offsetCommitTopic :: Parser TopicData
offsetCommitTopic =
  (\a b -> (a, b)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitPartition)

offsetCommitRequestV0 :: Parser OffsetCommitRequestV0
offsetCommitRequestV0 =
  OffsetCommitRequestV0 <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitTopic)

getConsumerOffsetTopicPartition ::
     PG.Connection -> String -> IO (Maybe (Int32, Int32))
getConsumerOffsetTopicPartition conn cgId = do
  let topicName = "__consumer_offsets"
      partitionId =
        (Data.ByteString.last . SHA256.hash . fromString $ cgId) `mod` 8
  getTopicPartition conn topicName (fromIntegral partitionId)

consumerOffsetKey :: String -> String -> Int32 -> ByteString
consumerOffsetKey cgId topicName partitionId =
  runPut $
  putKafkaString cgId *> putKafkaString topicName *> putInt32be partitionId

saveOffset ::
     PG.Connection -> String -> String -> Int32 -> Int64 -> String -> IO ()
saveOffset conn cgId topicName partitionId offset metadata = do
  Just (offsetTopicId, offsetPartitionId) <-
    getConsumerOffsetTopicPartition conn cgId
  let key = consumerOffsetKey cgId topicName partitionId
      value = runPut (putInt64be offset *> putKafkaString metadata)
      message = messageV0 (Just key) (Just value)
  writeMessageSet conn offsetTopicId offsetPartitionId [message]
  return ()

respondToRequest ::
     Pool.Pool PG.Connection
  -> OffsetCommitRequestV0
  -> IO OffsetCommitResponseV0
respondToRequest pool (OffsetCommitRequestV0 cgId topics) = do
  let getResponsePartition conn topicName (partitionId, offset, metadata) = do
        topicRes <- getTopicPartition conn topicName partitionId
        maybe
          (return (partitionId, UnknownTopicOrPartition))
          (const $ do
             saveOffset conn cgId topicName partitionId offset metadata
             return (partitionId, NoError))
          topicRes
      getResponseTopic conn (topicName, partitions) = do
        partsResponse <- forM partitions (getResponsePartition conn topicName)
        return (topicName, partsResponse)
      getResponseTopics conn = forM topics (getResponseTopic conn)
  responseTopics <- Pool.withResource pool getResponseTopics
  return $ OffsetCommitResponseV0 responseTopics

instance KafkaRequest OffsetCommitRequestV0 where
  respond pool req = KResp <$> respondToRequest pool req
