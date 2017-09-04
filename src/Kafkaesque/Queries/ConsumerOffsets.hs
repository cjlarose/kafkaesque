module Kafkaesque.Queries.ConsumerOffsets
  ( saveOffset
  ) where

import qualified Crypto.Hash.SHA256 as SHA256 (hash)
import qualified Data.ByteString (last)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int32, Int64)
import Data.Serialize.Put (putWord8, runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Message (messageV0)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.Log (writeMessageSet)
import Kafkaesque.Response (putInt32be, putInt64be, putKafkaString)

getConsumerOffsetTopicPartition ::
     PG.Connection -> String -> IO (Maybe (Int32, Int32))
getConsumerOffsetTopicPartition conn cgId = do
  let topicName = "__consumer_offsets"
      partitionId =
        (Data.ByteString.last . SHA256.hash . fromString $ cgId) `mod` 8
  getTopicPartition conn topicName (fromIntegral partitionId)

consumerOffsetKey :: String -> String -> Int32 -> ByteString
consumerOffsetKey cgId topicName partitionId =
  let putKey =
        putKafkaString cgId *> putKafkaString topicName *>
        putInt32be partitionId
  in SHA256.hash . runPut $ putKey

saveOffset ::
     PG.Connection -> String -> String -> Int32 -> Int64 -> String -> IO ()
saveOffset conn cgId topicName partitionId offset metadata = do
  Just (offsetTopicId, offsetPartitionId) <-
    getConsumerOffsetTopicPartition conn cgId
  let key = consumerOffsetKey cgId topicName partitionId
      value =
        runPut (putWord8 0 *> putInt64be offset *> putKafkaString metadata)
      message = messageV0 (Just key) (Just value)
  writeMessageSet conn offsetTopicId offsetPartitionId [message]
  return ()
