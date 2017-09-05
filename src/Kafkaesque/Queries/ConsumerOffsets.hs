{-# LANGUAGE QuasiQuotes #-}

module Kafkaesque.Queries.ConsumerOffsets
  ( saveOffset
  , getOffsetForConsumer
  ) where

import qualified Crypto.Hash.SHA256 as SHA256 (hash)
import Data.Attoparsec.ByteString (parseOnly, word8)
import qualified Data.ByteString (last)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)
import Data.Either (rights)
import Data.Int (Int32, Int64)
import Data.Maybe (listToMaybe)
import Data.Serialize.Put (putWord8, runPut)
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Kafkaesque.Message (Message(..), messageParser, messageV0)
import Kafkaesque.Parsers (kafkaString, signedInt64be)
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
  _ <- writeMessageSet conn offsetTopicId offsetPartitionId [message]
  return ()

getOffsetForConsumer ::
     PG.Connection
  -> String
  -> String
  -> Int32
  -> IO (Maybe (Either String (Int64, String)))
getOffsetForConsumer conn cgId topicName partitionId = do
  let key = consumerOffsetKey cgId topicName partitionId
      query =
        [sql| SELECT record FROM records WHERE topic_id = ? AND partition_id = ? ORDER BY log_offset DESC |]
      matchesKey (Message _ _ _ k _) = Just key == k
      valueParser =
        (\_ offset metadata -> (offset, metadata)) <$> word8 0 <*> signedInt64be <*>
        kafkaString
      extractOffsetAndMetadata (Message _ _ _ _ maybeV) =
        case maybeV of
          Nothing -> Left "Value in offset commit message is empty"
          Just v ->
            case parseOnly valueParser v of
              Left _ -> Left "Cannot parse value of offset commit message"
              Right offsetAndMetadata -> Right offsetAndMetadata
      getLatestOffset =
        listToMaybe .
        map extractOffsetAndMetadata .
        filter matchesKey .
        rights . map (\(PG.Only bs) -> parseOnly messageParser bs)
  Just (offsetTopicId, offsetPartitionId) <-
    getConsumerOffsetTopicPartition conn cgId
  res <-
    PG.query conn query (offsetTopicId, offsetPartitionId) :: IO [PG.Only ByteString]
  return . getLatestOffset $ res
