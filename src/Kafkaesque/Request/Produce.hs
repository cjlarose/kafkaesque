{-# LANGUAGE OverloadedStrings #-}

module Kafkaesque.Request.Produce
  ( produceRequestV0
  , produceRequestV1
  ) where

import Data.Attoparsec.Binary (anyWord32be)
import Data.Attoparsec.ByteString
       (Parser, anyWord8, many', parseOnly, take)
import Data.ByteString (ByteString)
import qualified Data.ByteString (length)
import Data.Int (Int16, Int32, Int64)
import Data.List (foldl')
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import Data.Serialize.Put (runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Message (Message(..), MessageSet)
import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Request.Parsers
       (kafkaArray, kafkaNullabeBytes, kafkaString, signedInt16be,
        signedInt32be, signedInt64be)
import Kafkaesque.Request.Queries (getTopicPartition)
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        ProduceResponseTopic, ProduceResponseV0(..), ProduceResponseV1(..),
        putMessage)

newtype TimeoutMs =
  TimeoutMs Int32

type PartitionData = (Int32, MessageSet)

type TopicData = (String, [PartitionData])

data ProduceRequestV0 =
  ProduceRequestV0 Int16
                   TimeoutMs
                   [TopicData]

data ProduceRequestV1 =
  ProduceRequestV1 Int16
                   TimeoutMs
                   [TopicData]

produceRequest :: Parser (Int16, TimeoutMs, [TopicData])
produceRequest =
  let message :: Parser Message
      message =
        Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*> kafkaNullabeBytes <*>
        kafkaNullabeBytes
      mesasgeSetElement :: Parser (Int64, Message)
      mesasgeSetElement =
        (\x y -> (x, y)) <$> (signedInt64be <* signedInt32be) <*> message
      records :: Parser MessageSet
      records = do
        messageSetSize <- signedInt32be
        bs <- Data.Attoparsec.ByteString.take . fromIntegral $ messageSetSize
        case parseOnly (many' mesasgeSetElement) bs of
          Left err -> fail err
          Right xs -> return xs
      partitionData :: Parser PartitionData
      partitionData =
        (\partitionId msgs -> (partitionId, msgs)) <$> signedInt32be <*> records
      topicData :: Parser TopicData
      topicData =
        (\name parts -> (name, parts)) <$> kafkaString <*>
        (fromMaybe [] <$> kafkaArray partitionData)
  in (\a b c -> (a, b, c)) <$> signedInt16be <*> (TimeoutMs <$> signedInt32be) <*>
     (fromMaybe [] <$> kafkaArray topicData)

produceRequestV0 :: Parser ProduceRequestV0
produceRequestV0 =
  (\(acks, timeout, topics) -> ProduceRequestV0 acks timeout topics) <$>
  produceRequest

produceRequestV1 :: Parser ProduceRequestV1
produceRequestV1 =
  (\(acks, timeout, topics) -> ProduceRequestV1 acks timeout topics) <$>
  produceRequest

getNextOffsetsForUpdate :: PG.Connection -> Int32 -> Int32 -> IO (Int64, Int64)
getNextOffsetsForUpdate conn topicId partitionId = do
  let query =
        "SELECT next_offset, total_bytes FROM partitions WHERE topic_id = ? AND partition_id = ? FOR UPDATE"
  res <- PG.query conn query (topicId, partitionId) :: IO [(Int64, Int64)]
  return . head $ res

insertMessages ::
     PG.Connection
  -> Int32
  -> Int32
  -> Int64
  -> Int64
  -> [(Int64, Message)]
  -> IO (Int64, Int64)
insertMessages conn topicId partitionId baseOffset totalBytes messages = do
  let (newTuples, finalOffset, finalTotalBytes) =
        foldl'
          (\(f, logOffset, currentTotalBytes) (_, message) ->
             let messageBytes = runPut $ putMessage message
                 messageLen =
                   fromIntegral (Data.ByteString.length messageBytes) :: Int64
                 endByteOffset = currentTotalBytes + messageLen + 12
                 tuple =
                   ( topicId
                   , partitionId
                   , PG.Binary messageBytes
                   , logOffset
                   , endByteOffset)
             in (f . (tuple :), logOffset + 1, endByteOffset))
          (id, baseOffset, totalBytes)
          messages
  let query =
        "INSERT INTO records (topic_id, partition_id, record, log_offset, byte_offset) VALUES (?, ?, ?, ?, ?)"
  PG.executeMany conn query $ newTuples []
  return (finalOffset, finalTotalBytes)

updatePartitionOffsets ::
     PG.Connection -> Int32 -> Int32 -> Int64 -> Int64 -> IO ()
updatePartitionOffsets conn topicId partitionId nextOffset totalBytes = do
  let query =
        "UPDATE partitions SET next_offset = ?, total_bytes = ? WHERE topic_id = ? AND partition_id = ?"
  PG.execute conn query (nextOffset, totalBytes, topicId, partitionId)
  return ()

writeMessageSet :: PG.Connection -> Int32 -> Int32 -> MessageSet -> IO Int64
writeMessageSet conn topicId partition messages =
  PG.withTransaction conn $ do
    (baseOffset, totalBytes) <- getNextOffsetsForUpdate conn topicId partition
    (finalOffset, finalTotalBytes) <-
      insertMessages conn topicId partition baseOffset totalBytes messages
    updatePartitionOffsets conn topicId partition finalOffset finalTotalBytes
    return baseOffset

respondToRequest ::
     Pool.Pool PG.Connection -> [TopicData] -> IO [ProduceResponseTopic]
respondToRequest pool
  -- TODO: Fetch topicIds in bulk
 = do
  let writePartitionMessages topicName partitionId messageSet conn = do
        topicPartitionRes <- getTopicPartition conn topicName partitionId
        maybe
          (return (UnknownTopicOrPartition, -1 :: Int64))
          (\(topicId, partitionId) -> do
             offset <- writeMessageSet conn topicId partitionId messageSet
             return (NoError, offset))
          topicPartitionRes
      getPartitionResponse topicName (partitionId, messageSet) = do
        (err, offset) <-
          Pool.withResource
            pool
            (writePartitionMessages topicName partitionId messageSet)
        return (partitionId, err, offset)
      getTopicResponse (topicName, parts) = do
        partResponses <- mapM (getPartitionResponse topicName) parts
        return (topicName, partResponses)
  mapM getTopicResponse

instance KafkaRequest ProduceRequestV0 where
  respond pool (ProduceRequestV0 _ _ topics) = do
    topicResponses <- respondToRequest pool topics
    return . KResp $ ProduceResponseV0 topicResponses

instance KafkaRequest ProduceRequestV1 where
  respond pool (ProduceRequestV1 _ _ topics) = do
    topicResponses <- respondToRequest pool topics
    let throttleTimeMs = 0 :: Int32
    return . KResp $ ProduceResponseV1 topicResponses throttleTimeMs
