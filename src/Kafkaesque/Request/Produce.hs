{-# LANGUAGE OverloadedStrings #-}

module Kafkaesque.Request.Produce
  ( produceRequest
  ) where

import Control.Monad (forM)
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
import Kafkaesque.Request.KafkaRequest (KafkaRequest, respond)
import Kafkaesque.Request.Parsers
       (kafkaArray, kafkaNullabeBytes, kafkaString, signedInt16be,
        signedInt32be, signedInt64be)
import Kafkaesque.Request.Queries (getTopicPartition)
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        KafkaResponse(ProduceResponseV0, ProduceResponseV1), putMessage)

newtype TimeoutMs =
  TimeoutMs Int32

type PartitionData = (Int32, MessageSet)

type TopicData = (String, [PartitionData])

data ProduceRequest =
  ProduceRequest ApiVersion
                 Int16
                 TimeoutMs
                 [TopicData]

produceRequest :: ApiVersion -> Parser ProduceRequest
produceRequest (ApiVersion v)
  | v <= 2 =
    let message :: Parser Message
        message =
          Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*>
          kafkaNullabeBytes <*>
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
          (\partitionId msgs -> (partitionId, msgs)) <$> signedInt32be <*>
          records
        topicData :: Parser TopicData
        topicData =
          (\name parts -> (name, parts)) <$> kafkaString <*>
          (fromMaybe [] <$> kafkaArray partitionData)
    in ProduceRequest (ApiVersion v) <$> signedInt16be <*>
       (TimeoutMs <$> signedInt32be) <*>
       (fromMaybe [] <$> kafkaArray topicData)

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

writeMessageSet ::
     PG.Connection -> Int32 -> Int32 -> MessageSet -> IO (KafkaError, Int64)
writeMessageSet conn topicId partition messages = do
  baseOffset <-
    PG.withTransaction conn $ do
      (baseOffset, totalBytes) <- getNextOffsetsForUpdate conn topicId partition
      (finalOffset, finalTotalBytes) <-
        insertMessages conn topicId partition baseOffset totalBytes messages
      updatePartitionOffsets conn topicId partition finalOffset finalTotalBytes
      return baseOffset
  return (NoError, baseOffset)

respondToRequest ::
     Pool.Pool PG.Connection -> ProduceRequest -> IO KafkaResponse
respondToRequest pool (ProduceRequest (ApiVersion v) acks timeout ts)
  -- TODO: Fetch topicIds in bulk
 = do
  topicResponses <-
    forM
      ts
      (\(topic, parts) -> do
         partResponses <-
           forM
             parts
             (\(partitionId, messageSet) -> do
                (err, offset) <-
                  Pool.withResource
                    pool
                    (\conn -> do
                       topicPartitionRes <-
                         getTopicPartition conn topic partitionId
                       maybe
                         (return (UnknownTopicOrPartition, -1 :: Int64))
                         (\(topicId, partitionId) ->
                            writeMessageSet conn topicId partitionId messageSet)
                         topicPartitionRes)
                return (partitionId, err, offset))
         return (topic, partResponses))
  case v of
    0 -> return $ ProduceResponseV0 topicResponses
    1 -> do
      let throttleTimeMs = 0 :: Int32
      return $ ProduceResponseV1 topicResponses throttleTimeMs

instance KafkaRequest ProduceRequest where
  respond = respondToRequest
