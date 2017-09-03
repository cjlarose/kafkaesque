{-# LANGUAGE OverloadedStrings #-}

module RequestHandlers.Produce
  ( respondToRequest
  ) where

import Control.Monad (forM)
import Data.ByteString (ByteString)
import qualified Data.ByteString (length)
import Data.Int (Int32, Int64)
import Data.List (foldl')
import qualified Data.Pool as Pool
import Data.Serialize.Put (runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Message (Message, MessageSet)
import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(ProduceRequest))
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        KafkaResponse(ProduceResponseV0, ProduceResponseV1), putMessage)
import RequestHandlers.Queries (getTopicPartition)

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

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
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
