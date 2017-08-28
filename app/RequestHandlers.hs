{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module RequestHandlers
  ( handleRequest
  ) where

import Control.Monad (forM, forM_)
import Data.Attoparsec.ByteString (endOfInput, parseOnly)
import Data.ByteString (ByteString)
import qualified Data.ByteString
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int32, Int64)
import qualified Data.Pool as Pool
import Data.Serialize.Put (putByteString, putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Kafkaesque.Message (Message, MessageSet)
import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(..), kafkaRequest)
import Kafkaesque.Response
       (Broker(..), KafkaError(..), KafkaResponse(..),
        PartitionMetadata(..), TopicMetadata(..), putMessage,
        writeResponse)

getTopicId :: PG.Connection -> String -> IO (Maybe Int32)
getTopicId conn topicName = do
  let query = "SELECT id FROM topics WHERE name = ?"
  res <- PG.query conn query (PG.Only topicName) :: IO [PG.Only Int32]
  case res of
    [PG.Only topicId] -> return . Just $ topicId
    _ -> return Nothing

getNextOffsetsForUpdate :: PG.Connection -> Int32 -> Int32 -> IO (Int64, Int64)
getNextOffsetsForUpdate conn topicId partitionId = do
  let query =
        "SELECT next_offset, total_bytes FROM partitions WHERE topic_id = ? AND partition_id = ? FOR UPDATE"
  res <- PG.query conn query (topicId, partitionId) :: IO [(Int64, Int64)]
  return . head $ res

getNextOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getNextOffset conn topicId partitionId = do
  let query =
        "SELECT next_offset FROM partitions WHERE topic_id = ? AND partition_id = ?"
  [PG.Only res] <- PG.query conn query (topicId, partitionId)
  return res

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
        foldl
          (\(tuples, logOffset, currentTotalBytes) (_, message) ->
             let messageBytes = runPut $ putMessage message
                 messageLen =
                   fromIntegral (Data.ByteString.length messageBytes) :: Int64
                 endByteOffset = currentTotalBytes + messageLen
                 tuple =
                   ( topicId
                   , partitionId
                   , PG.Binary messageBytes
                   , logOffset
                   , endByteOffset)
             in (tuple : tuples, logOffset + 1, endByteOffset))
          ([], baseOffset, totalBytes)
          messages
  let query =
        "INSERT INTO records (topic_id, partition_id, record, log_offset, byte_offset) VALUES (?, ?, ?, ?, ?)"
  PG.executeMany conn query newTuples
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

getPartitionCount :: PG.Connection -> Int32 -> IO Int64
getPartitionCount conn topicId = do
  let query = "SELECT COUNT(*) FROM partitions WHERE topic_id = ?"
  [PG.Only row] <- PG.query conn query (PG.Only topicId)
  return row

getTopicPartition ::
     PG.Connection -> String -> Int32 -> IO (Maybe (Int32, Int32))
getTopicPartition conn topicName partitionId
  -- TODO: Prevent multiple queries for partition count during produce or fetch requests
 = do
  let validatePartition topicId = do
        partitionCount <- getPartitionCount conn topicId
        if partitionId >= 0 && fromIntegral partitionId < partitionCount
          then return . Just $ (topicId, partitionId)
          else return Nothing
  topicIdRes <- getTopicId conn topicName
  maybe (return Nothing) validatePartition topicIdRes

getTopicsWithPartitionCounts :: PG.Connection -> IO [(String, Int64)]
getTopicsWithPartitionCounts conn = do
  let query =
        [sql| SELECT name, partition_count
              FROM (SELECT topic_id, COUNT(*) AS partition_count
                    FROM partitions
                    GROUP BY topic_id) t1
              LEFT JOIN topics ON t1.topic_id = topics.id; |]
  PG.query_ conn query

fetchMessages ::
     PG.Connection
  -> Int32
  -> Int32
  -> Int64
  -> Int32
  -> IO [(Int64, ByteString)]
fetchMessages conn topicId partitionId startOffset maxBytes = do
  let firstMessageQuery =
        [sql| SELECT byte_offset, octet_length(record)
              FROM records
              WHERE topic_id = ?
              AND partition_id = ?
              AND log_offset = ? |]
  resFirstMessage <-
    PG.query conn firstMessageQuery (topicId, partitionId, startOffset) :: IO [( Int64
                                                                               , Int64)]
  case resFirstMessage of
    [(firstByteOffset, firstMessageLength)] -> do
      let messageSetQuery =
            [sql| SELECT log_offset, record
                  FROM records
                  WHERE topic_id = ?
                  AND partition_id = ?
                  AND byte_offset BETWEEN ? AND ?
                  ORDER BY byte_offset |]
      let maxEndOffset =
            firstByteOffset + fromIntegral maxBytes - firstMessageLength
      PG.query
        conn
        messageSetQuery
        (topicId, partitionId, firstByteOffset, maxEndOffset)
    _ -> return []

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
respondToRequest pool (FetchRequest (ApiVersion 0) _ _ _ ts)
  -- TODO: Respect maxWaitTime
  -- TODO: Respect minBytes
  -- TODO: Fetch topicIds in bulk
 = do
  topicResponses <-
    forM
      ts
      (\(topic, parts) -> do
         partResponses <-
           forM
             parts
             (\(partitionId, offset, maxBytes) ->
                Pool.withResource
                  pool
                  (\conn -> do
                     topicPartitionRes <-
                       Pool.withResource
                         pool
                         (\conn -> getTopicPartition conn topic partitionId)
                     maybe
                       (return
                          ( (partitionId, UnknownTopicOrPartition, -1 :: Int64)
                          , []))
                       (\(topicId, partitionId) -> do
                          messageSet <-
                            fetchMessages
                              conn
                              topicId
                              partitionId
                              offset
                              maxBytes
                          highwaterMarkOffset <-
                            getNextOffset conn topicId partitionId
                          let header =
                                (partitionId, NoError, highwaterMarkOffset - 1)
                          return (header, messageSet))
                       topicPartitionRes))
         return (topic, partResponses))
  return . FetchResponseV0 $ topicResponses
respondToRequest pool (TopicMetadataRequest (ApiVersion 0) ts) = do
  let brokerNodeId = 42
  let brokers = [Broker brokerNodeId "localhost" 9092]
  let makePartitionMetadata partitionId =
        PartitionMetadata
          NoError
          (fromIntegral partitionId)
          brokerNodeId
          [brokerNodeId]
          [brokerNodeId]
  let makeTopicMetadata (name, partitionCount) =
        TopicMetadata
          NoError
          name
          (map makePartitionMetadata [0 .. (partitionCount - 1)])
  topics <- Pool.withResource pool getTopicsWithPartitionCounts
  let topicMetadata = map makeTopicMetadata topics
  return $ TopicMetadataResponseV0 brokers topicMetadata
respondToRequest pool (ApiVersionsRequest (ApiVersion 0) apiKeys) =
  return $
  ApiVersionsResponseV0 NoError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]

handleRequest ::
     Pool.Pool PG.Connection -> ByteString -> Either String (IO ByteString)
handleRequest pool request = do
  let parseResult = parseOnly (kafkaRequest <* endOfInput) request
      generateResponse ((_, _, correlationId, _), req) = do
        response <- respondToRequest pool req
        let putCorrelationId = putWord32be . fromIntegral $ correlationId
            putResponse = putByteString . writeResponse $ response
        return . runPut $ putCorrelationId *> putResponse
  generateResponse <$> parseResult
