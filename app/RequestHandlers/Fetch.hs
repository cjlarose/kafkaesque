{-# LANGUAGE QuasiQuotes #-}

module RequestHandlers.Fetch
  ( respondToRequest
  ) where

import Control.Monad (forM)
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Kafkaesque.Request
       (ApiVersion(..), FetchRequestPartition, FetchRequestTopic,
        KafkaRequest(FetchRequest))
import Kafkaesque.Response
       (FetchResponsePartition, FetchResponseTopic,
        KafkaError(NoError, OffsetOutOfRange, UnknownTopicOrPartition),
        KafkaResponse(FetchResponseV0))
import RequestHandlers.Queries (getNextOffset, getTopicPartition)

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
            firstByteOffset + fromIntegral maxBytes - (firstMessageLength + 12)
      PG.query
        conn
        messageSetQuery
        (topicId, partitionId, firstByteOffset, maxEndOffset)
    _ -> return []

fetchTopicPartition ::
     PG.Connection
  -> String
  -> FetchRequestPartition
  -> IO FetchResponsePartition
fetchTopicPartition conn topicName (partitionId, offset, maxBytes) = do
  topicPartitionRes <- getTopicPartition conn topicName partitionId
  maybe
    (return ((partitionId, UnknownTopicOrPartition, -1 :: Int64), []))
    (\(topicId, partitionId) -> do
       nextOffset <- getNextOffset conn topicId partitionId
       highwaterMarkOffset <- getNextOffset conn topicId partitionId
       if offset >= nextOffset
         then return
                ((partitionId, OffsetOutOfRange, highwaterMarkOffset - 1), [])
         else do
           messageSet <- fetchMessages conn topicId partitionId offset maxBytes
           let header = (partitionId, NoError, highwaterMarkOffset - 1)
           return (header, messageSet))
    topicPartitionRes

fetchTopic :: PG.Connection -> FetchRequestTopic -> IO FetchResponseTopic
fetchTopic conn (topicName, parts) = do
  partResponses <- forM parts (fetchTopicPartition conn topicName)
  return (topicName, partResponses)

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool (FetchRequest (ApiVersion 0) _ _ _ ts)
  -- TODO: Respect maxWaitTime
  -- TODO: Respect minBytes
  -- TODO: Fetch topicIds in bulk
 = FetchResponseV0 <$> Pool.withResource pool (forM ts . fetchTopic)
