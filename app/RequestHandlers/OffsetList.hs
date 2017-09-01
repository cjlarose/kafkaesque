{-# LANGUAGE QuasiQuotes #-}

module RequestHandlers.OffsetList
  ( respondToRequest
  ) where

import Data.Int (Int32, Int64)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(OffsetListRequest),
        OffsetListRequestPartition, OffsetListRequestTopic, OffsetListRequestTimestamp(LatestOffset, EarliestOffset))
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        KafkaResponse(OffsetListResponseVO), OffsetListResponsePartition,
        OffsetListResponseTopic)
import RequestHandlers.Queries (getTopicPartition)

getEarliestOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getEarliestOffset conn topicId partitionId = do
  let query = [sql| SELECT MIN(log_offset) FROM records WHERE topic_id = ? AND partition_id = ? |]
  [PG.Only min] <- PG.query conn query (topicId, partitionId)
  return min

getNextOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getNextOffset conn topicId partitionId = do
  let query = [sql| SELECT next_offset FROM partitions WHERE topic_id = ? AND partition_id = ? |]
  [PG.Only next] <- PG.query conn query (topicId, partitionId)
  return next

fetchTopicPartitionOffsets ::
     PG.Connection -> Int32 -> Int32 -> OffsetListRequestTimestamp -> Int32 -> IO [Int64]
fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets = do
  earliest <- getEarliestOffset conn topicId partitionId
  latest <- getNextOffset conn topicId partitionId
  -- TODO handle actual timestamp offsets
  let offsets = case timestamp of
                  LatestOffset -> [latest, earliest]
                  EarliestOffset -> [earliest]
  return . take (fromIntegral maxOffsets) $ offsets

fetchPartitionOffsets ::
     PG.Connection
  -> String
  -> OffsetListRequestPartition
  -> IO OffsetListResponsePartition
fetchPartitionOffsets conn topicName (partitionId, timestamp, maxOffsets) = do
  res <- getTopicPartition conn topicName partitionId
  case res of
    Nothing -> return (partitionId, UnknownTopicOrPartition, Nothing)
    Just (topicId, _) -> do
      offsets <-
        fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets
      return (partitionId, NoError, Just offsets)

fetchTopicOffsets :: PG.Connection -> OffsetListRequestTopic -> IO OffsetListResponseTopic
fetchTopicOffsets conn (topicName, partitions) = do
  partitionResponses <- mapM (fetchPartitionOffsets conn topicName) partitions
  return (topicName, partitionResponses)

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool (OffsetListRequest (ApiVersion 0) _ topics) = do
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (fetchTopicOffsets conn) topics)
  return $ OffsetListResponseVO topicResponses
