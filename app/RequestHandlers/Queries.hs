{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module RequestHandlers.Queries
  ( getTopicId
  , getPartitionCount
  , getTopicPartition
  , getTopicsWithPartitionCounts
  ) where

import Data.Int (Int32, Int64)
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

getTopicId :: PG.Connection -> String -> IO (Maybe Int32)
getTopicId conn topicName = do
  let query = "SELECT id FROM topics WHERE name = ?"
  res <- PG.query conn query (PG.Only topicName) :: IO [PG.Only Int32]
  case res of
    [PG.Only topicId] -> return . Just $ topicId
    _ -> return Nothing

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
