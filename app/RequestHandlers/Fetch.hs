{-# LANGUAGE OverloadedStrings #-}
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
       (ApiVersion(..), KafkaRequest(FetchRequest))
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        KafkaResponse(FetchResponseV0))
import RequestHandlers.Queries (getTopicPartition)

getNextOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getNextOffset conn topicId partitionId = do
  let query =
        "SELECT next_offset FROM partitions WHERE topic_id = ? AND partition_id = ?"
  [PG.Only res] <- PG.query conn query (topicId, partitionId)
  return res

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
