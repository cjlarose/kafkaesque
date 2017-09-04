{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Kafkaesque.Request.Fetch
  ( fetchRequestV0
  ) where

import Control.Monad (forM)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Data.Attoparsec.ByteString (Parser)
import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Request.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Request.Queries
       (getNextOffset, getTopicPartition)
import Kafkaesque.Response
       (FetchResponsePartition, FetchResponseTopic, FetchResponseV0(..),
        KafkaError(NoError, OffsetOutOfRange, UnknownTopicOrPartition))

type FetchRequestPartition = (Int32, Int64, Int32)

type FetchRequestTopic = (String, [FetchRequestPartition])

data FetchRequestV0 =
  FetchRequestV0 Int32
                 Int32
                 Int32
                 [FetchRequestTopic]

fetchRequestPartition :: Parser FetchRequestPartition
fetchRequestPartition =
  (\a b c -> (a, b, c)) <$> signedInt32be <*> signedInt64be <*> signedInt32be

fetchRequestTopic :: Parser FetchRequestTopic
fetchRequestTopic =
  (\topic partitions -> (topic, partitions)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray fetchRequestPartition)

fetchRequestV0 :: Parser FetchRequestV0
fetchRequestV0 =
  FetchRequestV0 <$> signedInt32be <*> signedInt32be <*> signedInt32be <*>
  (fromMaybe [] <$> kafkaArray fetchRequestTopic)

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

respondToRequest ::
     Pool.Pool PG.Connection -> FetchRequestV0 -> IO FetchResponseV0
respondToRequest pool (FetchRequestV0 _ _ _ ts)
  -- TODO: Respect maxWaitTime
  -- TODO: Respect minBytes
  -- TODO: Fetch topicIds in bulk
 = FetchResponseV0 <$> Pool.withResource pool (forM ts . fetchTopic)

instance KafkaRequest FetchRequestV0 FetchResponseV0 where
  respond = respondToRequest
