{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.OffsetCommit
  ( offsetCommitRequestV0
  , respondToRequestV0
  ) where

import Control.Monad (forM)
import Data.Attoparsec.ByteString (Parser)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError (noError, unknownTopicOrPartition)
import Kafkaesque.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.ConsumerOffsets (saveOffset)
import Kafkaesque.Request.KafkaRequest
       (APIKeyOffsetCommit, APIVersion0, OffsetCommitPartitionData,
        OffsetCommitTopicData, Request(OffsetCommitRequestV0),
        Response(OffsetCommitResponseV0))

offsetCommitPartition :: Parser OffsetCommitPartitionData
offsetCommitPartition =
  (\partitionId offset metadata -> (partitionId, offset, metadata)) <$>
  signedInt32be <*>
  signedInt64be <*>
  kafkaString

offsetCommitTopic :: Parser OffsetCommitTopicData
offsetCommitTopic =
  (\a b -> (a, b)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitPartition)

offsetCommitRequestV0 :: Parser (Request APIKeyOffsetCommit APIVersion0)
offsetCommitRequestV0 =
  OffsetCommitRequestV0 <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitTopic)

respondToRequestV0 ::
     Pool.Pool PG.Connection
  -> Request APIKeyOffsetCommit APIVersion0
  -> IO (Response APIKeyOffsetCommit APIVersion0)
respondToRequestV0 pool (OffsetCommitRequestV0 cgId topics) = do
  let getResponsePartition conn topicName (partitionId, offset, metadata) = do
        topicRes <- getTopicPartition conn topicName partitionId
        maybe
          (return (partitionId, unknownTopicOrPartition))
          (const $ do
             saveOffset conn cgId topicName partitionId offset metadata
             return (partitionId, noError))
          topicRes
      getResponseTopic conn (topicName, partitions) = do
        partsResponse <- forM partitions (getResponsePartition conn topicName)
        return (topicName, partsResponse)
      getResponseTopics conn = forM topics (getResponseTopic conn)
  responseTopics <- Pool.withResource pool getResponseTopics
  return $ OffsetCommitResponseV0 responseTopics
