module Kafkaesque.Request.OffsetCommit
  ( offsetCommitRequestV0
  ) where

import Control.Monad (forM)
import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError
       (KafkaError(NoError, UnknownTopicOrPartition))
import Kafkaesque.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.ConsumerOffsets (saveOffset)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response (OffsetCommitResponseV0(..))

type PartitionData = (Int32, Int64, String)

type TopicData = (String, [PartitionData])

data OffsetCommitRequestV0 =
  OffsetCommitRequestV0 String
                        [TopicData]

offsetCommitPartition :: Parser PartitionData
offsetCommitPartition =
  (\partitionId offset metadata -> (partitionId, offset, metadata)) <$>
  signedInt32be <*>
  signedInt64be <*>
  kafkaString

offsetCommitTopic :: Parser TopicData
offsetCommitTopic =
  (\a b -> (a, b)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitPartition)

offsetCommitRequestV0 :: Parser OffsetCommitRequestV0
offsetCommitRequestV0 =
  OffsetCommitRequestV0 <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetCommitTopic)

respondToRequest ::
     Pool.Pool PG.Connection
  -> OffsetCommitRequestV0
  -> IO OffsetCommitResponseV0
respondToRequest pool (OffsetCommitRequestV0 cgId topics) = do
  let getResponsePartition conn topicName (partitionId, offset, metadata) = do
        topicRes <- getTopicPartition conn topicName partitionId
        maybe
          (return (partitionId, UnknownTopicOrPartition))
          (const $ do
             saveOffset conn cgId topicName partitionId offset metadata
             return (partitionId, NoError))
          topicRes
      getResponseTopic conn (topicName, partitions) = do
        partsResponse <- forM partitions (getResponsePartition conn topicName)
        return (topicName, partsResponse)
      getResponseTopics conn = forM topics (getResponseTopic conn)
  responseTopics <- Pool.withResource pool getResponseTopics
  return $ OffsetCommitResponseV0 responseTopics

instance KafkaRequest OffsetCommitRequestV0 where
  respond pool req = KResp <$> respondToRequest pool req
