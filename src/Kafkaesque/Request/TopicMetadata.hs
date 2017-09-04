{-# LANGUAGE MultiParamTypeClasses #-}

module Kafkaesque.Request.TopicMetadata
  ( metadataRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Request.Parsers (kafkaArray, kafkaString)
import Kafkaesque.Request.Queries (getTopicsWithPartitionCounts)
import Kafkaesque.Response
       (Broker(..), KafkaError(NoError), PartitionMetadata(..),
        TopicMetadata(..), TopicMetadataResponseV0(..))

newtype TopicMetadataRequestV0 =
  TopicMetadataRequestV0 (Maybe [String])

metadataRequestV0 :: Parser TopicMetadataRequestV0
metadataRequestV0 = TopicMetadataRequestV0 <$> kafkaArray kafkaString

respondToRequest ::
     Pool.Pool PG.Connection -> TopicMetadataRequestV0 -> IO TopicMetadataResponseV0
respondToRequest pool (TopicMetadataRequestV0 ts) = do
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

instance KafkaRequest TopicMetadataRequestV0 TopicMetadataResponseV0 where
  respond = respondToRequest
