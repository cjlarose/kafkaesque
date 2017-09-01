module RequestHandlers.Metadata
  ( respondToRequest
  ) where

import qualified Data.Pool as Pool

import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(TopicMetadataRequest))
import Kafkaesque.Response
       (Broker(..), KafkaError(NoError),
        KafkaResponse(TopicMetadataResponseV0), PartitionMetadata(..),
        TopicMetadata(..))
import RequestHandlers.Queries (getTopicsWithPartitionCounts)

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
