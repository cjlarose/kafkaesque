module Kafkaesque.Request.TopicMetadata
  ( metadataRequest
  ) where

import Data.Attoparsec.ByteString (Parser)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest (KafkaRequest, respond)
import Kafkaesque.Request.Parsers (kafkaArray, kafkaString)
import Kafkaesque.Request.Queries (getTopicsWithPartitionCounts)
import Kafkaesque.Response
       (Broker(..), KafkaError(NoError),
        KafkaResponse(TopicMetadataResponseV0), PartitionMetadata(..),
        TopicMetadata(..))

data TopicMetadataRequest =
  TopicMetadataRequest ApiVersion
                       (Maybe [String])

metadataRequest :: ApiVersion -> Parser TopicMetadataRequest
metadataRequest (ApiVersion v)
  | v <= 3 = TopicMetadataRequest (ApiVersion v) <$> kafkaArray kafkaString

respondToRequest ::
     Pool.Pool PG.Connection -> TopicMetadataRequest -> IO KafkaResponse
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

instance KafkaRequest TopicMetadataRequest where
  respond = respondToRequest
