module Kafkaesque.Request.TopicMetadata
  ( metadataRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError (noError)
import Kafkaesque.Parsers (kafkaArray, kafkaString)
import Kafkaesque.Queries (getTopicsWithPartitionCounts)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response
       (Broker(..), PartitionMetadata(..), TopicMetadata(..),
        TopicMetadataResponseV0(..))

newtype TopicMetadataRequestV0 =
  TopicMetadataRequestV0 (Maybe [String])

metadataRequestV0 :: Parser TopicMetadataRequestV0
metadataRequestV0 = TopicMetadataRequestV0 <$> kafkaArray kafkaString

respondToRequest ::
     Pool.Pool PG.Connection -> TopicMetadataRequestV0 -> IO KafkaResponseBox
respondToRequest pool (TopicMetadataRequestV0 ts) = do
  let brokerNodeId = 42
  let brokers = [Broker brokerNodeId "localhost" 9092]
  let makePartitionMetadata partitionId =
        PartitionMetadata
          noError
          (fromIntegral partitionId)
          brokerNodeId
          [brokerNodeId]
          [brokerNodeId]
  let makeTopicMetadata (name, partitionCount) =
        TopicMetadata
          noError
          name
          (map makePartitionMetadata [0 .. (partitionCount - 1)])
  topics <- Pool.withResource pool getTopicsWithPartitionCounts
  let topicMetadata = map makeTopicMetadata topics
  return . KResp $ TopicMetadataResponseV0 brokers topicMetadata

instance KafkaRequest TopicMetadataRequestV0 where
  respond = respondToRequest
