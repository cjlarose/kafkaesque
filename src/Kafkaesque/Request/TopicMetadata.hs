{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.TopicMetadata
  ( metadataRequestV0
  , respondToRequestV0
  ) where

import Control.Arrow (second)
import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int64)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.ApiKey (Metadata)
import Kafkaesque.KafkaError (noError, unknownTopicOrPartition)
import Kafkaesque.Parsers (kafkaArray, kafkaString)
import Kafkaesque.Queries
       (getAllTopicsWithPartitionCounts, getPartitionCount, getTopicId)
import Kafkaesque.Request.KafkaRequest
       (APIVersion0, Broker(..), PartitionMetadata(..),
        Request(MetadataRequestV0), Response(MetadataResponseV0),
        TopicMetadata(..))

metadataRequestV0 :: Parser (Request Metadata APIVersion0)
metadataRequestV0 = MetadataRequestV0 <$> kafkaArray kafkaString

getTopicWithPartitionCount :: PG.Connection -> String -> IO (Maybe Int64)
getTopicWithPartitionCount conn topicName = do
  topicIdRes <- getTopicId conn topicName
  maybe (return Nothing) (fmap Just . getPartitionCount conn) topicIdRes

getTopicsWithPartitionCounts ::
     PG.Connection -> [String] -> IO [(String, Maybe Int64)]
getTopicsWithPartitionCounts conn =
  mapM
    (\topicName -> do
       count <- getTopicWithPartitionCount conn topicName
       return (topicName, count))

respondToRequestV0 ::
     Pool.Pool PG.Connection
  -> Request Metadata APIVersion0
  -> IO (Response Metadata APIVersion0)
respondToRequestV0 pool (MetadataRequestV0 requestedTopics) = do
  let brokerNodeId = 42
  let brokers = [Broker brokerNodeId "localhost" 9092]
  let makePartitionMetadata partitionId =
        PartitionMetadata
          noError
          (fromIntegral partitionId)
          brokerNodeId
          [brokerNodeId]
          [brokerNodeId]
  let makeTopicMetadata (name, Nothing) =
        TopicMetadata unknownTopicOrPartition name []
      makeTopicMetadata (name, Just partitionCount) =
        TopicMetadata
          noError
          name
          (map makePartitionMetadata [0 .. (partitionCount - 1)])
  let getTopics :: PG.Connection -> Maybe [String] -> IO [(String, Maybe Int64)]
      getTopics conn topics =
        case topics of
          Nothing -> map (second Just) <$> getAllTopicsWithPartitionCounts conn
          Just [] -> map (second Just) <$> getAllTopicsWithPartitionCounts conn
          Just xs -> getTopicsWithPartitionCounts conn xs
  topics <- Pool.withResource pool (`getTopics` requestedTopics)
  let topicMetadata = map makeTopicMetadata topics
  return $ MetadataResponseV0 brokers topicMetadata
