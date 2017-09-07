{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.OffsetFetch
  ( offsetFetchRequestV0
  , respondToRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError
       (noError, unexpectedError, unknownTopicOrPartition)
import Kafkaesque.Parsers (kafkaArray, kafkaString, signedInt32be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.ConsumerOffsets (getOffsetForConsumer)
import Kafkaesque.Request.KafkaRequest
       (APIKeyOffsetFetch, APIVersion0, Request(OffsetFetchRequestV0),
        Response(OffsetFetchResponseV0))

offsetFetchRequestV0 :: Parser (Request APIKeyOffsetFetch APIVersion0)
offsetFetchRequestV0 =
  let topic =
        (\a b -> (a, b)) <$> kafkaString <*>
        (fromMaybe [] <$> kafkaArray signedInt32be)
  in OffsetFetchRequestV0 <$> kafkaString <*>
     (fromMaybe [] <$> kafkaArray topic)

respondToRequestV0 ::
     Pool.Pool PG.Connection
  -> Request APIKeyOffsetFetch APIVersion0
  -> IO (Response APIKeyOffsetFetch APIVersion0)
respondToRequestV0 pool (OffsetFetchRequestV0 cgId topics) = do
  let getPartitionResponse conn topicName partitionId = do
        topicPartitionRes <- getTopicPartition conn topicName partitionId
        case topicPartitionRes of
          Nothing -> return (partitionId, -1, "", unknownTopicOrPartition)
          Just _ -> do
            offsetRes <- getOffsetForConsumer conn cgId topicName partitionId
            case offsetRes of
              Nothing -> return (partitionId, -1, "", noError)
              Just parseResult ->
                case parseResult of
                  Left _ -> return (partitionId, -1, "", unexpectedError)
                  Right (offset, metadata) ->
                    return (partitionId, offset, metadata, noError)
      getTopicResponse conn (topicName, partitionIds) = do
        parts <- mapM (getPartitionResponse conn topicName) partitionIds
        return (topicName, parts)
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (getTopicResponse conn) topics)
  return . OffsetFetchResponseV0 $ topicResponses
