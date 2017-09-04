module Kafkaesque.Request.OffsetFetch
  ( offsetFetchRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError
       (KafkaError(NoError, UnexpectedError, UnknownTopicOrPartition))
import Kafkaesque.Parsers (kafkaArray, kafkaString, signedInt32be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.ConsumerOffsets (getOffsetForConsumer)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response (OffsetFetchResponseV0(..))

data OffsetFetchRequestV0 =
  OffsetFetchRequestV0 String
                       [(String, [Int32])]

offsetFetchRequestV0 :: Parser OffsetFetchRequestV0
offsetFetchRequestV0 =
  let topic =
        (\a b -> (a, b)) <$> kafkaString <*>
        (fromMaybe [] <$> kafkaArray signedInt32be)
  in OffsetFetchRequestV0 <$> kafkaString <*>
     (fromMaybe [] <$> kafkaArray topic)

respondToRequest ::
     Pool.Pool PG.Connection -> OffsetFetchRequestV0 -> IO OffsetFetchResponseV0
respondToRequest pool (OffsetFetchRequestV0 cgId topics) = do
  let getPartitionResponse conn topicName partitionId = do
        topicPartitionRes <- getTopicPartition conn topicName partitionId
        case topicPartitionRes of
          Nothing -> return (partitionId, -1, "", UnknownTopicOrPartition)
          Just _ -> do
            offsetRes <- getOffsetForConsumer conn cgId topicName partitionId
            case offsetRes of
              Nothing -> return (partitionId, -1, "", NoError)
              Just offsetRes ->
                case offsetRes of
                  Left _ -> return (partitionId, -1, "", UnexpectedError)
                  Right (offset, metadata) ->
                    return (partitionId, offset, metadata, NoError)
      getTopicResponse conn (topicName, partitionIds) = do
        parts <- mapM (getPartitionResponse conn topicName) partitionIds
        return (topicName, parts)
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (getTopicResponse conn) topics)
  return . OffsetFetchResponseV0 $ topicResponses

instance KafkaRequest OffsetFetchRequestV0 where
  respond pool req = KResp <$> respondToRequest pool req
