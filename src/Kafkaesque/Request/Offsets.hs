{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.Offsets
  ( offsetsRequestV0
  , respondToRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int32, Int64)
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError
       (noError, unknownTopicOrPartition, unsupportedForMessageFormat)
import Kafkaesque.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Protocol.ApiKey (Offsets)
import Kafkaesque.Protocol.ApiVersion (V0)
import Kafkaesque.Queries
       (getEarliestOffset, getNextOffset, getTopicPartition)
import Kafkaesque.Request.KafkaRequest
       (OffsetListRequestPartition,
        OffsetListRequestTimestamp(EarliestOffset, LatestOffset,
                                   OffsetListTimestamp),
        OffsetListRequestTopic, OffsetListResponsePartition,
        OffsetListResponseTopic, Request(OffsetsRequestV0),
        Response(OffsetsResponseV0))

makeTimestamp :: Int64 -> Maybe OffsetListRequestTimestamp
makeTimestamp (-1) = Just LatestOffset
makeTimestamp (-2) = Just EarliestOffset
makeTimestamp t
  | t > 0 = Just $ OffsetListTimestamp t
  | otherwise = Nothing

offsetsRequestTimestamp :: Parser OffsetListRequestTimestamp
offsetsRequestTimestamp =
  makeTimestamp <$> signedInt64be >>=
  maybe (fail "Unable to parse timestamp") return

offsetsRequestPartition :: Parser OffsetListRequestPartition
offsetsRequestPartition =
  (\partitionId ts maxNumOffsets -> (partitionId, ts, maxNumOffsets)) <$>
  signedInt32be <*>
  offsetsRequestTimestamp <*>
  signedInt32be

offsetsRequestTopic :: Parser OffsetListRequestTopic
offsetsRequestTopic =
  (\t xs -> (t, xs)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray offsetsRequestPartition)

offsetsRequestV0 :: Parser (Request Offsets V0)
offsetsRequestV0 =
  OffsetsRequestV0 <$> signedInt32be <*>
  (fromMaybe [] <$> kafkaArray offsetsRequestTopic)

fetchTopicPartitionOffsets ::
     PG.Connection
  -> Int32
  -> Int32
  -> OffsetListRequestTimestamp
  -> Int32
  -> IO (Maybe [Int64])
fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets = do
  earliest <- getEarliestOffset conn topicId partitionId
  latest <- getNextOffset conn topicId partitionId
  -- TODO handle actual timestamp offsets
  let sendOffsets = Just . take (fromIntegral maxOffsets) . catMaybes
  return $
    case timestamp of
      LatestOffset -> sendOffsets [Just latest, earliest]
      EarliestOffset -> sendOffsets [earliest]
      OffsetListTimestamp _ -> Nothing

fetchPartitionOffsets ::
     PG.Connection
  -> String
  -> OffsetListRequestPartition
  -> IO OffsetListResponsePartition
fetchPartitionOffsets conn topicName (partitionId, timestamp, maxOffsets) = do
  res <- getTopicPartition conn topicName partitionId
  case res of
    Nothing -> return (partitionId, unknownTopicOrPartition, Nothing)
    Just (topicId, _) -> do
      offsetRes <-
        fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets
      return $
        case offsetRes of
          Nothing -> (partitionId, unsupportedForMessageFormat, Nothing)
          Just offsets -> (partitionId, noError, Just offsets)

fetchTopicOffsets ::
     PG.Connection -> OffsetListRequestTopic -> IO OffsetListResponseTopic
fetchTopicOffsets conn (topicName, partitions) = do
  partitionResponses <- mapM (fetchPartitionOffsets conn topicName) partitions
  return (topicName, partitionResponses)

respondToRequestV0 ::
     Pool.Pool PG.Connection -> Request Offsets V0 -> IO (Response Offsets V0)
respondToRequestV0 pool (OffsetsRequestV0 _ topics) = do
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (fetchTopicOffsets conn) topics)
  return $ OffsetsResponseV0 topicResponses
