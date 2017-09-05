module Kafkaesque.Request.OffsetList
  ( offsetsRequestV0
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
import Kafkaesque.Queries
       (getEarliestOffset, getNextOffset, getTopicPartition)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response
       (OffsetListResponsePartition, OffsetListResponseTopic,
        OffsetListResponseV0(..))

data OffsetListRequestTimestamp
  = LatestOffset
  | EarliestOffset
  | OffsetListTimestamp Int64

type OffsetListRequestPartition = (Int32, OffsetListRequestTimestamp, Int32)

type OffsetListRequestTopic = (String, [OffsetListRequestPartition])

data OffsetListRequestV0 =
  OffsetListRequestV0 Int32
                      [OffsetListRequestTopic]

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

offsetsRequestV0 :: Parser OffsetListRequestV0
offsetsRequestV0 =
  OffsetListRequestV0 <$> signedInt32be <*>
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
      offsets <-
        fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets
      return $
        case offsets of
          Nothing -> (partitionId, unsupportedForMessageFormat, Nothing)
          Just offsets -> (partitionId, noError, Just offsets)

fetchTopicOffsets ::
     PG.Connection -> OffsetListRequestTopic -> IO OffsetListResponseTopic
fetchTopicOffsets conn (topicName, partitions) = do
  partitionResponses <- mapM (fetchPartitionOffsets conn topicName) partitions
  return (topicName, partitionResponses)

respondToRequest ::
     Pool.Pool PG.Connection -> OffsetListRequestV0 -> IO KafkaResponseBox
respondToRequest pool (OffsetListRequestV0 _ topics) = do
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (fetchTopicOffsets conn) topics)
  return . KResp $ OffsetListResponseV0 topicResponses

instance KafkaRequest OffsetListRequestV0 where
  respond = respondToRequest
