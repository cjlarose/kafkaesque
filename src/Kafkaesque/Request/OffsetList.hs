module Kafkaesque.Request.OffsetList
  ( offsetsRequest
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int32, Int64)
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Request.Parsers
       (kafkaArray, kafkaString, signedInt32be, signedInt64be)
import Kafkaesque.Request.Queries
       (getEarliestOffset, getNextOffset, getTopicPartition)
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        OffsetListResponsePartition, OffsetListResponseTopic,
        OffsetListResponseVO(..))

data OffsetListRequestTimestamp
  = LatestOffset
  | EarliestOffset
  | OffsetListTimestamp Int64

type OffsetListRequestPartition = (Int32, OffsetListRequestTimestamp, Int32)

type OffsetListRequestTopic = (String, [OffsetListRequestPartition])

data OffsetListRequest =
  OffsetListRequest ApiVersion
                    Int32
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

offsetsRequest :: ApiVersion -> Parser OffsetListRequest
offsetsRequest (ApiVersion v)
  | v == 0 =
    OffsetListRequest (ApiVersion v) <$> signedInt32be <*>
    (fromMaybe [] <$> kafkaArray offsetsRequestTopic)

fetchTopicPartitionOffsets ::
     PG.Connection
  -> Int32
  -> Int32
  -> OffsetListRequestTimestamp
  -> Int32
  -> IO [Int64]
fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets = do
  earliest <- getEarliestOffset conn topicId partitionId
  latest <- getNextOffset conn topicId partitionId
  -- TODO handle actual timestamp offsets
  let offsets =
        case timestamp of
          LatestOffset -> [Just latest, earliest]
          EarliestOffset -> [earliest]
  return . take (fromIntegral maxOffsets) . catMaybes $ offsets

fetchPartitionOffsets ::
     PG.Connection
  -> String
  -> OffsetListRequestPartition
  -> IO OffsetListResponsePartition
fetchPartitionOffsets conn topicName (partitionId, timestamp, maxOffsets) = do
  res <- getTopicPartition conn topicName partitionId
  case res of
    Nothing -> return (partitionId, UnknownTopicOrPartition, Nothing)
    Just (topicId, _) -> do
      offsets <-
        fetchTopicPartitionOffsets conn topicId partitionId timestamp maxOffsets
      return (partitionId, NoError, Just offsets)

fetchTopicOffsets ::
     PG.Connection -> OffsetListRequestTopic -> IO OffsetListResponseTopic
fetchTopicOffsets conn (topicName, partitions) = do
  partitionResponses <- mapM (fetchPartitionOffsets conn topicName) partitions
  return (topicName, partitionResponses)

respondToRequest ::
     Pool.Pool PG.Connection -> OffsetListRequest -> IO KafkaResponseBox
respondToRequest pool (OffsetListRequest (ApiVersion 0) _ topics) = do
  topicResponses <-
    Pool.withResource pool (\conn -> mapM (fetchTopicOffsets conn) topics)
  return . KResp $ OffsetListResponseVO topicResponses

instance KafkaRequest OffsetListRequest where
  respond = respondToRequest
