module Kafkaesque.Request
  ( KafkaRequest(..)
  , kafkaRequest
  , ApiVersion(..)
  , OffsetListRequestTopic
  , OffsetListRequestPartition
  , OffsetListRequestTimestamp(OffsetListTimestamp, LatestOffset, EarliestOffset)
  ) where

import Data.Attoparsec.Binary
       (anyWord16be, anyWord32be, anyWord64be)
import Data.Attoparsec.ByteString
       (Parser, anyWord8, count, many', parseOnly, take)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (toString)
import Data.Int (Int16, Int32, Int64)
import Data.Maybe (fromMaybe)
import Kafkaesque.Message (Message(..), MessageSet)

newtype ApiVersion =
  ApiVersion Int

type PartitionData = (Int32, MessageSet)

type TopicData = (String, [PartitionData])

newtype TimeoutMs =
  TimeoutMs Int32

type FetchRequestPartition = (Int32, Int64, Int32)

type FetchRequestTopic = (String, [FetchRequestPartition])

data OffsetListRequestTimestamp
  = LatestOffset
  | EarliestOffset
  | OffsetListTimestamp Int64

type OffsetListRequestPartition = (Int32, OffsetListRequestTimestamp, Int32)

type OffsetListRequestTopic = (String, [OffsetListRequestPartition])

data KafkaRequest
  = ProduceRequest ApiVersion
                   Int16
                   TimeoutMs
                   [TopicData]
  | FetchRequest ApiVersion
                 Int32
                 Int32
                 Int32
                 [FetchRequestTopic]
  | OffsetListRequest ApiVersion
                      Int32
                      [OffsetListRequestTopic]
  | TopicMetadataRequest ApiVersion
                         (Maybe [String])
  | ApiVersionsRequest ApiVersion
                       (Maybe [Int16])

type RequestMetadata = (Int16, ApiVersion, Int32, Maybe String)

signedInt16be :: Parser Int16
signedInt16be = fromIntegral <$> anyWord16be

signedInt32be :: Parser Int32
signedInt32be = fromIntegral <$> anyWord32be

signedInt64be :: Parser Int64
signedInt64be = fromIntegral <$> anyWord64be

kafkaNullableString :: Parser (Maybe String)
kafkaNullableString = do
  len <- signedInt16be
  if len < 0
    then return Nothing
    else Just . toString <$> Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaString :: Parser String
kafkaString = do
  len <- signedInt16be
  if len < 0
    then fail "Expected non-null string"
    else toString <$> Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaBytes :: Parser ByteString
kafkaBytes = do
  len <- signedInt32be
  if len < 0
    then fail "Expected non-null bytes"
    else Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaNullabeBytes :: Parser (Maybe ByteString)
kafkaNullabeBytes = do
  len <- signedInt32be
  if len < 0
    then return Nothing
    else Just <$> Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaArray :: Parser a -> Parser (Maybe [a])
kafkaArray p = do
  len <- signedInt32be
  if len < 0
    then return Nothing
    else Just <$> count (fromIntegral len) p

metadataRequest :: ApiVersion -> Parser KafkaRequest
metadataRequest (ApiVersion v)
  | v <= 3 = TopicMetadataRequest (ApiVersion v) <$> kafkaArray kafkaString

produceRequest :: ApiVersion -> Parser KafkaRequest
produceRequest (ApiVersion v)
  | v <= 2 =
    let message :: Parser Message
        message =
          Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*>
          kafkaNullabeBytes <*>
          kafkaNullabeBytes
        mesasgeSetElement :: Parser (Int64, Message)
        mesasgeSetElement =
          (\x y -> (x, y)) <$> (signedInt64be <* signedInt32be) <*> message
        records :: Parser MessageSet
        records = do
          messageSetSize <- signedInt32be
          bs <- Data.Attoparsec.ByteString.take . fromIntegral $ messageSetSize
          case parseOnly (many' mesasgeSetElement) bs of
            Left err -> fail err
            Right xs -> return xs
        partitionData :: Parser PartitionData
        partitionData =
          (\partitionId msgs -> (partitionId, msgs)) <$> signedInt32be <*>
          records
        topicData :: Parser TopicData
        topicData =
          (\name parts -> (name, parts)) <$> kafkaString <*>
          (fromMaybe [] <$> kafkaArray partitionData)
    in ProduceRequest (ApiVersion v) <$> signedInt16be <*>
       (TimeoutMs <$> signedInt32be) <*>
       (fromMaybe [] <$> kafkaArray topicData)

fetchRequestPartition :: Parser FetchRequestPartition
fetchRequestPartition =
  (\a b c -> (a, b, c)) <$> signedInt32be <*> signedInt64be <*> signedInt32be

fetchRequestTopic :: Parser FetchRequestTopic
fetchRequestTopic =
  (\topic partitions -> (topic, partitions)) <$> kafkaString <*>
  (fromMaybe [] <$> kafkaArray fetchRequestPartition)

fetchRequest :: ApiVersion -> Parser KafkaRequest
fetchRequest (ApiVersion v)
  | v <= 2 =
    FetchRequest (ApiVersion v) <$> signedInt32be <*> signedInt32be <*>
    signedInt32be <*>
    (fromMaybe [] <$> kafkaArray fetchRequestTopic)

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

offsetsRequest :: ApiVersion -> Parser KafkaRequest
offsetsRequest (ApiVersion v)
  | v == 0 =
    OffsetListRequest (ApiVersion v) <$> signedInt32be <*>
    (fromMaybe [] <$> kafkaArray offsetsRequestTopic)

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId ->
     (apiKey, ApiVersion . fromIntegral $ apiVersion, correlationId, clientId)) <$>
  signedInt16be <*>
  signedInt16be <*>
  signedInt32be <*>
  kafkaNullableString

apiVersionsRequest :: ApiVersion -> Parser KafkaRequest
apiVersionsRequest (ApiVersion v)
  | v <= 1 = ApiVersionsRequest (ApiVersion v) <$> kafkaArray signedInt16be

kafkaRequest :: Parser (RequestMetadata, KafkaRequest)
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  let requestParser =
        case apiKey of
          0 -> produceRequest
          1 -> fetchRequest
          2 -> offsetsRequest
          3 -> metadataRequest
          18 -> apiVersionsRequest
          _ -> const $ fail "Unknown request type"
  (\r -> (metadata, r)) <$> requestParser apiVersion
