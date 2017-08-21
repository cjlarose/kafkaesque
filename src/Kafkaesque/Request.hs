module Kafkaesque.Request
  ( KafkaRequest(..)
  , kafkaRequest
  , ApiVersion(..)
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
  | TopicMetadataRequest ApiVersion
                         (Maybe [String])

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

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId ->
     (apiKey, ApiVersion . fromIntegral $ apiVersion, correlationId, clientId)) <$>
  signedInt16be <*>
  signedInt16be <*>
  signedInt32be <*>
  kafkaNullableString

kafkaRequest :: Parser (RequestMetadata, KafkaRequest)
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  let requestParser 0 = Just produceRequest
      requestParser 1 = Just fetchRequest
      requestParser 3 = Just metadataRequest
      requestParser _ = Nothing
  case requestParser apiKey of
    Just parser -> (\r -> (metadata, r)) <$> parser apiVersion
    _ -> fail "Unknown request type"
