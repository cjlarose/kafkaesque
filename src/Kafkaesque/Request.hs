module Kafkaesque.Request (KafkaRequest(..), kafkaRequest, ApiVersion(..)) where

import Data.Word (Word8, Word32)
import Data.Int (Int16, Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (toString)
import Data.Attoparsec.ByteString (Parser, parseOnly, take, count, many', anyWord8)
import Data.Attoparsec.Binary (anyWord16be, anyWord32be, anyWord64be)

newtype ApiVersion = ApiVersion Int
data Message = Message Word32 Word8 Word8 ByteString ByteString
type MessageSet = [(Int64, Message)]
type PartitionData = (Int32, MessageSet)
type TopicData = (String, [PartitionData])
newtype TimeoutMs = TimeoutMs Int32
data KafkaRequest = ProduceRequest ApiVersion Int16 TimeoutMs [TopicData]
                  | TopicMetadataRequest ApiVersion (Maybe [String])
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
  if len < 0 then
    return Nothing
  else
    Just . toString <$> Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaString :: Parser String
kafkaString = do
  len <- signedInt16be
  if len < 0 then
    fail "Expected non-null string"
  else
    toString <$> Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaBytes :: Parser ByteString
kafkaBytes = do
  len <- signedInt32be
  if len < 0 then
    fail "Expected non-null bytes"
  else
    Data.Attoparsec.ByteString.take (fromIntegral len)

kafkaArray :: Parser a -> Parser (Maybe [a])
kafkaArray p = do
  len <- signedInt32be
  if len < 0 then
    return Nothing
  else
    Just <$> count (fromIntegral len) p

metadataRequest :: ApiVersion -> Parser KafkaRequest
metadataRequest (ApiVersion v) | v <= 3 = TopicMetadataRequest (ApiVersion v) <$> kafkaArray kafkaString

produceRequest :: ApiVersion -> Parser KafkaRequest
produceRequest (ApiVersion v) | v <= 2 =
  let
    message :: Parser Message
    message = Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*> kafkaBytes <*> kafkaBytes

    mesasgeSetElement :: Parser (Int64, Message)
    mesasgeSetElement = (\x y -> (x, y)) <$> (signedInt64be <* signedInt32be) <*> message

    records :: Parser MessageSet
    records = do
      messageSetSize <- signedInt32be
      bs <- Data.Attoparsec.ByteString.take . fromIntegral $ messageSetSize
      case parseOnly (many' mesasgeSetElement) bs of
        Left err -> fail err
        Right xs -> return xs

    partitionData :: Parser PartitionData
    partitionData = (\partitionId msgs -> (partitionId, msgs)) <$> signedInt32be <*> records

    topicData :: Parser TopicData
    topicData = (\name parts -> (name, parts)) <$> kafkaString <*> (fromMaybe [] <$> kafkaArray partitionData)
  in
    ProduceRequest (ApiVersion v) <$>
      signedInt16be <*>
      (TimeoutMs <$> signedInt32be) <*>
      (fromMaybe [] <$> kafkaArray topicData)

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId -> (apiKey, ApiVersion . fromIntegral $ apiVersion, correlationId, clientId))
    <$> signedInt16be <*> signedInt16be <*> signedInt32be <*> kafkaNullableString

kafkaRequest :: Parser (RequestMetadata, KafkaRequest)
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  let
    requestParser 0 = Just produceRequest
    requestParser 3 = Just metadataRequest
    requestParser _ = Nothing
  case requestParser apiKey of
    Just parser -> (\r -> (metadata, r)) <$> parser apiVersion
    _           -> fail "Unknown request type"
