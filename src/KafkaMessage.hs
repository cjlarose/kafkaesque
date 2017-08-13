module KafkaMessage (KafkaRequest(..), Broker(..), KafkaError(..), PartitionMetadata(..), TopicMetadata(..), KafkaResponse(..), writeResponse, kafkaRequest) where

import Data.Int (Int16, Int32)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (toString, fromString)
import Data.Attoparsec.ByteString (Parser, take, count)
import Data.Attoparsec.Binary (anyWord16be, anyWord32be)
import Data.Serialize.Put (Put, runPut, putWord16be, putWord32be, putByteString)

signedInt16be :: Parser Int16
signedInt16be = fromIntegral <$> anyWord16be

signedInt32be :: Parser Int32
signedInt32be = fromIntegral <$> anyWord32be

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

kafkaArray :: Parser a -> Parser (Maybe [a])
kafkaArray p = do
  len <- signedInt32be
  if len < 0 then
    return Nothing
  else
    Just <$> count (fromIntegral len) p

data KafkaRequest = TopicMetadataRequest (Maybe [String])

data Broker = Broker Int32 String Int32
data KafkaError = NoError | UnknownTopicOrPartition
data PartitionMetadata = PartitionMetadata KafkaError Int32 Int32 [Int32] [Int32]
data TopicMetadata = TopicMetadata KafkaError String [PartitionMetadata]
data KafkaResponse = TopicMetadataResponseV0 [Broker] [TopicMetadata]

type RequestMetadata = (Int16, Int16, Int32, Maybe String)

metadataRequest :: Parser KafkaRequest
metadataRequest = TopicMetadataRequest <$> kafkaArray kafkaString

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId -> (apiKey, apiVersion, correlationId, clientId))
    <$> signedInt16be <*> signedInt16be <*> signedInt32be <*> kafkaNullableString

kafkaRequest :: Parser (Either String (RequestMetadata, KafkaRequest))
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  case apiKey of
    3 -> do
      request <- metadataRequest
      return . Right $ (metadata, request)
    _ -> return . Left $ "Unknown request type"

putKafkaString :: String -> Put
putKafkaString s = (putWord16be . fromIntegral . length $ s) *> putByteString (fromString s)

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs = (putWord32be . fromIntegral . length $ xs) *> mapM_ putter xs

kafkaErrorCode :: KafkaError -> Int
kafkaErrorCode NoError = 0
kafkaErrorCode UnknownTopicOrPartition = 3

writeResponse :: KafkaResponse -> ByteString
writeResponse (TopicMetadataResponseV0 brokers topicMetadata) =
  let
    putInt32be = putWord32be . fromIntegral
    putKakfaError = putWord16be . fromIntegral . kafkaErrorCode

    putBroker (Broker nodeId host port) = putInt32be nodeId *> putKafkaString host *> putInt32be port

    putPartitionMetadata (PartitionMetadata err partitionId leader replicas isr) =
      putKakfaError err *>
      putInt32be partitionId *>
      putInt32be leader *>
      putKafkaArray putInt32be replicas *>
      putKafkaArray putInt32be isr

    putTopicMetadata (TopicMetadata err name partitionMetadata) =
      putKakfaError err *>
      putKafkaString name *>
      putKafkaArray putPartitionMetadata partitionMetadata
  in
    runPut $ putKafkaArray putBroker brokers *> putKafkaArray putTopicMetadata topicMetadata
