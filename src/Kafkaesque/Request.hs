module Kafkaesque.Request (KafkaRequest(..), kafkaRequest, ApiVersion(..)) where

import Data.Int (Int16, Int32)
import Data.ByteString.UTF8 (toString)
import Data.Attoparsec.ByteString (Parser, take, count)
import Data.Attoparsec.Binary (anyWord16be, anyWord32be)

newtype ApiVersion = ApiVersion Int
data KafkaRequest = TopicMetadataRequest ApiVersion (Maybe [String])
type RequestMetadata = (Int16, ApiVersion, Int32, Maybe String)

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

metadataRequest :: ApiVersion -> Parser KafkaRequest
metadataRequest (ApiVersion v) | v <= 3 = TopicMetadataRequest (ApiVersion v) <$> kafkaArray kafkaString

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId -> (apiKey, ApiVersion . fromIntegral $ apiVersion, correlationId, clientId))
    <$> signedInt16be <*> signedInt16be <*> signedInt32be <*> kafkaNullableString

kafkaRequest :: Parser (RequestMetadata, KafkaRequest)
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  case apiKey of
    3 -> do
      request <- metadataRequest apiVersion
      return (metadata, request)
    _ -> fail "Unknown request type"
