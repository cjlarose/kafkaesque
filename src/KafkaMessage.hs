module KafkaMessage (KafkaRequest(..), kafkaRequest) where

import Data.Maybe (catMaybes)
import Data.Int (Int16, Int32)
import Data.ByteString.UTF8 (toString)
import Data.Attoparsec.ByteString (Parser, take, count)
import Data.Attoparsec.Binary (anyWord16be, anyWord32be)

signedInt16be :: Parser Int16
signedInt16be = fromIntegral <$> anyWord16be

signedInt32be :: Parser Int32
signedInt32be = fromIntegral <$> anyWord32be

kafkaString :: Parser (Maybe String)
kafkaString = do
  len <- signedInt16be
  if len < 0 then
    return Nothing
  else do
    str <- Data.Attoparsec.ByteString.take . fromIntegral $ len
    return . Just . toString $ str

kafkaArray :: Parser a -> Parser (Maybe [a])
kafkaArray p = do
  len <- signedInt32be
  if len < 0 then
    return Nothing
  else do
    xs <- count (fromIntegral len) p
    return . Just $ xs

data KafkaRequest = TopicMetadataRequest (Maybe [String])

metadataRequest :: Parser KafkaRequest
metadataRequest = (TopicMetadataRequest . (<$>) catMaybes) <$> kafkaArray kafkaString

requestMessageHeader :: Parser (Int16, Int16, Int32, Maybe String)
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId -> (apiKey, apiVersion, correlationId, clientId))
    <$> signedInt16be <*> signedInt16be <*> signedInt32be <*> kafkaString

kafkaRequest :: Parser (Either String KafkaRequest)
kafkaRequest = do
  (apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  case apiKey of
    3 -> do
      request <- metadataRequest
      return . Right $ request
    _ -> return . Left $ "Unknown request type"
