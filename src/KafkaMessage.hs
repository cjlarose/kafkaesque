module KafkaMessage(kafkaString) where

import Data.Int (Int16, Int32)
import Data.ByteString.UTF8 (toString)
import Data.Attoparsec.ByteString (Parser, take)
import Data.Attoparsec.Binary (anyWord16be, anyWord32be)

signedInt16be :: Parser Int16
signedInt16be = fromIntegral <$> anyWord16be

signedInt32be :: Parser Int32
signedInt32be = fromIntegral <$> anyWord32be

kafkaString :: Parser (Maybe String)
kafkaString = do
  len <- signedInt16be
  if (len < 0) then do
    str <- Data.Attoparsec.ByteString.take . fromIntegral $ len
    return . Just . toString $ str
  else
    return Nothing
