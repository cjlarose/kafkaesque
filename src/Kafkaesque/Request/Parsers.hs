module Kafkaesque.Request.Parsers
  ( signedInt16be
  , signedInt32be
  , signedInt64be
  , kafkaNullableString
  , kafkaString
  , kafkaBytes
  , kafkaNullabeBytes
  , kafkaArray
  , requestMessageHeader
  , RequestMetadata
  ) where

import Data.Attoparsec.Binary
       (anyWord16be, anyWord32be, anyWord64be)
import Data.Attoparsec.ByteString (Parser, count, take)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (toString)
import Data.Int (Int16, Int32, Int64)

import Kafkaesque.ApiVersion (ApiVersion(..))

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

type RequestMetadata = (Int16, ApiVersion, Int32, Maybe String)

requestMessageHeader :: Parser RequestMetadata
requestMessageHeader =
  (\apiKey apiVersion correlationId clientId ->
     (apiKey, ApiVersion . fromIntegral $ apiVersion, correlationId, clientId)) <$>
  signedInt16be <*>
  signedInt16be <*>
  signedInt32be <*>
  kafkaNullableString
