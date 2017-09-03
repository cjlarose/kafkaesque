module Kafkaesque.Request
  ( kafkaRequest
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
import Kafkaesque.Request.ApiVersions (apiVersionsRequest)
import Kafkaesque.Request.Fetch (fetchRequest)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaRequestBox(..))
import Kafkaesque.Request.OffsetList (offsetsRequest)
import Kafkaesque.Request.Parsers
       (RequestMetadata, requestMessageHeader)
import Kafkaesque.Request.Produce (produceRequest)
import Kafkaesque.Request.TopicMetadata (metadataRequest)

kafkaRequest :: Parser (RequestMetadata, KafkaRequestBox)
kafkaRequest = do
  metadata@(apiKey, apiVersion, correlationId, clientId) <- requestMessageHeader
  let requestParser =
        case apiKey of
          0 -> KR <$> produceRequest apiVersion
          1 -> KR <$> fetchRequest apiVersion
          2 -> KR <$> offsetsRequest apiVersion
          3 -> KR <$> metadataRequest apiVersion
          18 -> KR <$> apiVersionsRequest apiVersion
          _ -> fail "Unknown request type"
  (\r -> (metadata, r)) <$> requestParser
