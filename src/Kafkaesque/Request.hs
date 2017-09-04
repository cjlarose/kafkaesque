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

import Kafkaesque.ApiVersion (ApiVersion(..))
import Kafkaesque.Message (Message(..), MessageSet)
import Kafkaesque.Parsers (RequestMetadata, requestMessageHeader)
import Kafkaesque.Request.ApiVersions (apiVersionsRequestV0)
import Kafkaesque.Request.Fetch (fetchRequestV0)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaRequestBox(..))
import Kafkaesque.Request.OffsetList (offsetsRequestV0)
import Kafkaesque.Request.Produce
       (produceRequestV0, produceRequestV1)
import Kafkaesque.Request.TopicMetadata (metadataRequestV0)

kafkaRequest :: Parser (RequestMetadata, KafkaRequestBox)
kafkaRequest = do
  metadata@(apiKey, apiVersion@(ApiVersion v), correlationId, clientId) <-
    requestMessageHeader
  let requestParser =
        case (apiKey, v) of
          (0, 0) -> KR <$> produceRequestV0
          (0, 1) -> KR <$> produceRequestV1
          (1, 0) -> KR <$> fetchRequestV0
          (2, 0) -> KR <$> offsetsRequestV0
          (3, 0) -> KR <$> metadataRequestV0
          (18, 0) -> KR <$> apiVersionsRequestV0
          _ -> fail "Unknown request type"
  (\r -> (metadata, r)) <$> requestParser
