module Kafkaesque.Request
  ( kafkaRequest
  ) where

import Data.Attoparsec.ByteString (Parser)

import Kafkaesque.ApiVersion (ApiVersion(..))
import Kafkaesque.Parsers (RequestMetadata, requestMessageHeader)
import Kafkaesque.Request.ApiVersions (apiVersionsRequestV0)
import Kafkaesque.Request.Fetch (fetchRequestV0)
import Kafkaesque.Request.KafkaRequest (KafkaRequestBox(..))
import Kafkaesque.Request.OffsetCommit (offsetCommitRequestV0)
import Kafkaesque.Request.OffsetFetch (offsetFetchRequestV0)
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
          (8, 0) -> KR <$> offsetCommitRequestV0
          (9, 0) -> KR <$> offsetFetchRequestV0
          (18, 0) -> KR <$> apiVersionsRequestV0
          _ -> fail "Unknown request type"
  (\r -> (metadata, r)) <$> requestParser
