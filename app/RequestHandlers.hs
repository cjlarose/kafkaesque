module RequestHandlers
  ( handleRequest
  ) where

import Data.Attoparsec.ByteString (Parser, endOfInput, parseOnly)
import Data.ByteString (ByteString)
import Data.Int (Int32)
import qualified Data.Pool as Pool
import Data.Serialize.Put (putByteString, putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Parsers (kafkaRequest)
import qualified Kafkaesque.Request.ApiVersions as ApiVersions
import qualified Kafkaesque.Request.Fetch as Fetch
import Kafkaesque.Request.KafkaRequest (Request(..), Response(..))
import qualified Kafkaesque.Request.Metadata as Metadata
import qualified Kafkaesque.Request.OffsetCommit as OffsetCommit
import qualified Kafkaesque.Request.OffsetFetch as OffsetFetch
import qualified Kafkaesque.Request.Offsets as Offsets
import qualified Kafkaesque.Request.Produce as Produce
import Kafkaesque.Response (writeResponse)

makeHandler ::
     Parser (Request k v)
  -> (Pool.Pool PG.Connection -> Request k v -> IO (Response k v))
  -> Int32
  -> Pool.Pool PG.Connection
  -> ByteString
  -> Either String (IO ByteString)
makeHandler parser respond correlationId pool request = do
  let parseResult = parseOnly (parser <* endOfInput)
      putCorrelationId = putWord32be . fromIntegral
      putResponseBody = putByteString . writeResponse
      putFramedResponse resp =
        putCorrelationId correlationId *> putResponseBody resp
      generateResponse req = (runPut . putFramedResponse) <$> respond pool req
  generateResponse <$> parseResult request

handleRequest ::
     Pool.Pool PG.Connection -> ByteString -> Either String (IO ByteString)
handleRequest pool request = do
  ((apiKey, version, correlationId, _), body) <- parseOnly kafkaRequest request
  let handler =
        case (apiKey, version) of
          (0, 0) ->
            makeHandler Produce.produceRequestV0 Produce.respondToRequestV0
          (0, 1) ->
            makeHandler Produce.produceRequestV1 Produce.respondToRequestV1
          (1, 0) -> makeHandler Fetch.fetchRequestV0 Fetch.respondToRequestV0
          (2, 0) ->
            makeHandler Offsets.offsetsRequestV0 Offsets.respondToRequestV0
          (3, 0) ->
            makeHandler Metadata.metadataRequestV0 Metadata.respondToRequestV0
          (8, 0) ->
            makeHandler
              OffsetCommit.offsetCommitRequestV0
              OffsetCommit.respondToRequestV0
          (9, 0) ->
            makeHandler
              OffsetFetch.offsetFetchRequestV0
              OffsetFetch.respondToRequestV0
          (18, 0) ->
            makeHandler
              ApiVersions.apiVersionsRequestV0
              ApiVersions.respondToRequestV0
          _ -> \_ _ _ -> Left "Unsupported (ApiKey, ApiVersion) combination"
  handler correlationId pool body
