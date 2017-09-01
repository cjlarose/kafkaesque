module RequestHandlers
  ( handleRequest
  ) where

import Data.Attoparsec.ByteString (endOfInput, parseOnly)
import Data.ByteString (ByteString)
import qualified Data.Pool as Pool
import Data.Serialize.Put (putByteString, putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request (KafkaRequest(..), kafkaRequest)
import Kafkaesque.Response (KafkaResponse, writeResponse)
import qualified RequestHandlers.ApiVersions
import qualified RequestHandlers.Fetch
import qualified RequestHandlers.Metadata
import qualified RequestHandlers.Produce

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool req@ProduceRequest {} =
  RequestHandlers.Produce.respondToRequest pool req
respondToRequest pool req@FetchRequest {} =
  RequestHandlers.Fetch.respondToRequest pool req
respondToRequest pool req@TopicMetadataRequest {} =
  RequestHandlers.Metadata.respondToRequest pool req
respondToRequest pool req@ApiVersionsRequest {} =
  RequestHandlers.ApiVersions.respondToRequest pool req

handleRequest ::
     Pool.Pool PG.Connection -> ByteString -> Either String (IO ByteString)
handleRequest pool request = do
  let parseResult = parseOnly (kafkaRequest <* endOfInput)
      putCorrelationId = putWord32be . fromIntegral
      putResponseBody = putByteString . writeResponse
      putFramedResponse correlationId resp =
        putCorrelationId correlationId *> putResponseBody resp
      generateResponse ((_, _, correlationId, _), req) =
        (runPut . putFramedResponse correlationId) <$> respondToRequest pool req
  generateResponse <$> parseResult request
