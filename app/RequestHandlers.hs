module RequestHandlers
  ( handleRequest
  ) where

import Data.Attoparsec.ByteString (endOfInput, parseOnly)
import Data.ByteString (ByteString)
import qualified Data.Pool as Pool
import Data.Serialize.Put (putByteString, putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request (kafkaRequest)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequestBox(..), respond)
import Kafkaesque.Response (writeResponse)

handleRequest ::
     Pool.Pool PG.Connection -> ByteString -> Either String (IO ByteString)
handleRequest pool request = do
  let parseResult = parseOnly (kafkaRequest <* endOfInput)
      putCorrelationId = putWord32be . fromIntegral
      putResponseBody = putByteString . writeResponse
      putFramedResponse correlationId resp =
        putCorrelationId correlationId *> putResponseBody resp
      generateResponse ((_, _, correlationId, _), KR req) =
        (runPut . putFramedResponse correlationId) <$> respond pool req
  generateResponse <$> parseResult request
