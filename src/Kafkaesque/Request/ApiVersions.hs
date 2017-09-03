module Kafkaesque.Request.ApiVersions
  ( apiVersionsRequest
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int16)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest (KafkaRequest, respond)
import Kafkaesque.Request.Parsers (kafkaArray, signedInt16be)
import Kafkaesque.Response
       (KafkaError(NoError), KafkaResponse(ApiVersionsResponseV0))

data ApiVersionsRequest =
  ApiVersionsRequest ApiVersion
                     (Maybe [Int16])

apiVersionsRequest :: ApiVersion -> Parser ApiVersionsRequest
apiVersionsRequest (ApiVersion v)
  | v <= 1 = ApiVersionsRequest (ApiVersion v) <$> kafkaArray signedInt16be

respondToRequest ::
     Pool.Pool PG.Connection -> ApiVersionsRequest -> IO KafkaResponse
respondToRequest pool (ApiVersionsRequest (ApiVersion 0) apiKeys) =
  return $
  ApiVersionsResponseV0 NoError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]

instance KafkaRequest ApiVersionsRequest where
  respond = respondToRequest
