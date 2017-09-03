module RequestHandlers.ApiVersions
  ( respondToRequest
  ) where

import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(ApiVersionsRequest))
import Kafkaesque.Response
       (KafkaError(NoError), KafkaResponse(ApiVersionsResponseV0))

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool (ApiVersionsRequest (ApiVersion 0) apiKeys) =
  return $
  ApiVersionsResponseV0 NoError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]
