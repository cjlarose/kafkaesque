module Kafkaesque.Request.ApiVersions
  ( apiVersionsRequest
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int16)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion(..))
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Request.Parsers (kafkaArray, signedInt16be)
import Kafkaesque.Response
       (ApiVersionsResponseV0(..), KafkaError(NoError))

data ApiVersionsRequest =
  ApiVersionsRequest ApiVersion
                     (Maybe [Int16])

apiVersionsRequest :: ApiVersion -> Parser ApiVersionsRequest
apiVersionsRequest (ApiVersion v)
  | v <= 1 = ApiVersionsRequest (ApiVersion v) <$> kafkaArray signedInt16be

respondToRequest ::
     Pool.Pool PG.Connection -> ApiVersionsRequest -> IO KafkaResponseBox
respondToRequest pool (ApiVersionsRequest (ApiVersion 0) apiKeys) =
  return . KResp $
  ApiVersionsResponseV0 NoError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]

instance KafkaRequest ApiVersionsRequest where
  respond = respondToRequest
