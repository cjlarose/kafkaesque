module Kafkaesque.Request.ApiVersions
  ( apiVersionsRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.Int (Int16)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError (noError)
import Kafkaesque.Parsers (kafkaArray, signedInt16be)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response (ApiVersionsResponseV0(..))

newtype ApiVersionsRequestV0 =
  ApiVersionsRequestV0 (Maybe [Int16])

apiVersionsRequestV0 :: Parser ApiVersionsRequestV0
apiVersionsRequestV0 = ApiVersionsRequestV0 <$> kafkaArray signedInt16be

respondToRequest ::
     Pool.Pool PG.Connection -> ApiVersionsRequestV0 -> IO KafkaResponseBox
respondToRequest pool (ApiVersionsRequestV0 apiKeys) =
  return . KResp $
  ApiVersionsResponseV0 noError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]

instance KafkaRequest ApiVersionsRequestV0 where
  respond = respondToRequest
