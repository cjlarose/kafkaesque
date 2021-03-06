{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.ApiVersions
  ( apiVersionsRequestV0
  , respondToRequestV0
  ) where

import Data.Attoparsec.ByteString (Parser)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError (noError)
import Kafkaesque.Parsers (kafkaArray, signedInt16be)
import Kafkaesque.Protocol.ApiKey (ApiVersions)
import Kafkaesque.Protocol.ApiVersion (V0)
import Kafkaesque.Request.KafkaRequest
       (Request(ApiVersionsRequestV0), Response(ApiVersionsResponseV0))

apiVersionsRequestV0 :: Parser (Request ApiVersions V0)
apiVersionsRequestV0 = ApiVersionsRequestV0 <$> kafkaArray signedInt16be

respondToRequestV0 ::
     Pool.Pool PG.Connection
  -> Request ApiVersions V0
  -> IO (Response ApiVersions V0)
respondToRequestV0 _ (ApiVersionsRequestV0 _) =
  return $
  ApiVersionsResponseV0 noError [(0, 0, 1), (1, 0, 0), (3, 0, 0), (18, 0, 0)]
