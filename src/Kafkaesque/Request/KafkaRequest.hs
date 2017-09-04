{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Kafkaesque.Request.KafkaRequest
  ( KafkaRequest
  , KafkaRequestBox(..)
  , respond
  , KafkaResponse
  , KafkaResponseBox(..)
  , put
  ) where

import qualified Data.Pool as Pool
import Data.Serialize.Put (Put)
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion)

class KafkaResponse a where
  put :: a -> Put

data KafkaResponseBox =
  forall a. KafkaResponse a =>
            KResp a

class KafkaRequest a b where
  respond :: Pool.Pool PG.Connection -> a -> IO b

data KafkaRequestBox =
  forall a b. KafkaRequest a b =>
    KR a b
