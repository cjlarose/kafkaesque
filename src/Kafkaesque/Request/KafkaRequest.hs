{-# LANGUAGE ExistentialQuantification #-}

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

class KafkaResponse a where
  put :: a -> Put

data KafkaResponseBox =
  forall a. KafkaResponse a =>
            KResp a

class KafkaRequest a where
  respond :: Pool.Pool PG.Connection -> a -> IO KafkaResponseBox

data KafkaRequestBox =
  forall a. KafkaRequest a =>
            KR a
