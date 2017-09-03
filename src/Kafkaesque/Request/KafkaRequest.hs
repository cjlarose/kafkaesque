{-# LANGUAGE ExistentialQuantification #-}

module Kafkaesque.Request.KafkaRequest
  ( KafkaRequest
  , KafkaRequestBox(..)
  , respond
  ) where

import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Request.ApiVersion (ApiVersion)
import Kafkaesque.Response (KafkaResponse)

class KafkaRequest a where
  respond :: Pool.Pool PG.Connection -> a -> IO KafkaResponse

data KafkaRequestBox =
  forall a. KafkaRequest a =>
            KR a
