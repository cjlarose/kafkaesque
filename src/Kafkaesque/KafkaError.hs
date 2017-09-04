module Kafkaesque.KafkaError
  ( KafkaError(..)
  , kafkaErrorCode
  ) where

import Data.Int (Int16)

data KafkaError
  = NoError
  | OffsetOutOfRange
  | UnknownTopicOrPartition
  | UnexpectedError

kafkaErrorCode :: KafkaError -> Int16
kafkaErrorCode NoError = 0
kafkaErrorCode OffsetOutOfRange = 1
kafkaErrorCode UnknownTopicOrPartition = 3
kafkaErrorCode UnexpectedError = -1
