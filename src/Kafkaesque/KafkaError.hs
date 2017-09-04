module Kafkaesque.KafkaError
  ( KafkaError
  , noError
  , offsetOutOfRange
  , unknownTopicOrPartition
  , unexpectedError
  , kafkaErrorCode
  ) where

import Data.Int (Int16)

newtype KafkaError =
  KafkaError Int16

noError = KafkaError 0

offsetOutOfRange = KafkaError 1

unknownTopicOrPartition = KafkaError 3

unexpectedError = KafkaError (-1)

kafkaErrorCode :: KafkaError -> Int16
kafkaErrorCode (KafkaError errorCode) = errorCode
