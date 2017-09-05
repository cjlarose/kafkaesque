module Kafkaesque.KafkaError
  ( KafkaError
  , noError
  , offsetOutOfRange
  , unknownTopicOrPartition
  , unexpectedError
  , kafkaErrorCode
  , unsupportedForMessageFormat
  ) where

import Data.Int (Int16)

newtype KafkaError =
  KafkaError Int16

noError :: KafkaError
noError = KafkaError 0

offsetOutOfRange :: KafkaError
offsetOutOfRange = KafkaError 1

unknownTopicOrPartition :: KafkaError
unknownTopicOrPartition = KafkaError 3

unsupportedForMessageFormat :: KafkaError
unsupportedForMessageFormat = KafkaError 43

unexpectedError :: KafkaError
unexpectedError = KafkaError (-1)

kafkaErrorCode :: KafkaError -> Int16
kafkaErrorCode (KafkaError errorCode) = errorCode
