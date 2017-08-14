module Kafkaesque.Response (Broker(..), KafkaError(..), PartitionMetadata(..), TopicMetadata(..), KafkaResponse(..), writeResponse) where

import Data.Int (Int32)
import Data.Serialize.Put (Put, runPut, putWord16be, putWord32be, putByteString)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)

data Broker = Broker Int String Int
data KafkaError = NoError | UnknownTopicOrPartition
data PartitionMetadata = PartitionMetadata KafkaError Int Int [Int] [Int]
data TopicMetadata = TopicMetadata KafkaError String [PartitionMetadata]
data KafkaResponse = TopicMetadataResponseV0 [Broker] [TopicMetadata]

putInt16be :: Int -> Put
putInt16be = putWord16be . fromIntegral

putInt32be :: Int -> Put
putInt32be = putWord32be . fromIntegral

putKafkaString :: String -> Put
putKafkaString s = (putInt16be . length $ s) *> putByteString (fromString s)

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs = (putInt32be . length $ xs) *> mapM_ putter xs

kafkaErrorCode :: KafkaError -> Int
kafkaErrorCode NoError = 0
kafkaErrorCode UnknownTopicOrPartition = 3

putTopicMetadataResponse :: KafkaResponse -> Put
putTopicMetadataResponse (TopicMetadataResponseV0 brokers topicMetadata) =
  let
    putKakfaError = putInt16be . kafkaErrorCode

    putBroker (Broker nodeId host port) = putInt32be nodeId *> putKafkaString host *> putInt32be port

    putPartitionMetadata (PartitionMetadata err partitionId leader replicas isr) =
      putKakfaError err *>
      putInt32be partitionId *>
      putInt32be leader *>
      putKafkaArray putInt32be replicas *>
      putKafkaArray putInt32be isr

    putTopicMetadata (TopicMetadata err name partitionMetadata) =
      putKakfaError err *>
      putKafkaString name *>
      putKafkaArray putPartitionMetadata partitionMetadata
  in
    putKafkaArray putBroker brokers *> putKafkaArray putTopicMetadata topicMetadata

writeResponse :: KafkaResponse -> ByteString
writeResponse req@(TopicMetadataResponseV0 _ _) = runPut . putTopicMetadataResponse $ req
