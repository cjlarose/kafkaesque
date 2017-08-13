module Kafkaesque.Response (Broker(..), KafkaError(..), PartitionMetadata(..), TopicMetadata(..), KafkaResponse(..), writeResponse) where

import Data.Int (Int32)
import Data.Serialize.Put (Put, runPut, putWord16be, putWord32be, putByteString)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)

data Broker = Broker Int32 String Int32
data KafkaError = NoError | UnknownTopicOrPartition
data PartitionMetadata = PartitionMetadata KafkaError Int32 Int32 [Int32] [Int32]
data TopicMetadata = TopicMetadata KafkaError String [PartitionMetadata]
data KafkaResponse = TopicMetadataResponseV0 [Broker] [TopicMetadata]

putKafkaString :: String -> Put
putKafkaString s = (putWord16be . fromIntegral . length $ s) *> putByteString (fromString s)

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs = (putWord32be . fromIntegral . length $ xs) *> mapM_ putter xs

kafkaErrorCode :: KafkaError -> Int
kafkaErrorCode NoError = 0
kafkaErrorCode UnknownTopicOrPartition = 3

putTopicMetadataResponse :: KafkaResponse -> Put
putTopicMetadataResponse (TopicMetadataResponseV0 brokers topicMetadata) =
  let
    putInt32be = putWord32be . fromIntegral
    putKakfaError = putWord16be . fromIntegral . kafkaErrorCode

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
