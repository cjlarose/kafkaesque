module Kafkaesque.Response (Broker(..), KafkaError(..), PartitionMetadata(..), TopicMetadata(..), KafkaResponse(..), writeResponse) where

import Data.Int (Int32, Int64)
import Data.Serialize.Put (Put, runPut, putWord16be, putWord32be, putWord64be, putByteString)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)

data Broker = Broker Int String Int
data KafkaError = NoError | UnknownTopicOrPartition
data PartitionMetadata = PartitionMetadata KafkaError Int Int [Int] [Int]
data TopicMetadata = TopicMetadata KafkaError String [PartitionMetadata]
type ProduceResponsePartition = (Int32, KafkaError, Int64)
type ProduceResponseTopic = (String, [ProduceResponsePartition])
data KafkaResponse = ProduceResponseV0 [ProduceResponseTopic] Int32
                   | TopicMetadataResponseV0 [Broker] [TopicMetadata]

putInt16be :: Int -> Put
putInt16be = putWord16be . fromIntegral

putInt32be :: Int -> Put
putInt32be = putWord32be . fromIntegral

putInt64be :: Int -> Put
putInt64be = putWord64be . fromIntegral

putKafkaString :: String -> Put
putKafkaString s = (putInt16be . length $ s) *> putByteString (fromString s)

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs = (putInt32be . length $ xs) *> mapM_ putter xs

kafkaErrorCode :: KafkaError -> Int
kafkaErrorCode NoError = 0
kafkaErrorCode UnknownTopicOrPartition = 3

putKakfaError :: KafkaError -> Put
putKakfaError = putInt16be . kafkaErrorCode

putProduceResponse :: KafkaResponse -> Put
putProduceResponse (ProduceResponseV0 topics throttleTime) =
  let
    putPartition (partitionId, err, baseOffset) = (putInt32be . fromIntegral $ partitionId) *> putKakfaError err *> (putInt64be . fromIntegral $ baseOffset)
    putTopic (name, parts) = putKafkaString name *> putKafkaArray putPartition parts
  in
    putKafkaArray putTopic topics *> (putInt32be . fromIntegral $ throttleTime)

putTopicMetadataResponse :: KafkaResponse -> Put
putTopicMetadataResponse (TopicMetadataResponseV0 brokers topicMetadata) =
  let
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
writeResponse resp@(ProduceResponseV0 _ _) = runPut . putProduceResponse $ resp
writeResponse resp@(TopicMetadataResponseV0 _ _) = runPut . putTopicMetadataResponse $ resp
