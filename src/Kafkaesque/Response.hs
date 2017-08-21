module Kafkaesque.Response
  ( Broker(..)
  , KafkaError(..)
  , PartitionMetadata(..)
  , TopicMetadata(..)
  , KafkaResponse(..)
  , writeResponse
  , putMessage
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString (length)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int16, Int32, Int64)
import Data.Serialize.Put
       (Put, putByteString, putWord16be, putWord32be, putWord64be,
        putWord8, runPut)
import Kafkaesque.Message (Message(..), MessageSet)

data Broker =
  Broker Int32
         String
         Int32

data KafkaError
  = NoError
  | UnknownTopicOrPartition

data PartitionMetadata =
  PartitionMetadata KafkaError
                    Int32
                    Int32
                    [Int32]
                    [Int32]

data TopicMetadata =
  TopicMetadata KafkaError
                String
                [PartitionMetadata]

type ProduceResponsePartition = (Int32, KafkaError, Int64)

type ProduceResponseTopic = (String, [ProduceResponsePartition])

type PartitionHeader = (Int32, KafkaError, Int64)

type FetchResponsePartition = (PartitionHeader, MessageSet)

type FetchResponseTopic = (String, [FetchResponsePartition])

data KafkaResponse
  = ProduceResponseV0 [ProduceResponseTopic]
                      Int32
  | FetchResponseV0 [FetchResponseTopic]
  | TopicMetadataResponseV0 [Broker]
                            [TopicMetadata]

putInt16be :: Int16 -> Put
putInt16be = putWord16be . fromIntegral

putInt32be :: Int32 -> Put
putInt32be = putWord32be . fromIntegral

putInt64be :: Int64 -> Put
putInt64be = putWord64be . fromIntegral

putKafkaString :: String -> Put
putKafkaString s =
  (putInt16be . fromIntegral . length $ s) *> putByteString (fromString s)

putKafkaBytes :: ByteString -> Put
putKafkaBytes bs =
  (putInt32be . fromIntegral . Data.ByteString.length $ bs) *> putByteString bs

putKafkaNullabeBytes :: Maybe ByteString -> Put
putKafkaNullabeBytes bs =
  case bs of
    Nothing -> putInt32be (-1)
    Just bs ->
      (putInt32be . fromIntegral . Data.ByteString.length $ bs) *>
      putByteString bs

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs =
  (putInt32be . fromIntegral . length $ xs) *> mapM_ putter xs

kafkaErrorCode :: KafkaError -> Int16
kafkaErrorCode NoError = 0
kafkaErrorCode UnknownTopicOrPartition = 3

putKakfaError :: KafkaError -> Put
putKakfaError = putInt16be . kafkaErrorCode

putMessage :: Message -> Put
putMessage (Message crc32 magicByte attrs k v) =
  putWord32be crc32 *> putWord8 magicByte *> putWord8 attrs *>
  putKafkaNullabeBytes k *>
  putKafkaNullabeBytes v

putProduceResponse :: KafkaResponse -> Put
putProduceResponse (ProduceResponseV0 topics throttleTime) =
  let putPartition (partitionId, err, baseOffset) =
        putInt32be partitionId *> putKakfaError err *> putInt64be baseOffset
      putTopic (name, parts) =
        putKafkaString name *> putKafkaArray putPartition parts
  in putKafkaArray putTopic topics *> putInt32be throttleTime

putTopicMetadataResponse :: KafkaResponse -> Put
putTopicMetadataResponse (TopicMetadataResponseV0 brokers topicMetadata) =
  let putBroker (Broker nodeId host port) =
        putInt32be nodeId *> putKafkaString host *> putInt32be port
      putPartitionMetadata (PartitionMetadata err partitionId leader replicas isr) =
        putKakfaError err *> putInt32be partitionId *> putInt32be leader *>
        putKafkaArray putInt32be replicas *>
        putKafkaArray putInt32be isr
      putTopicMetadata (TopicMetadata err name partitionMetadata) =
        putKakfaError err *> putKafkaString name *>
        putKafkaArray putPartitionMetadata partitionMetadata
  in putKafkaArray putBroker brokers *>
     putKafkaArray putTopicMetadata topicMetadata

putFetchResponse :: KafkaResponse -> Put
putFetchResponse (FetchResponseV0 topics) =
  let putPartitionHeader (partitionId, err, highWatermark) =
        putInt32be partitionId *> putKakfaError err *> putInt64be highWatermark
      putMessageSet =
        mapM_
          (\(offset, message) -> do
             let messageBytes = runPut . putMessage $ message
             putInt64be offset *>
               putInt32be (fromIntegral . Data.ByteString.length $ messageBytes) *>
               putByteString messageBytes)
      putPartition (header, messageSet) =
        putPartitionHeader header *> putMessageSet messageSet
      putTopic (topic, partitions) =
        putKafkaString topic *> putKafkaArray putPartition partitions
  in putKafkaArray putTopic topics

writeResponse :: KafkaResponse -> ByteString
writeResponse resp@(ProduceResponseV0 _ _) = runPut . putProduceResponse $ resp
writeResponse resp@(FetchResponseV0 _) = runPut . putFetchResponse $ resp
writeResponse resp@(TopicMetadataResponseV0 _ _) =
  runPut . putTopicMetadataResponse $ resp
