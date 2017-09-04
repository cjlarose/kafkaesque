module Kafkaesque.Response
  ( Broker(..)
  , PartitionMetadata(..)
  , TopicMetadata(..)
  , writeResponse
  , putKafkaNullabeBytes
  , putInt32be
  , putInt64be
  , putKafkaString
  , ProduceResponseV0(..)
  , ProduceResponseV1(..)
  , FetchResponseV0(..)
  , OffsetListResponseV0(..)
  , TopicMetadataResponseV0(..)
  , OffsetCommitResponseV0(..)
  , OffsetFetchResponseV0(..)
  , ApiVersionsResponseV0(..)
  , ProduceResponseTopic
  , FetchResponseTopic
  , FetchResponsePartition
  , OffsetListResponseTopic
  , OffsetListResponsePartition
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString (length)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int16, Int32, Int64)
import Data.Serialize.Put
       (Put, putByteString, putWord16be, putWord32be, putWord64be, runPut)
import Kafkaesque.KafkaError (KafkaError, kafkaErrorCode)
import Kafkaesque.Request.KafkaRequest
       (KafkaResponse, KafkaResponseBox(..), put)

data Broker =
  Broker Int32
         String
         Int32

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

type FetchResponsePartition = (PartitionHeader, [(Int64, ByteString)])

type FetchResponseTopic = (String, [FetchResponsePartition])

type OffsetListResponsePartition = (Int32, KafkaError, Maybe [Int64])

type OffsetListResponseTopic = (String, [OffsetListResponsePartition])

newtype ProduceResponseV0 =
  ProduceResponseV0 [ProduceResponseTopic]

data ProduceResponseV1 =
  ProduceResponseV1 [ProduceResponseTopic]
                    Int32

newtype FetchResponseV0 =
  FetchResponseV0 [FetchResponseTopic]

newtype OffsetListResponseV0 =
  OffsetListResponseV0 [OffsetListResponseTopic]

data TopicMetadataResponseV0 =
  TopicMetadataResponseV0 [Broker]
                          [TopicMetadata]

newtype OffsetCommitResponseV0 =
  OffsetCommitResponseV0 [(String, [(Int32, KafkaError)])]

newtype OffsetFetchResponseV0 =
  OffsetFetchResponseV0 [(String, [(Int32, Int64, String, KafkaError)])]

data ApiVersionsResponseV0 =
  ApiVersionsResponseV0 KafkaError
                        [(Int16, Int16, Int16)]

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

putKafkaNullableArray :: (a -> Put) -> Maybe [a] -> Put
putKafkaNullableArray _ Nothing = putInt32be (-1)
putKafkaNullableArray putter (Just xs) = putKafkaArray putter xs

putKakfaError :: KafkaError -> Put
putKakfaError = putInt16be . kafkaErrorCode

putProduceResponseTopic :: ProduceResponseTopic -> Put
putProduceResponseTopic (name, parts) =
  let putPartition (partitionId, err, baseOffset) =
        putInt32be partitionId *> putKakfaError err *> putInt64be baseOffset
  in putKafkaString name *> putKafkaArray putPartition parts

instance KafkaResponse ProduceResponseV0 where
  put (ProduceResponseV0 topics) = putKafkaArray putProduceResponseTopic topics

instance KafkaResponse ProduceResponseV1 where
  put (ProduceResponseV1 topics throttleTime) =
    putKafkaArray putProduceResponseTopic topics *> putInt32be throttleTime

instance KafkaResponse TopicMetadataResponseV0 where
  put (TopicMetadataResponseV0 brokers topicMetadata) =
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

instance KafkaResponse FetchResponseV0 where
  put (FetchResponseV0 topics) =
    let putPartitionHeader (partitionId, err, highWatermark) =
          putInt32be partitionId *> putKakfaError err *>
          putInt64be highWatermark
        putMessageSet =
          mapM_
            (\(offset, messageBytes) ->
               putInt64be offset *>
               putInt32be (fromIntegral . Data.ByteString.length $ messageBytes) *>
               putByteString messageBytes)
        putPartition (header, messageSet) = do
          let messageSetBytes = runPut $ putMessageSet messageSet
          let messageSetLen =
                fromIntegral $ Data.ByteString.length messageSetBytes
          putPartitionHeader header
          putInt32be messageSetLen
          putByteString messageSetBytes
        putTopic (topic, partitions) =
          putKafkaString topic *> putKafkaArray putPartition partitions
    in putKafkaArray putTopic topics

instance KafkaResponse OffsetListResponseV0 where
  put (OffsetListResponseV0 topics) =
    let putPartition (partitionId, err, offset) =
          putInt32be partitionId *> putKakfaError err *>
          putKafkaNullableArray putInt64be offset
        putTopic (topic, partitions) =
          putKafkaString topic *> putKafkaArray putPartition partitions
    in putKafkaArray putTopic topics

instance KafkaResponse ApiVersionsResponseV0 where
  put (ApiVersionsResponseV0 err versions) =
    let putVersion (apiKey, minVersion, maxVersion) =
          putInt16be apiKey *> putInt16be minVersion *> putInt16be maxVersion
    in putKakfaError err *> putKafkaArray putVersion versions

instance KafkaResponse OffsetCommitResponseV0 where
  put (OffsetCommitResponseV0 topics) =
    let putPartition (partitionId, err) =
          putInt32be partitionId *> putKakfaError err
        putTopic (topicName, parts) =
          putKafkaString topicName *> putKafkaArray putPartition parts
    in putKafkaArray putTopic topics

instance KafkaResponse OffsetFetchResponseV0 where
  put (OffsetFetchResponseV0 topics) =
    let putPartition (partitionId, offset, metadata, err) =
          putInt32be partitionId *> putInt64be offset *> putKafkaString metadata *>
          putKakfaError err
        putTopic (topicName, parts) =
          putKafkaString topicName *> putKafkaArray putPartition parts
    in putKafkaArray putTopic topics

writeResponse :: KafkaResponseBox -> ByteString
writeResponse (KResp resp) = runPut . put $ resp
