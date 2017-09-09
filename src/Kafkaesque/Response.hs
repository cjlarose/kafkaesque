{-# LANGUAGE GADTs #-}

module Kafkaesque.Response
  ( putResponse
  , putKafkaNullabeBytes
  , putInt32be
  , putInt64be
  , putKafkaString
  ) where

import qualified Data.ByteString (length)
import Data.Serialize.Put (Put, putByteString, runPut)
import Kafkaesque.Request.KafkaRequest
       (Broker(..), PartitionMetadata(..), ProduceResponseTopic,
        Response(..), TopicMetadata(..))
import Kafkaesque.Serialize
       (putInt16be, putInt32be, putInt64be, putKafkaArray,
        putKafkaNullabeBytes, putKafkaNullableArray, putKafkaString,
        putKakfaError)

putProduceResponseTopic :: ProduceResponseTopic -> Put
putProduceResponseTopic (name, parts) =
  let putPartition (partitionId, err, baseOffset) =
        putInt32be partitionId *> putKakfaError err *> putInt64be baseOffset
  in putKafkaString name *> putKafkaArray putPartition parts

putResponse :: Response k v -> Put
putResponse (ProduceResponseV0 topics) =
  putKafkaArray putProduceResponseTopic topics
putResponse (ProduceResponseV1 topics throttleTime) =
  putKafkaArray putProduceResponseTopic topics *> putInt32be throttleTime
putResponse (MetadataResponseV0 brokers topicMetadata) =
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
putResponse (FetchResponseV0 topics) =
  let putPartitionHeader (partitionId, err, highWatermark) =
        putInt32be partitionId *> putKakfaError err *> putInt64be highWatermark
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
putResponse (OffsetsResponseV0 topics) =
  let putPartition (partitionId, err, offset) =
        putInt32be partitionId *> putKakfaError err *>
        putKafkaNullableArray putInt64be offset
      putTopic (topic, partitions) =
        putKafkaString topic *> putKafkaArray putPartition partitions
  in putKafkaArray putTopic topics
putResponse (OffsetCommitResponseV0 topics) =
  let putPartition (partitionId, err) =
        putInt32be partitionId *> putKakfaError err
      putTopic (topicName, parts) =
        putKafkaString topicName *> putKafkaArray putPartition parts
  in putKafkaArray putTopic topics
putResponse (OffsetFetchResponseV0 topics) =
  let putPartition (partitionId, offset, metadata, err) =
        putInt32be partitionId *> putInt64be offset *> putKafkaString metadata *>
        putKakfaError err
      putTopic (topicName, parts) =
        putKafkaString topicName *> putKafkaArray putPartition parts
  in putKafkaArray putTopic topics
putResponse (ApiVersionsResponseV0 err versions) =
  let putVersion (apiKey, minVersion, maxVersion) =
        putInt16be apiKey *> putInt16be minVersion *> putInt16be maxVersion
  in putKakfaError err *> putKafkaArray putVersion versions
