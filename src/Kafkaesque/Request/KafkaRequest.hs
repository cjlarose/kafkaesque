{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.KafkaRequest
  ( APIVersion0
  , APIVersion1
  , APIKeyFetch
  , APIKeyProduce
  , APIKeyOffsets
  , APIKeyMetadata
  , APIKeyOffsetCommit
  , APIKeyOffsetFetch
  , APIKeyApiVersions
  , Request(..)
  , Response(..)
  , TimeoutMs(..)
  , TopicData
  , PartitionData
  , ProduceResponseTopic
  , FetchResponseTopic
  , FetchResponsePartition
  , FetchRequestPartition
  , FetchRequestTopic
  , OffsetListResponsePartition
  , OffsetListResponseTopic
  , Broker(..)
  , PartitionMetadata(..)
  , TopicMetadata(..)
  , OffsetListRequestPartition
  , OffsetListRequestTopic
  , OffsetListRequestTimestamp(..)
  , OffsetCommitPartitionData
  , OffsetCommitTopicData
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import Kafkaesque.KafkaError (KafkaError)
import Kafkaesque.Message (MessageSet)

data APIVersion0

data APIVersion1

data APIKeyProduce

data APIKeyFetch

data APIKeyOffsets

data APIKeyMetadata

data APIKeyOffsetCommit

data APIKeyOffsetFetch

data APIKeyApiVersions

-- Produce
newtype TimeoutMs =
  TimeoutMs Int32

type PartitionData = (Int32, MessageSet)

type TopicData = (String, [PartitionData])

-- Fetch
type FetchRequestPartition = (Int32, Int64, Int32)

type FetchRequestTopic = (String, [FetchRequestPartition])

-- Offsets
data OffsetListRequestTimestamp
  = LatestOffset
  | EarliestOffset
  | OffsetListTimestamp Int64

type OffsetListRequestPartition = (Int32, OffsetListRequestTimestamp, Int32)

type OffsetListRequestTopic = (String, [OffsetListRequestPartition])

-- Metadata
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

-- OffsetCommit
type OffsetCommitPartitionData = (Int32, Int64, String)

type OffsetCommitTopicData = (String, [OffsetCommitPartitionData])

data Request k v where
  ProduceRequestV0
    :: Int16 -> TimeoutMs -> [TopicData] -> Request APIKeyProduce APIVersion0
  ProduceRequestV1
    :: Int16 -> TimeoutMs -> [TopicData] -> Request APIKeyProduce APIVersion1
  FetchRequestV0
    :: Int32
    -> Int32
    -> Int32
    -> [FetchRequestTopic]
    -> Request APIKeyFetch APIVersion0
  OffsetsRequestV0
    :: Int32 -> [OffsetListRequestTopic] -> Request APIKeyOffsets APIVersion0
  MetadataRequestV0 :: Maybe [String] -> Request APIKeyMetadata APIVersion0
  OffsetCommitRequestV0
    :: String
    -> [OffsetCommitTopicData]
    -> Request APIKeyOffsetCommit APIVersion0
  OffsetFetchRequestV0
    :: String -> [(String, [Int32])] -> Request APIKeyOffsetFetch APIVersion0
  ApiVersionsRequestV0 :: Maybe [Int16] -> Request APIKeyApiVersions APIVersion0

-- Produce
type ProduceResponsePartition = (Int32, KafkaError, Int64)

type ProduceResponseTopic = (String, [ProduceResponsePartition])

-- Fetch
type PartitionHeader = (Int32, KafkaError, Int64)

type FetchResponsePartition = (PartitionHeader, [(Int64, ByteString)])

type FetchResponseTopic = (String, [FetchResponsePartition])

-- Offsets
type OffsetListResponsePartition = (Int32, KafkaError, Maybe [Int64])

type OffsetListResponseTopic = (String, [OffsetListResponsePartition])

data Response k v where
  ProduceResponseV0
    :: [ProduceResponseTopic] -> Response APIKeyProduce APIVersion0
  ProduceResponseV1
    :: [ProduceResponseTopic] -> Int32 -> Response APIKeyProduce APIVersion1
  FetchResponseV0 :: [FetchResponseTopic] -> Response APIKeyFetch APIVersion0
  OffsetsResponseV0
    :: [OffsetListResponseTopic] -> Response APIKeyOffsets APIVersion0
  MetadataResponseV0
    :: [Broker] -> [TopicMetadata] -> Response APIKeyMetadata APIVersion0
  OffsetCommitResponseV0
    :: [(String, [(Int32, KafkaError)])]
    -> Response APIKeyOffsetCommit APIVersion0
  OffsetFetchResponseV0
    :: [(String, [(Int32, Int64, String, KafkaError)])]
    -> Response APIKeyOffsetFetch APIVersion0
  ApiVersionsResponseV0
    :: KafkaError
    -> [(Int16, Int16, Int16)]
    -> Response APIKeyApiVersions APIVersion0
