{-# LANGUAGE GADTs #-}

module Kafkaesque.Request.Produce
  ( produceRequestV0
  , produceRequestV1
  , respondToRequestV0
  , respondToRequestV1
  ) where

import Data.Attoparsec.ByteString (Parser, many', parseOnly, take)
import Data.Int (Int16, Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.KafkaError (noError, unknownTopicOrPartition)
import Kafkaesque.Message (Message, MessageSet, messageParser)
import Kafkaesque.Parsers
       (kafkaArray, kafkaString, signedInt16be, signedInt32be,
        signedInt64be)
import Kafkaesque.Protocol.ApiKey (Produce)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.Log (writeMessageSet)
import Kafkaesque.Request.KafkaRequest
       (APIVersion0, APIVersion1, PartitionData, ProduceResponseTopic,
        Request(..), Response(..), TimeoutMs(..), TopicData)

produceRequest :: Parser (Int16, TimeoutMs, [TopicData])
produceRequest =
  let mesasgeSetElement :: Parser (Int64, Message)
      mesasgeSetElement =
        (\x y -> (x, y)) <$> (signedInt64be <* signedInt32be) <*> messageParser
      records :: Parser MessageSet
      records = do
        messageSetSize <- signedInt32be
        bs <- Data.Attoparsec.ByteString.take . fromIntegral $ messageSetSize
        case parseOnly (many' mesasgeSetElement) bs of
          Left err -> fail err
          Right xs -> return xs
      partitionData :: Parser PartitionData
      partitionData =
        (\partitionId msgs -> (partitionId, msgs)) <$> signedInt32be <*> records
      topicData :: Parser TopicData
      topicData =
        (\name parts -> (name, parts)) <$> kafkaString <*>
        (fromMaybe [] <$> kafkaArray partitionData)
  in (\a b c -> (a, b, c)) <$> signedInt16be <*> (TimeoutMs <$> signedInt32be) <*>
     (fromMaybe [] <$> kafkaArray topicData)

produceRequestV0 :: Parser (Request Produce APIVersion0)
produceRequestV0 =
  (\(acks, timeout, topics) -> ProduceRequestV0 acks timeout topics) <$>
  produceRequest

produceRequestV1 :: Parser (Request Produce APIVersion1)
produceRequestV1 =
  (\(acks, timeout, topics) -> ProduceRequestV1 acks timeout topics) <$>
  produceRequest

respondToRequest ::
     Pool.Pool PG.Connection -> [TopicData] -> IO [ProduceResponseTopic]
respondToRequest pool
  -- TODO: Fetch topicIds in bulk
 = do
  let writePartitionMessages topicName partitionId messageSet conn = do
        topicPartitionRes <- getTopicPartition conn topicName partitionId
        maybe
          (return (unknownTopicOrPartition, -1 :: Int64))
          (\(topicId, _) -> do
             offset <-
               writeMessageSet conn topicId partitionId (map snd messageSet)
             return (noError, offset))
          topicPartitionRes
      getPartitionResponse topicName (partitionId, messageSet) = do
        (err, offset) <-
          Pool.withResource
            pool
            (writePartitionMessages topicName partitionId messageSet)
        return (partitionId, err, offset)
      getTopicResponse (topicName, parts) = do
        partResponses <- mapM (getPartitionResponse topicName) parts
        return (topicName, partResponses)
  mapM getTopicResponse

respondToRequestV0 ::
     Pool.Pool PG.Connection
  -> Request Produce APIVersion0
  -> IO (Response Produce APIVersion0)
respondToRequestV0 pool (ProduceRequestV0 _ _ topics) = do
  topicResponses <- respondToRequest pool topics
  return $ ProduceResponseV0 topicResponses

respondToRequestV1 ::
     Pool.Pool PG.Connection
  -> Request Produce APIVersion1
  -> IO (Response Produce APIVersion1)
respondToRequestV1 pool (ProduceRequestV1 _ _ topics) = do
  topicResponses <- respondToRequest pool topics
  let throttleTimeMs = 0 :: Int32
  return $ ProduceResponseV1 topicResponses throttleTimeMs
