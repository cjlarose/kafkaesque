module Kafkaesque.Request.Produce
  ( produceRequestV0
  , produceRequestV1
  ) where

import Data.Attoparsec.Binary (anyWord32be)
import Data.Attoparsec.ByteString
       (Parser, anyWord8, many', parseOnly, take)
import Data.Int (Int16, Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.Simple as PG

import Kafkaesque.Message (Message(..), MessageSet)
import Kafkaesque.Parsers
       (kafkaArray, kafkaNullabeBytes, kafkaString, signedInt16be,
        signedInt32be, signedInt64be)
import Kafkaesque.Queries (getTopicPartition)
import Kafkaesque.Queries.Log (writeMessageSet)
import Kafkaesque.Request.KafkaRequest
       (KafkaRequest, KafkaResponseBox(..), respond)
import Kafkaesque.Response
       (KafkaError(NoError, UnknownTopicOrPartition),
        ProduceResponseTopic, ProduceResponseV0(..), ProduceResponseV1(..))

newtype TimeoutMs =
  TimeoutMs Int32

type PartitionData = (Int32, MessageSet)

type TopicData = (String, [PartitionData])

data ProduceRequestV0 =
  ProduceRequestV0 Int16
                   TimeoutMs
                   [TopicData]

data ProduceRequestV1 =
  ProduceRequestV1 Int16
                   TimeoutMs
                   [TopicData]

produceRequest :: Parser (Int16, TimeoutMs, [TopicData])
produceRequest =
  let message :: Parser Message
      message =
        Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*> kafkaNullabeBytes <*>
        kafkaNullabeBytes
      mesasgeSetElement :: Parser (Int64, Message)
      mesasgeSetElement =
        (\x y -> (x, y)) <$> (signedInt64be <* signedInt32be) <*> message
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

produceRequestV0 :: Parser ProduceRequestV0
produceRequestV0 =
  (\(acks, timeout, topics) -> ProduceRequestV0 acks timeout topics) <$>
  produceRequest

produceRequestV1 :: Parser ProduceRequestV1
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
          (return (UnknownTopicOrPartition, -1 :: Int64))
          (\(topicId, partitionId) -> do
             offset <- writeMessageSet conn topicId partitionId messageSet
             return (NoError, offset))
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

instance KafkaRequest ProduceRequestV0 where
  respond pool (ProduceRequestV0 _ _ topics) = do
    topicResponses <- respondToRequest pool topics
    return . KResp $ ProduceResponseV0 topicResponses

instance KafkaRequest ProduceRequestV1 where
  respond pool (ProduceRequestV1 _ _ topics) = do
    topicResponses <- respondToRequest pool topics
    let throttleTimeMs = 0 :: Int32
    return . KResp $ ProduceResponseV1 topicResponses throttleTimeMs
