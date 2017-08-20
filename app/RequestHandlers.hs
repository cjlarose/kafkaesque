{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module RequestHandlers
  ( handleRequest
  ) where

import Control.Monad (forM, forM_)
import Data.Attoparsec.ByteString (endOfInput, parseOnly)
import Data.ByteString (ByteString)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int32, Int64)
import qualified Data.Pool as Pool
import Data.Serialize.Put (putByteString, putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)

import Kafkaesque.Message (Message, MessageSet)
import Kafkaesque.Request
       (ApiVersion(..), KafkaRequest(..), kafkaRequest)
import Kafkaesque.Response
       (Broker(..), KafkaError(..), KafkaResponse(..),
        PartitionMetadata(..), TopicMetadata(..), putMessage,
        writeResponse)

getTopicId :: PG.Connection -> String -> IO Int32
getTopicId conn topicName = do
  let query = "SELECT id FROM topics WHERE name = ?"
  [PG.Only topicId] <-
    PG.query conn query (PG.Only topicName) :: IO [PG.Only Int32]
  return topicId

getNextOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getNextOffset conn topicId partitionId = do
  let query =
        "SELECT next_offset FROM partitions WHERE topic_id = ? AND partition_id = ? FOR UPDATE"
  [PG.Only baseOffset] <-
    PG.query conn query (topicId, partitionId) :: IO [PG.Only Int64]
  return baseOffset

insertMessage :: PG.Connection -> Int32 -> Int32 -> Int64 -> Message -> IO ()
insertMessage conn topicId partitionId offset message = do
  let messageBytes = PG.Binary . runPut $ putMessage message
  let query =
        "INSERT INTO records (topic_id, partition_id, record, base_offset) VALUES (?, ?, ?, ?)"
  PG.execute conn query (topicId, partitionId, messageBytes, offset)
  return ()

incrementNextOffset :: PG.Connection -> Int32 -> Int32 -> Int -> IO ()
incrementNextOffset conn topicId partitionId amount = do
  let query =
        "UPDATE partitions SET next_offset = next_offset + ? WHERE topic_id = ? AND partition_id = ?"
  PG.execute conn query (amount, topicId, partitionId)
  return ()

writeMessageSet ::
     Pool.Pool PG.Connection
  -> String
  -> Int32
  -> MessageSet
  -> IO (KafkaError, Int64)
writeMessageSet pool topic partition messages =
  Pool.withResource
    pool
    (\conn -> do
       topicId <- getTopicId conn topic
       baseOffset <-
         PG.withTransaction conn $ do
           baseOffset <- getNextOffset conn topicId partition
           forM_
             (zip messages [0 ..])
             (\((_, message), idx) ->
                insertMessage conn topicId partition (baseOffset + idx) message)
           incrementNextOffset conn topicId partition $ Prelude.length messages
           return baseOffset
       return (NoError, baseOffset))

getTopics :: PG.Connection -> IO [(String, Int64)]
getTopics conn = do
  let query =
        [sql| SELECT name, partition_count
              FROM (SELECT topic_id, COUNT(*) AS partition_count
                    FROM partitions
                    GROUP BY topic_id) t1
              LEFT JOIN topics ON t1.topic_id = topics.id; |]
  PG.query_ conn query

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool (ProduceRequest (ApiVersion 1) acks timeout ts) = do
  topicResponses <-
    forM
      ts
      (\(topic, parts) -> do
         partResponses <-
           forM
             parts
             (\(partitionId, messageSet) -> do
                (err, offset) <-
                  writeMessageSet pool topic partitionId messageSet
                return (partitionId, err, offset))
         return (topic, partResponses))
  let throttleTimeMs = 0 :: Int32
  return $ ProduceResponseV0 topicResponses throttleTimeMs
respondToRequest pool (TopicMetadataRequest (ApiVersion 0) ts) = do
  let brokerNodeId = 42
  let brokers = [Broker brokerNodeId "localhost" 9092]
  let makePartitionMetadata partitionId =
        PartitionMetadata
          NoError
          (fromIntegral partitionId)
          brokerNodeId
          [brokerNodeId]
          [brokerNodeId]
  let makeTopicMetadata (name, partitionCount) =
        TopicMetadata
          NoError
          name
          (map makePartitionMetadata [0 .. (partitionCount - 1)])
  topics <- Pool.withResource pool getTopics
  let topicMetadata = map makeTopicMetadata topics
  return $ TopicMetadataResponseV0 brokers topicMetadata

handleRequest :: Pool.Pool PG.Connection -> ByteString -> IO ByteString
handleRequest pool request =
  case parseOnly (kafkaRequest <* endOfInput) request of
    Left err -> return . fromString $ err
    Right ((_, _, correlationId, _), req) -> do
      response <- respondToRequest pool req
      let putCorrelationId = putWord32be . fromIntegral $ correlationId
          putResponse = putByteString . writeResponse $ response
      return . runPut $ putCorrelationId *> putResponse
