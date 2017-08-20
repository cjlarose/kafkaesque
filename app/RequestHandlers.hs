{-# LANGUAGE OverloadedStrings #-}

module RequestHandlers (handleRequest) where

import Data.Int (Int32, Int64)
import Control.Monad (forM, forM_)
import Data.Serialize.Put (runPut, putWord32be, putByteString)
import Data.ByteString.UTF8 (fromString)
import Data.ByteString (ByteString)
import qualified Database.PostgreSQL.Simple as PG
import qualified Data.Pool as Pool
import Data.Attoparsec.ByteString (parseOnly, endOfInput)

import Kafkaesque.Message (MessageSet, Message)
import Kafkaesque.Request (KafkaRequest(..), ApiVersion(..), kafkaRequest)
import Kafkaesque.Response (Broker(..), TopicMetadata(..), PartitionMetadata(..), KafkaError(..), KafkaResponse(..), putMessage, writeResponse)

getTopicId :: PG.Connection -> String -> IO Int32
getTopicId conn topicName = do
  let query = "SELECT id FROM topics WHERE name = ?"
  [PG.Only topicId] <- PG.query conn query (PG.Only topicName) :: IO [PG.Only Int32]
  return topicId

getNextOffset :: PG.Connection -> Int32 -> Int32 -> IO Int64
getNextOffset conn topicId partitionId = do
  let query = "SELECT next_offset FROM partitions WHERE topic_id = ? AND partition_id = ? FOR UPDATE"
  [PG.Only baseOffset] <- PG.query conn query (topicId, partitionId) :: IO [PG.Only Int64]
  return baseOffset

insertMessage :: PG.Connection -> Int32 -> Int32 -> Int64 -> Message -> IO ()
insertMessage conn topicId partitionId offset message = do
  let messageBytes = PG.Binary . runPut $ putMessage message
  let query = "INSERT INTO records (topic_id, partition_id, record, base_offset) VALUES (?, ?, ?, ?)"
  PG.execute conn query (topicId, partitionId, messageBytes, offset)
  return ()

incrementNextOffset :: PG.Connection -> Int32 -> Int32 -> Int -> IO ()
incrementNextOffset conn topicId partitionId amount = do
  let query = "UPDATE partitions SET next_offset = next_offset + ? WHERE topic_id = ? AND partition_id = ?"
  PG.execute conn query (amount, topicId, partitionId)
  return ()

writeMessageSet :: Pool.Pool PG.Connection -> String -> Int32 -> MessageSet -> IO (KafkaError, Int64)
writeMessageSet pool topic partition messages =
  Pool.withResource pool (\conn -> do
    topicId <- getTopicId conn topic
    baseOffset <- PG.withTransaction conn $ do
                    baseOffset <- getNextOffset conn topicId partition
                    forM_ (zip messages [0..]) (\((_, message), idx) ->
                      insertMessage conn topicId partition (baseOffset + idx) message)
                    incrementNextOffset conn topicId partition $ Prelude.length messages
                    return baseOffset
    return (NoError, baseOffset))

respondToRequest :: Pool.Pool PG.Connection -> KafkaRequest -> IO KafkaResponse
respondToRequest pool (ProduceRequest (ApiVersion 1) acks timeout ts) = do
  topicResponses <- forM ts (\(topic, parts) -> do
                      partResponses <- forM parts (\(partitionId, messageSet) -> do
                        (err, offset) <- writeMessageSet pool topic partitionId messageSet
                        return (partitionId, err, offset))
                      return (topic, partResponses) )

  let throttleTimeMs = 0 :: Int32
  return $ ProduceResponseV0 topicResponses throttleTimeMs
respondToRequest _ (TopicMetadataRequest (ApiVersion 0) ts) =
  let
    brokers = [Broker 42 "localhost" 9092]
    topicMetadata = [ TopicMetadata NoError "topic-a" [ PartitionMetadata NoError 0 42 [42] [42] ] ]
  in
    pure $ TopicMetadataResponseV0 brokers topicMetadata

handleRequest :: Pool.Pool PG.Connection -> ByteString -> IO ByteString
handleRequest pool request =
  case parseOnly (kafkaRequest <* endOfInput) request of
    Left err -> pure . fromString $ err
    Right ((_, _, correlationId, _), req) -> do
      response <- respondToRequest pool req
      let
        putCorrelationId = putWord32be . fromIntegral $ correlationId
        putResponse = putByteString . writeResponse $ response
      pure . runPut $ putCorrelationId *> putResponse

