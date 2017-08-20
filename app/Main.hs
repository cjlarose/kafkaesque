{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Control.Concurrent (forkIO)
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString (send, recv)
import Data.ByteString.UTF8 (fromString)
import Data.ByteString (hGet, hPut, ByteString, length)
import System.IO (IOMode(ReadWriteMode), hClose)
import Control.Monad (forever, forM, forM_)
import Kafkaesque.Request (KafkaRequest(..), ApiVersion(..), MessageSet, kafkaRequest)
import Kafkaesque.Response (Broker(..), TopicMetadata(..), PartitionMetadata(..), KafkaError(..), KafkaResponse(..), writeResponse)
import Data.Attoparsec.ByteString (parseOnly, endOfInput)
import Data.Serialize.Get (runGet, getWord32be)
import Data.Serialize.Put (runPut, putWord32be, putByteString)
import Database.PostgreSQL.Simple as PG (connect, defaultConnectInfo, connectDatabase, connectUser, execute, execute_)

writeMessageBatch :: String -> Int32 -> MessageSet -> IO (KafkaError, Int64)
writeMessageBatch topic partition messages = return (NoError, 0 :: Int64)

respondToRequest :: KafkaRequest -> IO KafkaResponse
respondToRequest (ProduceRequest (ApiVersion 1) acks timeout ts) = do
  topicResponses <- forM ts (\(topic, parts) -> do
                      partResponses <- forM parts (\(partitionId, messageSet) -> do
                        (err, offset) <- writeMessageBatch topic partitionId messageSet
                        return (partitionId, err, offset))
                      return (topic, partResponses) )

  let throttleTimeMs = 0 :: Int32
  return $ ProduceResponseV0 topicResponses throttleTimeMs
respondToRequest (TopicMetadataRequest (ApiVersion 0) ts) =
  let
    brokers = [Broker 42 "localhost" 9092]
    topicMetadata = [ TopicMetadata NoError "topic-a" [ PartitionMetadata NoError 0 42 [42] [42] ] ]
  in
    pure $ TopicMetadataResponseV0 brokers topicMetadata

handleRequest :: ByteString -> IO ByteString
handleRequest request =
  case parseOnly (kafkaRequest <* endOfInput) request of
    Left err -> pure . fromString $ "Oops"
    Right ((_, _, correlationId, _), req) -> do
      response <- respondToRequest req
      let
        putCorrelationId = putWord32be . fromIntegral $ correlationId
        putResponse = putByteString . writeResponse $ response
      pure . runPut $ putCorrelationId *> putResponse

runConn :: (Socket, SockAddr) -> IO ()
runConn (sock, _) = do
  handle <- socketToHandle sock ReadWriteMode
  forever $ do
    len <- hGet handle 4
    let res = runGet getWord32be len
    case res of
      Left err -> return ()
      Right lenAsWord -> do
        let msgLen = fromIntegral lenAsWord :: Int32
        msg <- hGet handle . fromIntegral $ msgLen
        response <- handleRequest msg
        hPut handle . runPut . putWord32be . fromIntegral . Data.ByteString.length $ response
        hPut handle response

mainLoop :: Socket -> IO ()
mainLoop sock = do
  conn <- accept sock
  forkIO (runConn conn)
  mainLoop sock

createTables :: IO ()
createTables = do
  conn <- PG.connect PG.defaultConnectInfo { PG.connectDatabase = "kafkaesque", PG.connectUser = "kafkaesque" }
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS topics \
                   \ ( id SERIAL PRIMARY KEY \
                   \ , name text NOT NULL UNIQUE \
                   \ , partition_count int NOT NULL )"
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS next_offsets \
                   \ ( topic_id int NOT NULL REFERENCES topics (id) \
                   \ , partition int NOT NULL \
                   \ , next_offset bigint NOT NULL \
                   \ , PRIMARY KEY (topic_id, partition) )"
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS records \
                   \ ( topic_id int NOT NULL REFERENCES topics (id) \
                   \ , partition int NOT NULL \
                   \ , record bytea NOT NULL \
                   \ , base_offset bigint NOT NULL )"

  let initialTopics = [("topic-a", 2), ("topic-b", 4)] :: [(String, Int)]
  forM_ initialTopics $ PG.execute conn "INSERT INTO topics (name, partition_count) VALUES (?, ?) ON CONFLICT DO NOTHING"
  return ()

main :: IO ()
main = do
  createTables
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 9092 iNADDR_ANY)
  listen sock 2
  mainLoop sock
