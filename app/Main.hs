{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Int (Int32, Int64)
import Control.Concurrent (forkIO)
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString (send, recv)
import Data.ByteString (hGet, hPut, length)
import System.IO (IOMode(ReadWriteMode), hClose)
import Control.Monad (forever, forM, forM_)
import Data.Serialize.Get (runGet, getWord32be)
import Data.Serialize.Put (runPut, putWord32be)
import qualified Database.PostgreSQL.Simple as PG
import qualified Data.Pool as Pool
import RequestHandlers (handleRequest)

runConn :: Pool.Pool PG.Connection -> (Socket, SockAddr) -> IO ()
runConn pool (sock, _) = do
  handle <- socketToHandle sock ReadWriteMode
  forever $ do
    len <- hGet handle 4
    let res = runGet getWord32be len
    case res of
      Left err -> return ()
      Right lenAsWord -> do
        let msgLen = fromIntegral lenAsWord :: Int32
        msg <- hGet handle . fromIntegral $ msgLen
        response <- handleRequest pool msg
        hPut handle . runPut . putWord32be . fromIntegral . Data.ByteString.length $ response
        hPut handle response

mainLoop :: Pool.Pool PG.Connection -> Socket -> IO ()
mainLoop pool sock = do
  conn <- accept sock
  forkIO (runConn pool conn)
  mainLoop pool sock

createTables :: PG.Connection -> IO ()
createTables conn = do
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS topics \
                   \ ( id SERIAL PRIMARY KEY \
                   \ , name text NOT NULL UNIQUE )"
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS partitions \
                   \ ( topic_id int NOT NULL REFERENCES topics (id) \
                   \ , partition_id int NOT NULL \
                   \ , next_offset bigint NOT NULL \
                   \ , PRIMARY KEY (topic_id, partition_id) )"
  PG.execute_ conn "CREATE TABLE IF NOT EXISTS records \
                   \ ( topic_id int NOT NULL \
                   \ , partition_id int NOT NULL \
                   \ , record bytea NOT NULL \
                   \ , base_offset bigint NOT NULL \
                   \ , FOREIGN KEY (topic_id, partition_id) REFERENCES partitions )"

  let initialTopics = [("topic-a", 2), ("topic-b", 4)] :: [(String, Int)]
  forM_ initialTopics (\(topic, partitionCount) -> do
    PG.execute conn "INSERT INTO topics (name) VALUES (?) ON CONFLICT DO NOTHING" $ PG.Only topic
    [PG.Only topicId] <- PG.query conn "SELECT id FROM topics WHERE name = ?" (PG.Only topic) :: IO [PG.Only Int32]
    forM_ [0..(partitionCount - 1)] (\partitionId ->
      PG.execute conn "INSERT INTO partitions (topic_id, partition_id, next_offset) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (topicId, partitionId, 0 :: Int64)))

main :: IO ()
main = do
  let createConn = PG.connect PG.defaultConnectInfo { PG.connectDatabase = "kafkaesque", PG.connectUser = "kafkaesque" }
  pool <- Pool.createPool createConn PG.close 1 10 8
  Pool.withResource pool createTables
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 9092 iNADDR_ANY)
  listen sock 2
  mainLoop pool sock
