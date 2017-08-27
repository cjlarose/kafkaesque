{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Main where

import Control.Concurrent (forkIO)
import Control.Exception (SomeException(..), handle)
import Control.Monad (forM, forM_)
import Control.Monad.Fix (fix)
import Data.ByteString (ByteString, hGet, hPut, length)
import Data.Int (Int32, Int64)
import qualified Data.Pool as Pool
import Data.Serialize.Get (getWord32be, runGet)
import Data.Serialize.Put (putWord32be, runPut)
import qualified Database.PostgreSQL.Simple as PG
import Database.PostgreSQL.Simple.SqlQQ (sql)
import Network.Socket hiding (recv, send)
import Network.Socket.ByteString (recv, send)
import RequestHandlers (handleRequest)
import System.IO (Handle, IOMode(ReadWriteMode), hClose)

getFramedMessage :: Handle -> IO (Maybe ByteString)
getFramedMessage hdl = do
  len <- hGet hdl 4
  let res = runGet getWord32be len
  case res of
    Left _ -> return Nothing
    Right lenAsWord -> do
      let msgLen = fromIntegral lenAsWord :: Int32
      msg <- hGet hdl . fromIntegral $ msgLen
      return . Just $ msg

runConn :: Pool.Pool PG.Connection -> (Socket, SockAddr) -> IO ()
runConn pool (sock, _) = do
  hdl <- socketToHandle sock ReadWriteMode
  handle (\(SomeException e) -> print e) $
    fix
      (\loop -> do
         content <- getFramedMessage hdl
         case content of
           Nothing -> return ()
           Just msg -> do
             response <- handleRequest pool msg
             case response of
               Left err -> print err
               Right responseBytes -> do
                 hPut hdl .
                   runPut . putWord32be . fromIntegral . Data.ByteString.length $
                   responseBytes
                 hPut hdl responseBytes
             loop)
  hClose hdl

mainLoop :: Pool.Pool PG.Connection -> Socket -> IO ()
mainLoop pool sock = do
  conn <- accept sock
  forkIO (runConn pool conn)
  mainLoop pool sock

createTables :: PG.Connection -> IO ()
createTables conn = do
  PG.execute_
    conn
    [sql| CREATE TABLE IF NOT EXISTS topics
          ( id SERIAL PRIMARY KEY
          , name text NOT NULL UNIQUE ) |]
  PG.execute_
    conn
    [sql| CREATE TABLE IF NOT EXISTS partitions
          ( topic_id int NOT NULL REFERENCES topics (id)
          , partition_id int NOT NULL
          , next_offset bigint NOT NULL
          , total_bytes bigint NOT NULL
          , PRIMARY KEY (topic_id, partition_id) ) |]
  PG.execute_
    conn
    [sql| CREATE TABLE IF NOT EXISTS records
          ( topic_id int NOT NULL
          , partition_id int NOT NULL
          , record bytea NOT NULL
          , log_offset bigint NOT NULL
          , byte_offset bigint NOT NULL
          , FOREIGN KEY (topic_id, partition_id) REFERENCES partitions ) |]
  PG.execute_
    conn
    [sql| CREATE INDEX ON records (topic_id, partition_id, log_offset) |]
  PG.execute_
    conn
    [sql| CREATE INDEX ON records (topic_id, partition_id, byte_offset) |]
  let initialTopics =
        [("topic-a", 2), ("topic-b", 4), ("test", 1)] :: [(String, Int)]
  forM_
    initialTopics
    (\(topic, partitionCount) -> do
       PG.execute
         conn
         "INSERT INTO topics (name) VALUES (?) ON CONFLICT DO NOTHING" $
         PG.Only topic
       [PG.Only topicId] <-
         PG.query conn "SELECT id FROM topics WHERE name = ?" (PG.Only topic) :: IO [PG.Only Int32]
       forM_
         [0 .. (partitionCount - 1)]
         (\partitionId ->
            PG.execute
              conn
              "INSERT INTO partitions (topic_id, partition_id, next_offset, total_bytes) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING"
              (topicId, partitionId, 0 :: Int64, 0 :: Int64)))

main :: IO ()
main = do
  let createConn =
        PG.connect
          PG.defaultConnectInfo
          {PG.connectDatabase = "kafkaesque", PG.connectUser = "kafkaesque"}
  pool <- Pool.createPool createConn PG.close 1 10 8
  Pool.withResource pool createTables
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 9092 iNADDR_ANY)
  listen sock 2
  mainLoop pool sock
