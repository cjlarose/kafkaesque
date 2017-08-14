module Main where

import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Control.Concurrent (forkIO)
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString (send, recv)
import Data.ByteString.UTF8 (fromString)
import Data.ByteString (hGet, hPut, ByteString, length)
import System.IO (IOMode(ReadWriteMode), hClose)
import Control.Monad (forever)
import Kafkaesque.Request (KafkaRequest(..), ApiVersion(..), kafkaRequest)
import Kafkaesque.Response (Broker(..), TopicMetadata(..), PartitionMetadata(..), KafkaError(..), KafkaResponse(..), writeResponse)
import Data.Attoparsec.ByteString (parseOnly, endOfInput)
import Data.Serialize.Get (runGet, getWord32be)
import Data.Serialize.Put (runPut, putWord32be, putByteString)

respondToRequest :: KafkaRequest -> KafkaResponse
respondToRequest (ProduceRequest (ApiVersion 1) acks timeout ts) =
  let
    partitionResponse (partitionId, _) = (fromIntegral partitionId, NoError, 0 :: Int64)
    topicResponse (name, parts) = (name, map partitionResponse parts)
    topicResponses = map topicResponse ts
    throttleTimeMs = 0 :: Int32
  in
    ProduceResponseV0 topicResponses throttleTimeMs
respondToRequest (TopicMetadataRequest (ApiVersion 0) ts) =
  let
    brokers = [Broker 42 "localhost" 9092]
    topicMetadata = [ TopicMetadata NoError "topic-a" [ PartitionMetadata NoError 0 42 [42] [42] ] ]
  in
    TopicMetadataResponseV0 brokers topicMetadata

handleRequest :: ByteString -> ByteString
handleRequest request =
  case parseOnly (kafkaRequest <* endOfInput) request of
    Left err -> fromString "Oops"
    Right ((_, _, correlationId, _), req) ->
      let
        putCorrelationId = putWord32be . fromIntegral $ correlationId
        putResponse = putByteString . writeResponse . respondToRequest $ req
      in
        runPut $ putCorrelationId *> putResponse

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
        let response = handleRequest msg
        hPut handle . runPut . putWord32be . fromIntegral . Data.ByteString.length $ response
        hPut handle response

mainLoop :: Socket -> IO ()
mainLoop sock = do
  conn <- accept sock
  forkIO (runConn conn)
  mainLoop sock

main :: IO ()
main = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 9092 iNADDR_ANY)
  listen sock 2
  mainLoop sock
