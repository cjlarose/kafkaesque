module Main where

import Data.Int (Int16, Int32)
import Network.Socket hiding (send, recv)
import Network.Socket.ByteString (send, recv)
import Data.ByteString.UTF8 (fromString)
import Data.ByteString (hGet, hPut, ByteString)
import System.IO (IOMode(ReadWriteMode), hClose)
import Data.Binary.Strict.Get (runGet, getWord32be)
import Data.Int (Int32)
import Control.Monad (forever)
import KafkaMessage (requestMessageHeader)
import Data.Attoparsec.ByteString (parseOnly, endOfInput)

handleRequest :: ByteString -> ByteString
handleRequest request =
  case (parseOnly (requestMessageHeader <* endOfInput) request) of
    Left err -> fromString "Oops"
    Right (apiKey, apiVersion, correlationId, clientId) -> fromString "success"

runConn :: (Socket, SockAddr) -> IO ()
runConn (sock, _) = do
    handle <- socketToHandle sock ReadWriteMode
    forever $ do
      len <- hGet handle 4
      let (res, _) = runGet getWord32be len
      case res of
        Left err -> return ()
        Right lenAsWord -> do
          let msgLen = fromIntegral lenAsWord :: Int32
          msg <- hGet handle . fromIntegral $ msgLen
          let response = handleRequest msg
          hPut handle $ response

mainLoop :: Socket -> IO ()
mainLoop sock = do
    conn <- accept sock
    runConn conn
    mainLoop sock

main :: IO ()
main = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 4242 iNADDR_ANY)
    listen sock 2
    mainLoop sock
