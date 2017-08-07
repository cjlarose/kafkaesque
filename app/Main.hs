module Main where

import Network.Socket hiding (send, recv)
import Network.Socket.ByteString (send, recv)
import Data.ByteString.UTF8 (fromString)
import Data.ByteString (hGet, hPut)
import System.IO (IOMode(ReadWriteMode), hClose)
import Data.Binary.Strict.Get (runGet, getWord32be)
import Data.Int (Int32)

runConn :: (Socket, SockAddr) -> IO ()
runConn (sock, _) = do
    handle <- socketToHandle sock ReadWriteMode
    len <- hGet handle 4
    let (res, _) = runGet getWord32be len
    case res of
      Left err -> return ()
      Right lenAsWord -> hPut handle . fromString . show $ (fromIntegral lenAsWord :: Int32)
    hClose handle

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
