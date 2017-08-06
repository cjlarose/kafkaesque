module Main where

import Network.Socket (close, socket, setSocketOption, bind, listen, Socket,
                       SockAddr, accept, Family(AF_INET), SocketType(Stream),
                       SocketOption(ReuseAddr), SockAddr(SockAddrInet),
                       iNADDR_ANY)
import Network.Socket.ByteString (send)
import Data.ByteString.UTF8 (fromString)

runConn :: (Socket, SockAddr) -> IO ()
runConn (sock, _) = do
    send sock (fromString "Hello!\n")
    close sock

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
