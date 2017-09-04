module Kafkaesque.Message
  ( Message(..)
  , MessageSet
  , putMessage
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Serialize.Put (Put, putWord32be, putWord8, runPut)
import Data.Word (Word32, Word8)
import Kafkaesque.Response (putKafkaNullabeBytes)

data Message =
  Message Word32
          Word8
          Word8
          (Maybe ByteString)
          (Maybe ByteString)

type MessageSet = [(Int64, Message)]

putMessage :: Message -> Put
putMessage (Message crc32 magicByte attrs k v) =
  putWord32be crc32 *> putWord8 magicByte *> putWord8 attrs *>
  putKafkaNullabeBytes k *>
  putKafkaNullabeBytes v
