module Kafkaesque.Message
  ( Message(..)
  , MessageSet
  , messageParser
  , putMessage
  , messageV0
  ) where

import Data.Attoparsec.Binary (anyWord32be)
import Data.Attoparsec.ByteString (Parser, anyWord8)
import Data.ByteString (ByteString)
import Data.Digest.CRC32 (crc32)
import Data.Int (Int64)
import Data.Serialize.Put (Put, putWord32be, putWord8, runPut)
import Data.Word (Word32, Word8)
import Kafkaesque.Parsers (kafkaNullabeBytes)
import Kafkaesque.Serialize (putKafkaNullabeBytes)

data Message =
  Message Word32
          Word8
          Word8
          (Maybe ByteString)
          (Maybe ByteString)

type MessageSet = [(Int64, Message)]

putMessageContents ::
     Word8 -> Word8 -> Maybe ByteString -> Maybe ByteString -> Put
putMessageContents magicByte attrs k v =
  putWord8 magicByte *> putWord8 attrs *> putKafkaNullabeBytes k *>
  putKafkaNullabeBytes v

putMessage :: Message -> Put
putMessage (Message digest magicByte attrs k v) =
  putWord32be digest *> putMessageContents magicByte attrs k v

messageParser :: Parser Message
messageParser =
  Message <$> anyWord32be <*> anyWord8 <*> anyWord8 <*> kafkaNullabeBytes <*>
  kafkaNullabeBytes

messageV0 :: Maybe ByteString -> Maybe ByteString -> Message
messageV0 k v =
  let magicByte = 0
      attrs = 0
      messageBody = runPut $ putMessageContents magicByte attrs k v
      digest = crc32 messageBody
  in Message digest 0 0 k v
