module Kafkaesque.Message
  ( Message(..)
  , MessageSet
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Word (Word32, Word8)

data Message =
  Message Word32
          Word8
          Word8
          (Maybe ByteString)
          (Maybe ByteString)

type MessageSet = [(Int64, Message)]
