module Kafkaesque.Message (Message(..), MessageSet) where

import Data.Word (Word8, Word32)
import Data.Int (Int64)
import Data.ByteString (ByteString)

data Message = Message Word32 Word8 Word8 (Maybe ByteString) (Maybe ByteString)
type MessageSet = [(Int64, Message)]
