module Kafkaesque.Serialize
  ( putInt16be
  , putInt32be
  , putInt64be
  , putKafkaString
  , putKafkaBytes
  , putKafkaNullabeBytes
  , putKafkaArray
  , putKafkaNullableArray
  , putKakfaError
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString (length)
import Data.ByteString.UTF8 (fromString)
import Data.Int (Int16, Int32, Int64)
import Data.Serialize.Put
       (Put, putByteString, putWord16be, putWord32be, putWord64be)
import Kafkaesque.KafkaError (KafkaError, kafkaErrorCode)

putInt16be :: Int16 -> Put
putInt16be = putWord16be . fromIntegral

putInt32be :: Int32 -> Put
putInt32be = putWord32be . fromIntegral

putInt64be :: Int64 -> Put
putInt64be = putWord64be . fromIntegral

putKafkaString :: String -> Put
putKafkaString s =
  (putInt16be . fromIntegral . length $ s) *> putByteString (fromString s)

putKafkaBytes :: ByteString -> Put
putKafkaBytes bs =
  (putInt32be . fromIntegral . Data.ByteString.length $ bs) *> putByteString bs

putKafkaNullabeBytes :: Maybe ByteString -> Put
putKafkaNullabeBytes = maybe (putInt32be (-1)) putKafkaBytes

putKafkaArray :: (a -> Put) -> [a] -> Put
putKafkaArray putter xs =
  (putInt32be . fromIntegral . length $ xs) *> mapM_ putter xs

putKafkaNullableArray :: (a -> Put) -> Maybe [a] -> Put
putKafkaNullableArray _ Nothing = putInt32be (-1)
putKafkaNullableArray putter (Just xs) = putKafkaArray putter xs

putKakfaError :: KafkaError -> Put
putKakfaError = putInt16be . kafkaErrorCode
