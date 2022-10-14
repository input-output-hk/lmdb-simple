{-# LANGUAGE Rank2Types #-}

module Database.LMDB.Simple.Codec (
    -- * Types
    Codec (..)
    -- * Serialisation and deserialisation with explicit dictionaries
  , deserialise
  , deserialiseIncremental
  , serialise
  , serialiseIncremental
    -- * Marshalling and others
  , marshalIn
  , marshalOut
  , marshalOutBS
  , serialiseLBS
  , serialiseBS
  , peekVal
  , pokeVal
  ) where

import           Control.Exception             (throw)
import           Control.Monad                 ((>=>))
import           Control.Monad.ST

import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BS
import qualified Data.ByteString.Lazy          as LBS
import qualified Data.ByteString.Lazy.Internal as LBS
import           Data.ByteString.Unsafe        (unsafeUseAsCStringLen)

import           Foreign                       (Ptr, castPtr, peek, poke)

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import qualified Codec.CBOR.Read               as CBOR.Read
import qualified Codec.CBOR.Write              as CBOR.Write

import           Database.LMDB.Raw

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

-- | A @'Codec' a@ contains a CBOR encoder and a CBOR decoder for values of type
-- @a@.
data Codec a = Codec {
    encoder :: !(a -> Encoding)
  , decoder :: !(forall s. Decoder s a)
  }

{-------------------------------------------------------------------------------
  Serialisation and deserialisation with explicit dictionaries

  Note: Based on functions of the same name from the @serialise@ package, but
  changed to use @'Codec'@ instead of the @'Serialise'@ class to
  serialise/deserialise values:
  https://hackage.haskell.org/package/serialise-0.2.6.0/docs/src/Codec.Serialise.html
-------------------------------------------------------------------------------}

serialiseIncremental :: Codec a -> a -> BS.Builder
serialiseIncremental codec = CBOR.Write.toBuilder . encoder codec

deserialiseIncremental :: Codec a -> ST s (CBOR.Read.IDecode s a)
deserialiseIncremental codec = CBOR.Read.deserialiseIncremental (decoder codec)

serialise :: Codec a -> a -> LBS.ByteString
serialise codec = CBOR.Write.toLazyByteString . encoder codec

deserialise :: Codec a -> LBS.ByteString -> a
deserialise codec bs0 =
    runST (supplyAllInput bs0 =<< deserialiseIncremental codec)
  where
    supplyAllInput _bs (CBOR.Read.Done _ _ x) = return x
    supplyAllInput  bs (CBOR.Read.Partial k)  =
      case bs of
        LBS.Chunk chunk bs' -> k (Just chunk) >>= supplyAllInput bs'
        LBS.Empty           -> k Nothing      >>= supplyAllInput LBS.Empty
    supplyAllInput _ (CBOR.Read.Fail _ _ exn) = throw exn

{-------------------------------------------------------------------------------
  Marshalling and others
-------------------------------------------------------------------------------}

marshalIn :: Codec v -> MDB_val -> IO v
marshalIn codec (MDB_val len ptr) =
  deserialise codec . LBS.fromStrict <$>
    BS.packCStringLen (castPtr ptr, fromIntegral len)

marshalOut :: Codec v -> v -> (MDB_val -> IO a) -> IO a
marshalOut codec = marshalOutBS . serialiseBS codec

marshalOutBS :: BS.ByteString -> (MDB_val -> IO a) -> IO a
marshalOutBS bs f =
  unsafeUseAsCStringLen bs $ \(ptr, len) ->
  f $ MDB_val (fromIntegral len) (castPtr ptr)

serialiseLBS :: Codec v -> v -> LBS.ByteString
serialiseLBS = serialise

serialiseBS :: Codec v -> v -> BS.ByteString
serialiseBS codec = LBS.toStrict . serialiseLBS codec

peekVal :: Codec v -> Ptr MDB_val -> IO v
peekVal codec = peek >=> marshalIn codec

pokeVal :: Codec v -> Ptr MDB_val -> v -> IO ()
pokeVal codec ptr v = marshalOut codec v (poke ptr)
