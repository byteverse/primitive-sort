{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Element.Int8 where

import Data.Int (Int8)
import Data.Primitive (ByteArray(..),MutableByteArray(..))
import Control.Monad.ST (ST)
import Control.Monad.Primitive (primitive_,primitive)
import GHC.Int (Int(..))
import GHC.Prim
import qualified Data.Primitive as P

type T = Int8

gt :: T -> T -> Bool
gt = (>)
lt :: T -> T -> Bool
lt = (<)
gte :: T -> T -> Bool
gte = (>=)
lte :: T -> T -> Bool
lte = (<=)
eq :: T -> T -> Bool
eq = (==)

size :: Int
size = 1

indexByteArray :: ByteArray -> Int -> T
indexByteArray = P.indexByteArray

readByteArray :: MutableByteArray s -> Int -> ST s T
readByteArray = P.readByteArray

writeByteArray :: MutableByteArray s -> Int -> T -> ST s ()
writeByteArray = P.writeByteArray

newByteArray :: Int -> ST s (MutableByteArray s)
newByteArray = P.newByteArray

copyByteArray ::
     MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> ByteArray -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyByteArray (MutableByteArray dst#) (I# doff#) (ByteArray src#) (I# soff#) (I# n#)
  = primitive_ (copyByteArray#
      src# 
      soff#
      dst#
      doff#
      n#
    )

copyMutableByteArray ::
     MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> MutableByteArray s -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyMutableByteArray = P.copyMutableByteArray

getSizeofMutableByteArray :: 
     MutableByteArray s -- ^ array
  -> ST s Int
getSizeofMutableByteArray (MutableByteArray arr#)
  = primitive (\s# -> 
      case getSizeofMutableByteArray# arr# s# of
        (# s'#, sz# #) -> (# s'#, I# sz# #)
    )

sizeofByteArray :: ByteArray -> Int
sizeofByteArray = P.sizeofByteArray
