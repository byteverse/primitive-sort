{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Element.Word16 where

import Data.Word (Word16)
import Data.Primitive (ByteArray(..),MutableByteArray(..))
import Control.Monad.ST (ST)
import GHC.Int (Int(..))
import Control.Monad.Primitive (primitive_,primitive)
import GHC.Prim
import qualified Data.Primitive as P

type T = Word16

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
size = 2

indexByteArray :: ByteArray -> Int -> T
indexByteArray = P.indexByteArray

readByteArray :: MutableByteArray s -> Int -> ST s T
readByteArray = P.readByteArray

writeByteArray :: MutableByteArray s -> Int -> T -> ST s ()
writeByteArray = P.writeByteArray

copyByteArray ::
     MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> ByteArray -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyByteArray dst doff src soff n =
  P.copyByteArray dst (doff * 2) src (soff * 2) (n * 2)

copyMutableByteArray ::
     MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> MutableByteArray s -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyMutableByteArray dst doff src soff n =
  P.copyMutableByteArray dst (doff * 2) src (soff * 2) (n * 2)

sizeofByteArray :: ByteArray -> Int
sizeofByteArray arr = div (P.sizeofByteArray arr) 2

newByteArray :: Int -> ST s (MutableByteArray s)
newByteArray sz = P.newByteArray (sz * 2)

getSizeofMutableByteArray :: 
     MutableByteArray s -- ^ array
  -> ST s Int
getSizeofMutableByteArray (MutableByteArray arr#)
  = primitive (\s# -> 
      case getSizeofMutableByteArray# arr# s# of
        (# s'#, sz# #) -> (# s'#, I# (quotInt# sz# 2#) #)
    )


