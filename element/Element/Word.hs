{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Element.Word where

import Data.Word (Word)
import Data.Primitive (ByteArray(..),MutableByteArray(..))
import Control.Monad.ST (ST)
import GHC.Int (Int(..))
import Control.Monad.Primitive (primitive_,primitive)
import GHC.Prim
import qualified Data.Primitive as P

type T = Word

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
size = P.sizeOf (undefined :: T)

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
  P.copyByteArray dst (doff * size) src (soff * size) (n * size)

copyMutableByteArray ::
     MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> MutableByteArray s -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyMutableByteArray dst doff src soff n =
  P.copyMutableByteArray dst (doff * size) src (soff * size) (n * size)

sizeofByteArray :: ByteArray -> Int
sizeofByteArray arr = div (P.sizeofByteArray arr) size

newByteArray :: Int -> ST s (MutableByteArray s)
newByteArray sz = P.newByteArray (sz * size)

getSizeofMutableByteArray :: 
     MutableByteArray s -- ^ array
  -> ST s Int
getSizeofMutableByteArray (MutableByteArray arr#)
  = primitive (\s# -> 
      case getSizeofMutableByteArray# arr# s# of
        (# s'#, sz# #) -> (# s'#, I# (quotInt# sz# (P.sizeOf# (undefined :: T))) #)
    )


