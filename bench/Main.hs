{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Criterion.Main 
import Type.Reflection (typeRep,TypeRep)
import Data.Primitive (ByteArray,Prim)
import Control.Monad.ST (ST,runST)
import Data.Int
import Data.Word
import System.Random (mkStdGen,randoms,Random)
import qualified GHC.OldList as L
import qualified Sort.Merge.Int8
import qualified Sort.Merge.Word16
import qualified Sort.Merge.Word
import qualified Sort.Merge
import qualified Data.Primitive as P

main :: IO ()
main = defaultMain
  [ bgroup "backpack"
    [ benchType (typeRep :: TypeRep Int8) Sort.Merge.Int8.sort
    , benchType (typeRep :: TypeRep Word) Sort.Merge.Word.sort
    ]
  , bgroup "specialize"
    [ benchType (typeRep :: TypeRep Int8) Sort.Merge.sortInt8
    , benchType (typeRep :: TypeRep Word) Sort.Merge.sortWord
    ]
  ]

data Size = Small | Medium | Large
  deriving (Enum,Bounded)

data Arrangement = Unsorted | Presorted | Reversed
  deriving (Enum,Bounded)

allSizes :: [Size]
allSizes = [minBound..maxBound]

allArrangements :: [Arrangement]
allArrangements = [minBound..maxBound]

showSize :: Size -> String
showSize x = case x of
  Small -> "small"
  Medium -> "medium"
  Large -> "large"

numSize :: Size -> Int
numSize x = case x of
  Small -> 10000
  Medium -> 100000
  Large -> 1000000

showArrangement :: Arrangement -> String
showArrangement x = case x of
  Unsorted -> "unsorted"
  Presorted -> "presorted"
  Reversed -> "reversed"

buildArrangement :: (Prim a, Num a, Random a, Enum a, Bounded a)
  => Arrangement -> TypeRep a -> Int -> ByteArray
buildArrangement x = case x of
  Unsorted -> unsorted
  Presorted -> presorted
  Reversed -> reversed

benchType :: (Prim a, Num a, Random a, Enum a, Bounded a)
  => TypeRep a -> (ByteArray -> ByteArray) -> Benchmark
benchType rep sort = bgroup
  (show rep)
  (map (\arrange -> benchArrangement rep arrange sort) allArrangements)

benchArrangement :: (Prim a, Num a, Random a, Enum a, Bounded a)
  => TypeRep a -> Arrangement -> (ByteArray -> ByteArray) -> Benchmark
benchArrangement rep arrange sort = bgroup
  (showArrangement arrange)
  (map (\sz -> let arr = buildArrangement arrange rep (numSize sz) in benchSize arr sz sort) allSizes)

benchSize :: ByteArray -> Size -> (ByteArray -> ByteArray) -> Benchmark
benchSize arr sz sort =
  bench (showSize sz) (whnf sort arr)

unsorted :: forall a. (Prim a, Random a) => TypeRep a -> Int -> ByteArray
unsorted typ n = byteArrayFromList
  (L.take n (randoms (mkStdGen 42) :: [a]))

presorted :: forall a. (Prim a, Num a, Enum a, Bounded a) => TypeRep a -> Int -> ByteArray
presorted typ n = byteArrayFromList
  (L.take n (iterate (+1) (minBound :: a)))

reversed :: forall a. (Prim a, Num a, Enum a, Bounded a) => TypeRep a -> Int -> ByteArray
reversed typ n = byteArrayFromList
  (L.take n (iterate (subtract 1) (maxBound :: a)))

byteArrayFromList :: Prim a => [a] -> ByteArray
byteArrayFromList xs = byteArrayFromListN (L.length xs) xs

byteArrayFromListN :: forall a. Prim a => Int -> [a] -> ByteArray
byteArrayFromListN len vs = runST run where
  run :: forall s. ST s ByteArray
  run = do
    arr <- P.newByteArray (len * P.sizeOf (undefined :: a))
    let go :: [a] -> Int -> ST s ()
        go !xs !ix = case xs of
          [] -> return ()
          a : as -> do
            P.writeByteArray arr ix a
            go as (ix + 1)
    go vs 0
    P.unsafeFreezeByteArray arr

