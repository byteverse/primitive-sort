{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeApplications #-}

import Gauge.Main 
import Type.Reflection (typeRep,TypeRep)
import Data.Primitive (ByteArray(..),PrimArray(..),Prim)
import Control.Monad.ST (ST,runST)
import Data.Int
import Data.Word
import System.Random (mkStdGen,randoms,Random)
import GHC.Prim (proxy#, Proxy#)
import qualified GHC.OldList as L
import qualified Sort.Merge.Int8
import qualified Sort.Merge.Word16
import qualified Sort.Merge.Word
import qualified Sort.Merge
import qualified Data.Primitive as P
import qualified Data.Primitive.Sort
import qualified GHC.Exts as E

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
  , bgroup "contiguous"
    [ benchType (typeRep :: TypeRep Int8) (primArrayToByteArray . Data.Primitive.Sort.sort @PrimArray @Int8 . byteArrayToPrimArray)
    , benchType (typeRep :: TypeRep Word) (primArrayToByteArray . Data.Primitive.Sort.sort @PrimArray @Word . byteArrayToPrimArray)
    -- , benchType (typeRep :: TypeRep Int) sortInt
    -- , benchType (typeRep :: TypeRep Word32) (Sort.Merge.sort (proxy# :: Proxy# Word32))
    ]
  , bgroup "tagged-unique"
    [ bench "mini" (whnf (\(k,v) -> evalPair (Data.Primitive.Sort.sortUniqueTagged k v)) (sizedInts Mini, sizedInts Mini))
    , bench "tiny" (whnf (\(k,v) -> evalPair (Data.Primitive.Sort.sortUniqueTagged k v)) (sizedInts Tiny, sizedInts Tiny))
    , bench "small" (whnf (\(k,v) -> evalPair (Data.Primitive.Sort.sortUniqueTagged k v)) (sizedInts Small, sizedInts Small))
    ]
  ]

evalPair :: (PrimArray a, PrimArray b) -> ()
evalPair (!_,!_) = ()

primArrayToByteArray :: PrimArray a -> ByteArray
primArrayToByteArray (PrimArray x) = ByteArray x

byteArrayToPrimArray :: ByteArray -> PrimArray a
byteArrayToPrimArray (ByteArray x) = PrimArray x

data Size = Mini | Tiny | Small | Medium | Large | Gigantic
  deriving (Enum,Bounded)

data Arrangement = Unsorted | Presorted | Reversed
  deriving (Enum,Bounded)

allSizes :: [Size]
allSizes = [minBound..maxBound]

allArrangements :: [Arrangement]
allArrangements = [minBound..maxBound]

showSize :: Size -> String
showSize x = case x of
  Mini -> "mini"
  Tiny -> "tiny"
  Small -> "small"
  Medium -> "medium"
  Large -> "large"
  Gigantic -> "gigantic"

numSize :: Size -> Int
numSize x = case x of
  Mini -> 10
  Tiny -> 100
  Small -> 1000
  Medium -> 10000
  Large -> 100000
  Gigantic -> 1000000

sizedInts :: Size -> PrimArray Int
sizedInts x = case x of
  Mini -> intsMini
  Tiny -> intsTiny
  Small -> intsSmall
  Medium -> intsMedium
  Large -> intsLarge
  Gigantic -> intsGigantic

intsMini,intsTiny,intsSmall,intsMedium,intsLarge,intsGigantic :: PrimArray Int
intsMini = E.fromList (L.take 10 (randoms (mkStdGen 23) :: [Int]))
intsTiny = E.fromList (L.take 100 (randoms (mkStdGen 87) :: [Int]))
intsSmall = E.fromList (L.take 1000 (randoms (mkStdGen 19) :: [Int]))
intsMedium = E.fromList (L.take 10000 (randoms (mkStdGen 47) :: [Int]))
intsLarge = E.fromList (L.take 100000 (randoms (mkStdGen 53) :: [Int]))
intsGigantic = E.fromList (L.take 1000000 (randoms (mkStdGen 12) :: [Int]))

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

reversed :: forall a. (Prim a, Num a, Enum a, Bounded a)
  => TypeRep a -> Int -> ByteArray
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

