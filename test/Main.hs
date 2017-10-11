{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit
import Test.QuickCheck as Q
import qualified Test.QuickCheck.Property as QP
import Type.Reflection (TypeRep,typeRep)

import Data.List
import Data.Ord
import Data.Word
import Data.Int
import Data.Primitive (ByteArray,Prim)
import Data.Proxy (Proxy(..))
import Control.Monad.ST (ST,runST)
import Test.SmallCheck.Series (Serial(..),Series)
import Control.Exception (Exception,toException)

import qualified GHC.OldList as L
import qualified Data.Primitive as P
import qualified Sort.Merge.Int8
import qualified Sort.Merge.Word16
import qualified Sort.Merge.Word

main :: IO ()
main = defaultMain $ testGroup "Merge Sort"
  [ tests (typeRep :: TypeRep Int8) Sort.Merge.Int8.sort
  , tests (typeRep :: TypeRep Word16) Sort.Merge.Word16.sort
  , tests (typeRep :: TypeRep Word) Sort.Merge.Word.sort
  ]

tests :: forall n. (Prim n, Ord n, Show n, Arbitrary n, Serial IO n) => TypeRep n -> (ByteArray -> ByteArray) -> TestTree
tests p sortArray = testGroup (show p) [properties (Proxy :: Proxy n) sortArray, unitTests (Proxy :: Proxy n) sortArray]

properties :: (Prim n, Ord n, Show n, Arbitrary n, Serial IO n) => Proxy n -> (ByteArray -> ByteArray) -> TestTree
properties p sortArray = testGroup "Properties" [scProps p sortArray, qcProps p sortArray]

scProps :: forall n. (Prim n, Ord n, Show n, Serial IO n) => Proxy n -> (ByteArray -> ByteArray) -> TestTree
scProps _ sortArray = testGroup "(checked by SmallCheck)"
  [ SC.testProperty "sort == sort . reverse" $ \list ->
      eqByteArray (sortArray (byteArrayFromList (list :: [n]))) (sortArray (byteArrayFromList (reverse list)))
  , SC.testProperty "sort == Data.List.sort" $ \list ->
      (==) (byteArrayToList (sortArray (byteArrayFromList (list :: [n])))) (Data.List.sort list)
  ]

qcProps :: forall n. (Prim n, Arbitrary n, Show n, Ord n) => Proxy n -> (ByteArray -> ByteArray) -> TestTree
qcProps p sortArray = testGroup "(checked by QuickCheck)"
  [ testGroup "sort == sort . reverse"
    [ sizedQuickCheckReverse p sortArray "small" 20 10 100
    , sizedQuickCheckReverse p sortArray "medium" 5 10000 100000
    , sizedQuickCheckReverse p sortArray "large" 2 100000 200000
    ]
  , testGroup "sort == Data.List.sort"
    [ sizedQuickCheckCorrect p sortArray "small" 20 10 100
    , sizedQuickCheckCorrect p sortArray "medium" 5 10000 100000
    , sizedQuickCheckCorrect p sortArray "large" 2 100000 200000
    ]
  ]

sizedQuickCheckReverse :: forall n. (Arbitrary n, Prim n)
  => Proxy n -> (ByteArray -> ByteArray) -> String -> Int -> Int -> Int -> TestTree
sizedQuickCheckReverse _ sortArray szName numTests szMin szMax = 
  adjustOption (\_ -> QC.QuickCheckTests numTests) $
    QC.testProperty szName $ do
      sz <- Q.choose (szMin,szMax)
      list <- Q.vector sz
      return (eqByteArray (sortArray (byteArrayFromList (list :: [n]))) (sortArray (byteArrayFromList (reverse list))))

sizedQuickCheckCorrect :: forall n. (Arbitrary n, Prim n, Ord n, Show n)
  => Proxy n -> (ByteArray -> ByteArray) -> String -> Int -> Int -> Int -> TestTree
sizedQuickCheckCorrect _ sortArray szName numTests szMin szMax = 
  adjustOption (\_ -> QC.QuickCheckTests numTests) $
    QC.testProperty szName $ do
      sz <- Q.choose (szMin,szMax)
      list <- Q.vector sz
      let actual = byteArrayToList (sortArray (byteArrayFromList (list :: [n])))
          expected = Data.List.sort list
      return $ if actual == expected
        then property QP.succeeded
        else if sz < 100
          then property (QP.exception ("expected " ++ show expected ++ " but got " ++ show actual) (toException MyException))
          else property QP.failed

data MyException = MyException
  deriving (Show,Eq)
instance Exception MyException

unitTests :: forall n. Prim n => Proxy n -> (ByteArray -> ByteArray) -> TestTree
unitTests _ _ = testGroup "Unit Tests"
  [ -- testCase "List comparison (different length)" $
    --   [1, 2, 3] `compare` [1,2] @?= GT
  ]


byteArrayToList :: forall a. Prim a => ByteArray -> [a]
byteArrayToList arr = go 0 where
  !len = div (P.sizeofByteArray arr) (P.sizeOf (undefined :: a))
  go :: Int -> [a]
  go !ix = if ix < len
    then P.indexByteArray arr ix : go (ix + 1)
    else []

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

eqByteArray :: ByteArray -> ByteArray -> Bool
eqByteArray paA paB =
  let !sizA = P.sizeofByteArray paA
      !sizB = P.sizeofByteArray paB
      go !ix = if ix < sizA
        then if P.indexByteArray paA ix == (P.indexByteArray paB ix :: Word8)
          then go (ix + 1)
          else False
        else True
  in if sizA == sizB
       then go 0
       else False

instance Monad m => Serial m Int8 where
  series = fmap fromIntegral (series :: Series m Int)

instance Monad m => Serial m Word16 where
  series = fmap fromIntegral (series :: Series m Int)

instance Monad m => Serial m Word where
  series = fmap fromIntegral (series :: Series m Int)

