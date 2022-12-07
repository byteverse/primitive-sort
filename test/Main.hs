{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -Wall -fno-warn-orphans #-}

import Test.HUnit.Base ((@?=))
import Test.QuickCheck as Q
import Test.SmallCheck.Series (Serial(..))
import Test.Tasty
import Test.Tasty.HUnit (testCase)
import Test.Tasty.QuickCheck as QC
import Test.Tasty.SmallCheck as SC

import Control.Applicative (liftA2)
import Control.Exception (Exception,toException)
import Control.Monad.ST (ST,runST)
import Data.Int
import Data.List
import Data.Primitive (ByteArray(..),PrimArray(..),Prim,Array)
import Data.Proxy (Proxy(..))
import Data.Word
import Type.Reflection (TypeRep,typeRep)

import qualified Data.Map as M
import qualified Data.Primitive as P
import qualified Data.Primitive.Sort
import qualified Data.Set as S
import qualified GHC.Exts as E
import qualified GHC.OldList as L
import qualified Test.QuickCheck.Property as QP

main :: IO ()
main = defaultMain $ testGroup "Sort"
  [ testGroup "Contiguous"
    [ tests (typeRep :: TypeRep Int8) (primArrayToByteArray . Data.Primitive.Sort.sort @Int8 . byteArrayToPrimArray)
    , tests (typeRep :: TypeRep Word) (primArrayToByteArray . Data.Primitive.Sort.sort @Word . byteArrayToPrimArray)
    , SC.testProperty "sortUnique == Set.toList . Set.fromList" $ \(list :: [Int]) ->
        let actual = E.toList (Data.Primitive.Sort.sortUnique (E.fromList list :: PrimArray Int))
            expected = S.toList (S.fromList list)
         in if actual == expected
              then Right "unused"
              else Left ("expected " ++ show expected ++ " but got " ++ show actual)
    , testCase "sortTagged" $
        Data.Primitive.Sort.sortTagged
          (E.fromList [2, 1, 0] :: PrimArray Int)
          (E.fromList [1, 1, 0] :: PrimArray Word8)
        @?=
        (E.fromList [0,1,2], E.fromList [0,1,1] :: PrimArray Word8)
    , testCase "sortUniqueTagged" $
        Data.Primitive.Sort.sortUniqueTagged
          (E.fromList [2, 1, 0] :: Array Int)
          (E.fromList [1, 1, 0] :: PrimArray Word8)
        @?=
        (E.fromList [0,1,2], E.fromList [0,1,1] :: PrimArray Word8)
    , SC.testProperty "sortUniqueTagged == Map.toList . Map.fromList" $ \(list :: [(Int,Word8)]) ->
        let keys = E.fromList (map fst list) :: PrimArray Int
            vals = E.fromList (map snd list) :: PrimArray Word8
            (actualKeys,actualVals) = Data.Primitive.Sort.sortUniqueTagged keys vals
            actual = zip (E.toList actualKeys) (E.toList actualVals)
            expected = M.toList (M.fromList list)
         in if actual == expected
              then Right "unused"
              else Left ("expected " ++ show expected ++ " but got " ++ show actual)
    ]
  , testGroup "Tagged"
    [ testsTagged (typeRep :: TypeRep Word16) (typeRep :: TypeRep Word32)
        (\k v -> pairPrimArrayToByteArray $ uncurry (Data.Primitive.Sort.sortTagged @Word16 @Word32) $ pairByteArrayToPrimArray (k,v))
    ]
  ]

primArrayToByteArray :: PrimArray a -> ByteArray
primArrayToByteArray (PrimArray x) = ByteArray x

byteArrayToPrimArray :: ByteArray -> PrimArray a
byteArrayToPrimArray (ByteArray x) = PrimArray x

pairPrimArrayToByteArray :: (PrimArray a, PrimArray b) -> (ByteArray,ByteArray)
pairPrimArrayToByteArray (PrimArray x,PrimArray y) = (ByteArray x,ByteArray y)

pairByteArrayToPrimArray :: (ByteArray,ByteArray) -> (PrimArray a, PrimArray b) 
pairByteArrayToPrimArray (ByteArray x,ByteArray y) = (PrimArray x,PrimArray y)

tests :: forall n. (Prim n, Ord n, Show n, Arbitrary n, Serial IO n) => TypeRep n -> (ByteArray -> ByteArray) -> TestTree
tests p sortArray = testGroup (show p) [properties (Proxy :: Proxy n) sortArray, unitTests (Proxy :: Proxy n) sortArray]

testsTagged :: forall n a. (Prim a, Ord a, Show a, Arbitrary a, Serial IO a, Prim n, Ord n, Show n, Arbitrary n, Serial IO n, Num n, Enum n)
  => TypeRep a -> TypeRep n -> (ByteArray -> ByteArray -> (ByteArray, ByteArray)) -> TestTree
testsTagged p n sortArray = testGroup (show p ++ " tagged with " ++ show n) 
  [ propertiesTagged (Proxy :: Proxy a) (Proxy :: Proxy n) sortArray
  ]

properties :: (Prim n, Ord n, Show n, Arbitrary n, Serial IO n) => Proxy n -> (ByteArray -> ByteArray) -> TestTree
properties p sortArray = testGroup "Properties"
  [ scProps p sortArray
  , qcProps p sortArray
  ]

propertiesTagged :: (Prim a, Ord a, Show a, Arbitrary a, Serial IO a, Prim n, Ord n, Show n, Arbitrary n, Serial IO n, Num n, Enum n)
  => Proxy a -> Proxy n -> (ByteArray -> ByteArray -> (ByteArray, ByteArray)) -> TestTree
propertiesTagged p n sortArray = testGroup "Properties"
  [ scPropsTagged p n sortArray
  , qcPropsTagged p n sortArray
  ]

scProps :: forall n. (Prim n, Ord n, Show n, Serial IO n) => Proxy n -> (ByteArray -> ByteArray) -> TestTree
scProps _ sortArray = testGroup "(checked by SmallCheck)"
  [ SC.testProperty "sort == sort . reverse" $ \list ->
      eqByteArray (sortArray (byteArrayFromList (list :: [n]))) (sortArray (byteArrayFromList (reverse list)))
  , SC.testProperty "sort == Data.List.sort" $ \list ->
      (==) (byteArrayToList (sortArray (byteArrayFromList (list :: [n])))) (Data.List.sort list)
  ]

scPropsTagged :: forall n a. (Prim a, Ord a, Show a, Serial IO a, Prim n, Ord n, Show n, Serial IO n, Num n, Enum n)
  => Proxy a -> Proxy n -> (ByteArray -> ByteArray -> (ByteArray,ByteArray)) -> TestTree
scPropsTagged _ _ sortArray = testGroup "(checked by SmallCheck)"
  [ SC.testProperty "sort == Data.List.sort" $ \list ->
      let taggedList = tagWithIndices list :: [Tag a n]
          actual = taggedByteArrayToList (uncurry sortArray (taggedByteArrayFromList (taggedList :: [Tag a n])))
          expected = Data.List.sort taggedList
       in if actual == expected
            then Right "unused"
            else Left ("expected " ++ show expected ++ " but got " ++ show actual)
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

qcPropsTagged :: forall n a. (Prim a, Arbitrary a, Show a, Ord a, Prim n, Arbitrary n, Show n, Ord n)
  => Proxy a -> Proxy n -> (ByteArray -> ByteArray -> (ByteArray,ByteArray)) -> TestTree
qcPropsTagged p n sortArray = testGroup "(checked by QuickCheck)"
  [ testGroup "sort == Data.List.sort"
    [ sizedQuickCheckCorrectTagged p n sortArray "small" 20 10 100
    , sizedQuickCheckCorrectTagged p n sortArray "medium" 5 10000 100000
    , sizedQuickCheckCorrectTagged p n sortArray "large" 2 100000 200000
    ]
  ]

sizedQuickCheckReverse :: forall n. (Arbitrary n, Prim n)
  => Proxy n -> (ByteArray -> ByteArray) -> String -> Int -> Int -> Int -> TestTree
sizedQuickCheckReverse _ sortArray szName countTests szMin szMax = 
  adjustOption (\_ -> QC.QuickCheckTests countTests) $
    QC.testProperty szName $ do
      sz <- Q.choose (szMin,szMax)
      list <- Q.vector sz
      return (eqByteArray (sortArray (byteArrayFromList (list :: [n]))) (sortArray (byteArrayFromList (reverse list))))

sizedQuickCheckCorrect :: forall n. (Arbitrary n, Prim n, Ord n, Show n)
  => Proxy n -> (ByteArray -> ByteArray) -> String -> Int -> Int -> Int -> TestTree
sizedQuickCheckCorrect _ sortArray szName countTests szMin szMax = 
  adjustOption (\_ -> QC.QuickCheckTests countTests) $
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

sizedQuickCheckCorrectTagged :: forall n a. (Arbitrary a, Prim a, Ord a, Show a, Arbitrary n, Prim n, Ord n, Show n)
  => Proxy a -> Proxy n -> (ByteArray -> ByteArray -> (ByteArray,ByteArray)) -> String -> Int -> Int -> Int -> TestTree
sizedQuickCheckCorrectTagged _ _ sortArray szName countTests szMin szMax = 
  adjustOption (\_ -> QC.QuickCheckTests countTests) $
    QC.testProperty szName $ do
      sz <- Q.choose (szMin,szMax)
      list <- Q.vector sz
      let actual = taggedByteArrayToList (uncurry sortArray (taggedByteArrayFromList (list :: [Tag a n])))
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

taggedByteArrayToList :: forall n a. (Prim a, Prim n) => (ByteArray, ByteArray) -> [Tag a n]
taggedByteArrayToList (arr,tags) = go 0 where
  !len = div (P.sizeofByteArray arr) (P.sizeOf (undefined :: a))
  go :: Int -> [Tag a n]
  go !ix = if ix < len
    then Tag (P.indexByteArray arr ix) (P.indexByteArray tags ix) : go (ix + 1)
    else []

byteArrayFromList :: Prim a => [a] -> ByteArray
byteArrayFromList xs = byteArrayFromListN (L.length xs) xs

taggedByteArrayFromList :: (Prim a, Prim n) => [Tag a n] -> (ByteArray,ByteArray)
taggedByteArrayFromList xs = taggedByteArrayFromListN (L.length xs) xs

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

taggedByteArrayFromListN :: forall n a. (Prim a, Prim n)
  => Int -> [Tag a n] -> (ByteArray,ByteArray)
taggedByteArrayFromListN len vs = runST run where
  run :: forall s. ST s (ByteArray,ByteArray)
  run = do
    arr <- P.newByteArray (len * P.sizeOf (undefined :: a))
    tags <- P.newByteArray (len * P.sizeOf (undefined :: n))
    let go :: [Tag a n] -> Int -> ST s ()
        go !xs !ix = case xs of
          [] -> return ()
          Tag a n : as -> do
            P.writeByteArray arr ix a
            P.writeByteArray tags ix n
            go as (ix + 1)
    go vs 0
    liftA2 (,) (P.unsafeFreezeByteArray arr) (P.unsafeFreezeByteArray tags)


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

data Tag a b = Tag a b
  deriving (Show)

instance Eq a => Eq (Tag a b) where
  Tag a1 _ == Tag a2 _ = a1 == a2

instance Ord a => Ord (Tag a b) where
  compare (Tag a1 _) (Tag a2 _) = compare a1 a2

instance (Serial m a, Serial m b) => Serial m (Tag a b) where
  series = fmap tagFromTuple series

instance (Arbitrary a, Arbitrary b) => Arbitrary (Tag a b) where
  arbitrary = liftA2 Tag arbitrary arbitrary

tagFromTuple :: (a,b) -> Tag a b
tagFromTuple (a,b) = Tag a b

tagWithIndices :: (Num n, Enum n) => [a] -> [Tag a n]
tagWithIndices xs = map tagFromTuple (zip xs [0,1..])



