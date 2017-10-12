{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{-# OPTIONS_GHC -O2 -Wall #-}

module Sort.Merge
  ( -- * Sorting
    sortWord
  , sortWord8
  , sortWord16
  , sortWord32
  , sortWord64
  , sortInt
  , sortInt8
  , sortInt16
  , sortInt32
  , sortInt64
    -- * Tagged Sorting
  , sortWordWord
  , sortWord8Word
  , sortWord16Word32
  ) where

import Control.Monad.ST
import GHC.ST (ST(..))
import GHC.IO (IO(..))
import GHC.Int (Int(..))
import Data.Primitive (Prim,ByteArray(..),MutableByteArray(..),unsafeFreezeByteArray,writeByteArray,readByteArray)
import Data.Primitive.Types (sizeOf# ) 
import Control.Monad
import GHC.Prim
import Control.Concurrent (getNumCapabilities)
import Data.Int
import Data.Word
import qualified Data.Primitive as P
import qualified Data.Vector as V

sortWord :: ByteArray -> ByteArray
sortWord arr = sort (proxy# :: Proxy# Word) arr

sortWord8 :: ByteArray -> ByteArray
sortWord8 arr = sort (proxy# :: Proxy# Word8) arr

sortWord16 :: ByteArray -> ByteArray
sortWord16 arr = sort (proxy# :: Proxy# Word16) arr

sortWord32 :: ByteArray -> ByteArray
sortWord32 arr = sort (proxy# :: Proxy# Word32) arr

sortWord64 :: ByteArray -> ByteArray
sortWord64 arr = sort (proxy# :: Proxy# Word64) arr

sortInt :: ByteArray -> ByteArray
sortInt arr = sort (proxy# :: Proxy# Int) arr

sortInt8 :: ByteArray -> ByteArray
sortInt8 arr = sort (proxy# :: Proxy# Int8) arr

sortInt16 :: ByteArray -> ByteArray
sortInt16 arr = sort (proxy# :: Proxy# Int16) arr

sortInt32 :: ByteArray -> ByteArray
sortInt32 arr = sort (proxy# :: Proxy# Int32) arr

sortInt64 :: ByteArray -> ByteArray
sortInt64 arr = sort (proxy# :: Proxy# Int64) arr

-- The first type name is the element type. The second one is
-- the tag type.

sortWordWord :: ByteArray -> ByteArray -> (ByteArray,ByteArray)
sortWordWord arr tags = sortTagged (proxy# :: Proxy# Word) (proxy# :: Proxy# Word) arr tags

sortWord8Word:: ByteArray -> ByteArray -> (ByteArray,ByteArray)
sortWord8Word arr tags = sortTagged (proxy# :: Proxy# Word8) (proxy# :: Proxy# Word) arr tags

sortWord16Word32:: ByteArray -> ByteArray -> (ByteArray,ByteArray)
sortWord16Word32 arr tags = sortTagged (proxy# :: Proxy# Word16) (proxy# :: Proxy# Word32) arr tags


sort :: forall a. (Ord a, Prim a) => Proxy# a -> ByteArray -> ByteArray
sort !_ !src = runST $ do
  let len = sizeofByteArray (proxy# :: Proxy# a) src
  dst <- newByteArray (proxy# :: Proxy# a) len
  work <- newByteArray (proxy# :: Proxy# a) len
  copyByteArray (proxy# :: Proxy# a) work 0 src 0 len 
  copyByteArray (proxy# :: Proxy# a) dst 0 src 0 len
  caps <- unsafeEmbedIO getNumCapabilities
  let minElemsPerThread = 20000
      maxThreads = div len minElemsPerThread
      preThreads = min caps maxThreads
      threads = if preThreads == 1 then 1 else preThreads * 8
  -- I cannot understand why, but GHC's runtime does better
  -- when we let this schedule 8 times as many threads as
  -- we have capabilities. However, we only get this benefit
  -- when we actually have more than one capability.
  splitMergeParallel (proxy# :: Proxy# a) dst work threads 0 len
  unsafeFreezeByteArray dst

-- | Admits an additional array of tags. This array must be the
--   same length as the source array. The second element of the
--   result tuple is this tag array, sorted to match the sorted
--   element array.
sortTagged :: forall n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> ByteArray -- source
  -> ByteArray -- source tags
  -> (ByteArray, ByteArray)
sortTagged !_ !_ !src !srcTags = runST $ do
  let len = sizeofByteArray (proxy# :: Proxy# a) src
  dst <- newByteArray (proxy# :: Proxy# a) len
  work <- newByteArray (proxy# :: Proxy# a) len
  dstTags <- newByteArray (proxy# :: Proxy# n) len
  workTags <- newByteArray (proxy# :: Proxy# n) len
  copyByteArray (proxy# :: Proxy# a) work 0 src 0 len 
  copyByteArray (proxy# :: Proxy# a) dst 0 src 0 len
  copyByteArray (proxy# :: Proxy# n) workTags 0 srcTags 0 len 
  copyByteArray (proxy# :: Proxy# n) dstTags 0 srcTags 0 len
  caps <- unsafeEmbedIO getNumCapabilities
  let minElemsPerThread = 20000
      maxThreads = div len minElemsPerThread
      preThreads = min caps maxThreads
      threads = if preThreads == 1 then 1 else preThreads * 8
  splitMergeParallelTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) dst work dstTags workTags threads 0 len
  dstFrozen <- unsafeFreezeByteArray dst
  dstTagsFrozen <- unsafeFreezeByteArray dstTags
  return (dstFrozen,dstTagsFrozen)

unsafeEmbedIO :: IO a -> ST s a
unsafeEmbedIO (IO f) = ST (unsafeCoerce# f)

half :: Int -> Int
half x = div x 2

splitMergeParallel :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> Int -- spark limit, should be power of two
  -> Int -- start
  -> Int -- end
  -> ST s ()
splitMergeParallel !_ !arr !work !level !start !end = if level > 1
  then if end - start < threshold
    then insertionSortRange (proxy# :: Proxy# a) arr start end
    else do
      let !mid = div (end + start) 2
          !levelDown = half level
      tandem 
        (splitMergeParallel (proxy# :: Proxy# a) work arr levelDown start mid)
        (splitMergeParallel (proxy# :: Proxy# a) work arr levelDown mid end)
      mergeParallel (proxy# :: Proxy# a) work arr level start mid end
  else splitMerge (proxy# :: Proxy# a) arr work start end

splitMergeParallelTagged :: forall s n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> MutableByteArray s -- source and destination tags
  -> MutableByteArray s -- work array tags
  -> Int -- spark limit, should be power of two
  -> Int -- start
  -> Int -- end
  -> ST s ()
splitMergeParallelTagged !_ !_ !arr !work !arrTags !workTags !level !start !end = if level > 1
  then -- if end - start < threshold
    -- then insertionSortRange (proxy# :: Proxy# a) arr start end
    -- else 
    do
      let !mid = div (end + start) 2
          !levelDown = half level
      tandem 
        (splitMergeParallelTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags levelDown start mid)
        (splitMergeParallelTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags levelDown mid end)
      mergeParallelTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags level start mid end
  else splitMergeTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) arr work arrTags workTags start end

replicateIxed :: Monad m => Int -> (Int -> m a) -> m ()
replicateIxed !n f = go (n - 1) where
  go !ix = if ix >= 0
    then f ix >> go (ix - 1)
    else return ()

splitMergeTagged :: forall s n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> MutableByteArray s -- source and destination tags
  -> MutableByteArray s -- work array tags
  -> Int -- start
  -> Int -- end
  -> ST s ()
splitMergeTagged !_ !_ !arr !work !arrTags !workTags !start !end = if end - start < 2
  then return ()
  else do
    -- if end - start > threshold
      -- then do
        let !mid = div (end + start) 2
        splitMergeTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags start mid
        splitMergeTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags mid end
        mergeTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) work arr workTags arrTags start mid end
      -- else insertionSortRange (proxy# :: Proxy# a) arr start end

splitMerge :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> Int -- start
  -> Int -- end
  -> ST s ()
splitMerge !_ !arr !work !start !end = if end - start < 2
  then return ()
  else do
    if end - start > threshold
      then do
        let !mid = div (end + start) 2
        splitMerge (proxy# :: Proxy# a) work arr start mid
        splitMerge (proxy# :: Proxy# a) work arr mid end
        merge (proxy# :: Proxy# a) work arr start mid end
      else insertionSortRange (proxy# :: Proxy# a) arr start end

data Build
  = BuildStep
    {-# UNPACK #-} !Int -- previous A end
    {-# UNPACK #-} !Int -- previous B end
    {-# UNPACK #-} !Int -- How many chunks have we already iterated over
  | BuildBegin

data Indices = Indices
  {-# UNPACK #-} !Int -- start A
  {-# UNPACK #-} !Int -- end A
  {-# UNPACK #-} !Int -- start B
  {-# UNPACK #-} !Int -- end B

mergeParallel :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
mergeParallel !_ !src !dst !threads !start !mid !end = do
  v <- createIndicesVector (proxy# :: Proxy# a) src threads start mid end
  forConcurrently_ v $ \(Indices startA endA startB endB) -> do
    let !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguous (proxy# :: Proxy# a) src dst startA endA startB endB startDst

mergeParallelTagged :: forall s n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> MutableByteArray s -- source tags
  -> MutableByteArray s -- dest tags
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
mergeParallelTagged !_ !_ !src !dst !srcTags !dstTags !threads !start !mid !end = do
  v <- createIndicesVector (proxy# :: Proxy# a) src threads start mid end
  forConcurrently_ v $ \(Indices startA endA startB endB) -> do
    let !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguousTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) src dst srcTags dstTags startA endA startB endB startDst

createIndicesVector :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s (V.Vector Indices)
createIndicesVector !_ !src !threads !start !mid !end = V.unfoldrNM threads (\x -> case x of
    BuildBegin -> do
      !endElem <- readByteArray src (start + chunk) 
      !endA <- findIndexOfGtElem src (endElem :: a) start mid
      !endB <- findIndexOfGtElem src endElem mid end
      return (Just (Indices start endA mid endB,BuildStep endA endB 1))
    BuildStep prevEndA prevEndB ix ->
      if | prevEndA == mid && prevEndB == end -> return Nothing
         | prevEndA == mid ->
             return (Just (Indices mid mid prevEndB end,BuildStep mid end (ix + 1)))
         | prevEndB == end ->
             return (Just (Indices prevEndA mid end end,BuildStep mid end (ix + 1)))
         | otherwise -> if ix == threads - 1
             then return (Just (Indices prevEndA mid prevEndB end, BuildStep mid end (ix + 1)))
             else do
               -- We use the left half for this lookup. We could instead
               -- use both halves and take the median.
               !endElem <- readByteArray src (start + chunk * (ix + 1))
               !endA <- findIndexOfGtElem src (endElem :: a) prevEndA mid
               !endB <- findIndexOfGtElem src endElem prevEndB end
               return (Just (Indices prevEndA endA prevEndB endB, BuildStep endA endB (ix + 1)))
  ) BuildBegin
  where
  !chunk = div (end - start) threads

-- If the needle is bigger than everything in the slice
-- of the array, this returns the end index (which is out
-- of bounds). Callers of this function should be able
-- to handle that.
findIndexOfGtElem :: forall s a. (Ord a, Prim a) => MutableByteArray s -> a -> Int -> Int -> ST s Int
findIndexOfGtElem !v !needle !start !end = go start end
  where
  go :: Int -> Int -> ST s Int
  go !lo !hi = if lo < hi
    then do
      let !mid = lo + half (hi - lo)
      !val <- readByteArray v mid
      if | val == needle -> gallopToGtIndex v needle (mid + 1) hi
         | val < needle -> go (mid + 1) hi
         | otherwise -> go lo mid
    else return lo

-- | TODO: should probably turn this into a real galloping search
gallopToGtIndex :: forall s a. (Ord a, Prim a) => MutableByteArray s -> a -> Int -> Int -> ST s Int
gallopToGtIndex !v !val !start !end = go start
  where
  go :: Int -> ST s Int
  go !ix = if ix < end
    then do
      !a <- readByteArray v ix
      if a > val
        then return ix
        else go (ix + 1)
    else return end

-- stepA assumes that we previously incremented ixA.
-- Consequently, we do not need to check that ixB
-- is still in bounds. As a precondition, both
-- indices are guarenteed to start in bounds.
merge :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
merge !_ !src !dst !start !mid !end = mergeNonContiguous (proxy# :: Proxy# a) src dst start mid mid end start

mergeTagged :: forall s n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> MutableByteArray s -- source tags
  -> MutableByteArray s -- dest tags
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
mergeTagged !_ !_ !src !dst !srcTags !dstTags !start !mid !end =
  mergeNonContiguousTagged (proxy# :: Proxy# a) (proxy# :: Proxy# n) src dst srcTags dstTags start mid mid end start

mergeNonContiguous :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- start A
  -> Int -- end A
  -> Int -- start B
  -> Int -- end B
  -> Int -- start destination
  -> ST s ()
mergeNonContiguous !_ !src !dst !startA !endA !startB !endB !startDst =
  if startB < endB
    then stepA startA startB startDst
    else if startA < endA
      then stepB startA startB startDst
      else return ()
  where
  continue :: Int -> Int -> Int -> ST s ()
  continue ixA ixB ixDst = do
    !a <- readByteArray src ixA
    !b <- readByteArray src ixB
    if (a :: a) <= b
      then do
        writeByteArray dst ixDst a
        stepA (ixA + 1) ixB (ixDst + 1)
      else do
        writeByteArray dst ixDst b
        stepB ixA (ixB + 1) (ixDst + 1)
  stepB :: Int -> Int -> Int -> ST s ()
  stepB !ixA !ixB !ixDst = if ixB < endB
    then continue ixA ixB ixDst
    else finishA ixA ixDst
  stepA :: Int -> Int -> Int -> ST s ()
  stepA !ixA !ixB !ixDst = if ixA < endA
    then continue ixA ixB ixDst
    else finishB ixB ixDst
  finishB :: Int -> Int -> ST s ()
  finishB !ixB !ixDst = copyMutableByteArray (proxy# :: Proxy# a) dst ixDst src ixB (endB - ixB)
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = copyMutableByteArray (proxy# :: Proxy# a) dst ixDst src ixA (endA - ixA)

mergeNonContiguousTagged :: forall s n a. (Ord a, Prim a, Prim n)
  => Proxy# a
  -> Proxy# n
  -> MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> MutableByteArray s -- source tags
  -> MutableByteArray s -- dest tags
  -> Int -- start A
  -> Int -- end A
  -> Int -- start B
  -> Int -- end B
  -> Int -- start destination
  -> ST s ()
mergeNonContiguousTagged !_ !_ !src !dst !srcTags !dstTags !startA !endA !startB !endB !startDst =
  if startB < endB
    then stepA startA startB startDst
    else if startA < endA
      then stepB startA startB startDst
      else return ()
  where
  continue :: Int -> Int -> Int -> ST s ()
  continue ixA ixB ixDst = do
    !a <- readByteArray src ixA
    !b <- readByteArray src ixB
    if (a :: a) <= b
      then do
        writeByteArray dst ixDst a
        (readByteArray srcTags ixA :: ST s n) >>= writeByteArray dstTags ixA
        stepA (ixA + 1) ixB (ixDst + 1)
      else do
        writeByteArray dst ixDst b
        (readByteArray srcTags ixB :: ST s n) >>= writeByteArray dstTags ixB
        stepB ixA (ixB + 1) (ixDst + 1)
  stepB :: Int -> Int -> Int -> ST s ()
  stepB !ixA !ixB !ixDst = if ixB < endB
    then continue ixA ixB ixDst
    else finishA ixA ixDst
  stepA :: Int -> Int -> Int -> ST s ()
  stepA !ixA !ixB !ixDst = if ixA < endA
    then continue ixA ixB ixDst
    else finishB ixB ixDst
  finishB :: Int -> Int -> ST s ()
  finishB !ixB !ixDst = do
    copyMutableByteArray (proxy# :: Proxy# a) dst ixDst src ixB (endB - ixB)
    copyMutableByteArray (proxy# :: Proxy# n) dstTags ixDst srcTags ixB (endB - ixB)
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = do
    copyMutableByteArray (proxy# :: Proxy# a) dst ixDst src ixA (endA - ixA)
    copyMutableByteArray (proxy# :: Proxy# n) dstTags ixDst srcTags ixA (endA - ixA)

-- step is a more simple approach, but it actually
-- slows down the merge process by 10%. Weird.
-- step :: Int -> Int -> Int -> ST s ()
-- step !ixA !ixB !ixDst = case (ltFast ixA mid) .&. (ltFast ixB end) of
--   1 -> do
--     !a <- readByteArray src ixA
--     !b <- readByteArray src ixB
--     if a <= b
--       then do
--         writeElement dst ixDst a
--         step (ixA + 1) ixB (ixDst + 1)
--       else do
--         writeElement dst ixDst b
--         step ixA (ixB + 1) (ixDst + 1)
--   _ -> if ixA >= mid
--     then finishB ixB ixDst
--     else finishA ixA ixDst

threshold :: Int
threshold = 16

-- insertionSort :: ByteArray -> ByteArray
-- insertionSort arr = runST $ do
--   let len = sizeofByteArray arr
--   work <- newByteArray len
--   copyByteArray work 0 arr 0 len
--   insertionSortRange work 0 len
--   unsafeFreezeByteArray work

insertionSortRange :: forall s a. (Ord a, Prim a)
  => Proxy# a
  -> MutableByteArray s
  -> Int -- start
  -> Int -- end
  -> ST s ()
insertionSortRange !_ !arr !start !end = go start
  where
  go :: Int -> ST s ()
  go !ix = if ix < end
    then do
      !a <- readByteArray arr ix
      insertElement arr (a :: a) start ix
      go (ix + 1)
    else return ()
    
insertElement :: forall s a. (Ord a, Prim a) => MutableByteArray s -> a -> Int -> Int -> ST s ()
insertElement !arr !a !start !end = go end
  where
  go :: Int -> ST s ()
  go !ix = if ix > start
    then do
      !b <- readByteArray arr (ix - 1)
      if b <= a
        then do
          copyMutableByteArray (proxy# :: Proxy# a) arr (ix + 1) arr ix (end - ix)
          writeByteArray arr ix a
        else go (ix - 1)
    else do
      copyMutableByteArray (proxy# :: Proxy# a) arr (ix + 1) arr ix (end - ix)
      writeByteArray arr ix a

forkST_ :: ST s a -> ST s ()
forkST_ action = ST $ \s1 -> case forkST# action s1 of
  (# s2, _ #) -> (# s2, () #)

forkST# :: a -> State# s -> (# State# s, ThreadId# #)
forkST# = unsafeCoerce# fork#

data Lock s = Lock (MVar# s ())

newLock :: ST s (Lock s)
newLock = ST $ \s1 -> case newMVar# s1 of
  (# s2, v #) -> (# s2, Lock v #)

takeLock :: Lock s -> ST s ()
takeLock (Lock mvar#) = ST $ \ s# -> takeMVar# mvar# s#

putLock  :: Lock s -> ST s ()
putLock (Lock mvar#) = ST $ \ s# ->
  case putMVar# mvar# () s# of
    s2# -> (# s2#, () #)

-- | Execute the first computation on the main thread and
--   the second one on another thread in parallel. Blocks
--   until both are finished.
tandem :: ST s () -> ST s () -> ST s ()
tandem a b = do
  lock <- newLock
  forkST_ (b >> putLock lock)
  a
  takeLock lock

-- | Fold over a collection in parallel, discarding the results.
forConcurrently_ :: V.Vector a -> (a -> ST s b) -> ST s ()
forConcurrently_ xs f = do
  lock <- newLock
  total <- V.foldM (\ !n a -> forkST_ (f a >> putLock lock) >> return (n + 1)) 0 xs
  replicateM_ total (takeLock lock)

copyMutableByteArray :: forall s a.
     (Prim a)
  => Proxy# a
  -> MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> MutableByteArray s -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyMutableByteArray !_ (MutableByteArray dst#) (I# doff#) (MutableByteArray src#) (I# soff#) (I# n#)
  = ST $ \s1 -> case copyMutableByteArray# src# (soff# *# (sizeOf# (undefined :: a))) dst# (doff# *# (sizeOf# (undefined :: a))) (n# *# (sizeOf# (undefined :: a))) s1 of
      s2 -> (# s2, () #)

copyByteArray :: forall s a. (Prim a)
  => Proxy# a
  -> MutableByteArray s -- ^ destination array
  -> Int -- ^ offset into destination array
  -> ByteArray -- ^ source array
  -> Int -- ^ offset into source array
  -> Int -- ^ number of bytes to copy
  -> ST s ()
copyByteArray !_ !dst !doff !src !soff !n =
  P.copyByteArray dst (doff * P.sizeOf (undefined :: a)) src (soff * P.sizeOf (undefined :: a)) (n * P.sizeOf (undefined :: a))

newByteArray :: forall s a. Prim a => Proxy# a -> Int -> ST s (MutableByteArray s)
newByteArray !_ !sz = P.newByteArray (sz * P.sizeOf (undefined :: a))

sizeofByteArray :: forall a. Prim a => Proxy# a -> ByteArray -> Int
sizeofByteArray !_ !arr = div (P.sizeofByteArray arr) (P.sizeOf (undefined :: a))

