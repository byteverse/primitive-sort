{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{-# OPTIONS_GHC -O2 -Wall #-}

module Data.Primitive.Sort
  ( -- * Sorting
    sort
  , sortUnique
  , sortMutable
  , sortUniqueMutable
  ) where

import Control.Monad.ST
import GHC.ST (ST(..))
import GHC.IO (IO(..))
import GHC.Int (Int(..))
import Control.Monad
import GHC.Prim
import Control.Concurrent (getNumCapabilities)
import Data.Int
import Data.Word
import Data.Primitive.PrimArray (PrimArray)
import Data.Primitive.Contiguous (Contiguous,Mutable,Element)
import qualified Data.Primitive.Contiguous as C
import qualified Data.Primitive as P
import qualified Data.Vector as V

sort :: (Contiguous arr, Element arr a, Ord a)
  => arr a
  -> arr a
{-# INLINABLE sort #-}
sort !src = runST $ do
  let len = C.size src
  dst <- C.new (C.size src)
  C.copy dst 0 src 0 len
  sortMutable dst
  C.unsafeFreeze dst

-- | Sort the mutable array. This operation preserves duplicate
-- elements. For example:
--
-- > [5,7,2,3,5,4,3] => [3,3,4,5,5,7]
sortMutable :: (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> ST s ()
{-# INLINABLE sortMutable #-}
sortMutable !dst = do
  len <- C.sizeMutable dst
  if len < threshold
    then insertionSortRange dst 0 len
    else do
      work <- C.new len
      C.copyMutable work 0 dst 0 len 
      caps <- unsafeEmbedIO getNumCapabilities
      let minElemsPerThread = 20000
          maxThreads = div len minElemsPerThread
          preThreads = min caps maxThreads
          threads = if preThreads == 1 then 1 else preThreads * 8
      -- I cannot understand why, but GHC's runtime does better
      -- when we let this schedule 8 times as many threads as
      -- we have capabilities. However, we only get this benefit
      -- when we actually have more than one capability.
      splitMergeParallel dst work threads 0 len

sortUnique :: (Contiguous arr, Element arr a, Ord a)
  => arr a -> arr a
sortUnique src = runST $ do
  let len = C.size src
  dst <- C.new len
  C.copy dst 0 src 0 len
  res <- sortUniqueMutable dst
  C.unsafeFreeze res
{-# INLINABLE sortUnique #-}

sortUniqueMutable :: (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> ST s (Mutable arr s a)
sortUniqueMutable marr = do
  sortMutable marr
  uniqueMutable marr
{-# INLINABLE sortUniqueMutable #-}

-- | Discards adjacent equal elements from an array. This operation
-- may run in-place, or it may need to allocate a new array, so the
-- argument may not be reused after this function is applied to it.
uniqueMutable :: forall arr s a. (Contiguous arr, Element arr a, Eq a)
  => Mutable arr s a -> ST s (Mutable arr s a)
{-# INLINABLE uniqueMutable #-}
uniqueMutable !marr = do
  !len <- C.sizeMutable marr
  if len > 1
    then do
      !a0 <- C.read marr 0
      let findFirstDuplicate :: a -> Int -> ST s Int
          findFirstDuplicate !prev !ix = if ix < len
            then do
              a <- C.read marr ix
              if a == prev
                then return ix
                else findFirstDuplicate a (ix + 1)
            else return ix
      dupIx <- findFirstDuplicate a0 1
      if dupIx == len
        then return marr
        else do
          let deduplicate :: a -> Int -> Int -> ST s Int
              deduplicate !prev !srcIx !dstIx = if srcIx < len
                then do
                  a <- C.read marr srcIx
                  if a == prev
                    then deduplicate a (srcIx + 1) dstIx
                    else do
                      C.write marr dstIx a
                      deduplicate a (srcIx + 1) (dstIx + 1)
                else return dstIx
          !a <- C.read marr dupIx
          !reducedLen <- deduplicate a (dupIx + 1) dupIx
          C.resize marr reducedLen
    else return marr

unsafeEmbedIO :: IO a -> ST s a
unsafeEmbedIO (IO f) = ST (unsafeCoerce# f)

half :: Int -> Int
half x = div x 2

splitMergeParallel :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source and destination
  -> Mutable arr s a -- work array
  -> Int -- spark limit, should be power of two
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE splitMergeParallel #-}
splitMergeParallel !arr !work !level !start !end = if level > 1
  then if end - start < threshold
    then insertionSortRange arr start end
    else do
      let !mid = div (end + start) 2
          !levelDown = half level
      tandem 
        (splitMergeParallel work arr levelDown start mid)
        (splitMergeParallel work arr levelDown mid end)
      mergeParallel work arr level start mid end
  else splitMerge arr work start end

splitMerge :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source and destination
  -> Mutable arr s a -- work array
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE splitMerge #-}
splitMerge !arr !work !start !end = if end - start < 2
  then return ()
  else do
    if end - start > threshold
      then do
        let !mid = div (end + start) 2
        splitMerge work arr start mid
        splitMerge work arr mid end
        merge work arr start mid end
      else insertionSortRange arr start end

mergeParallel :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source
  -> Mutable arr s a -- dest
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
{-# INLINABLE mergeParallel #-}
mergeParallel !src !dst !threads !start !mid !end = do
  lock <- newLock
  let go :: Int -- previous A end
         -> Int -- previous B end
         -> Int -- how many chunk have we already iterated over
         -> ST s Int
      go !prevEndA !prevEndB !ix = 
        if | prevEndA == mid && prevEndB == end -> return ix
           | prevEndA == mid -> do
               forkST_ $ do
                 let !startA = mid
                     !endA = mid
                     !startB = prevEndB
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putLock lock
               go mid end (ix + 1)
           | prevEndB == end -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = end
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putLock lock
               go mid end (ix + 1)
           | ix == threads - 1 -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = prevEndB
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putLock lock
               return (ix + 1)
           | otherwise -> do
               -- We use the left half for this lookup. We could instead
               -- use both halves and take the median.
               !endElem <- C.read src (start + chunk * (ix + 1))
               !endA <- findIndexOfGtElem src (endElem :: a) prevEndA mid
               !endB <- findIndexOfGtElem src endElem prevEndB end
               forkST_ $ do
                 let !startA = prevEndA
                     !startB = prevEndB
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putLock lock
               go endA endB (ix + 1)
  !endElem <- C.read src (start + chunk) 
  !endA <- findIndexOfGtElem src (endElem :: a) start mid
  !endB <- findIndexOfGtElem src endElem mid end
  forkST_ $ do
    let !startA = start
        !startB = mid
        !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguous src dst startA endA startB endB startDst
    putLock lock
  total <- go endA endB 1
  replicateM_ total (takeLock lock)
  where
  !chunk = div (end - start) threads

-- If the needle is bigger than everything in the slice
-- of the array, this returns the end index (which is out
-- of bounds). Callers of this function should be able
-- to handle that.
findIndexOfGtElem :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -> a -> Int -> Int -> ST s Int
{-# INLINABLE findIndexOfGtElem #-}
findIndexOfGtElem !v !needle !start !end = go start end
  where
  go :: Int -> Int -> ST s Int
  go !lo !hi = if lo < hi
    then do
      let !mid = lo + half (hi - lo)
      !val <- C.read v mid
      if | val == needle -> gallopToGtIndex v needle (mid + 1) hi
         | val < needle -> go (mid + 1) hi
         | otherwise -> go lo mid
    else return lo

-- | TODO: should probably turn this into a real galloping search
gallopToGtIndex :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -> a -> Int -> Int -> ST s Int
{-# INLINABLE gallopToGtIndex #-}
gallopToGtIndex !v !val !start !end = go start
  where
  go :: Int -> ST s Int
  go !ix = if ix < end
    then do
      !a <- C.read v ix
      if a > val
        then return ix
        else go (ix + 1)
    else return end

-- stepA assumes that we previously incremented ixA.
-- Consequently, we do not need to check that ixB
-- is still in bounds. As a precondition, both
-- indices are guarenteed to start in bounds.
merge :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source
  -> Mutable arr s a -- dest
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
{-# INLINABLE merge #-}
merge !src !dst !start !mid !end = mergeNonContiguous src dst start mid mid end start

mergeNonContiguous :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source
  -> Mutable arr s a -- dest
  -> Int -- start A
  -> Int -- end A
  -> Int -- start B
  -> Int -- end B
  -> Int -- start destination
  -> ST s ()
{-# INLINABLE mergeNonContiguous #-}
mergeNonContiguous !src !dst !startA !endA !startB !endB !startDst =
  if startB < endB
    then stepA startA startB startDst
    else if startA < endA
      then stepB startA startB startDst
      else return ()
  where
  continue :: Int -> Int -> Int -> ST s ()
  continue ixA ixB ixDst = do
    !a <- C.read src ixA
    !b <- C.read src ixB
    if (a :: a) <= b
      then do
        C.write dst ixDst a
        stepA (ixA + 1) ixB (ixDst + 1)
      else do
        C.write dst ixDst b
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
  finishB !ixB !ixDst = C.copyMutable dst ixDst src ixB (endB - ixB)
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = C.copyMutable dst ixDst src ixA (endA - ixA)

threshold :: Int
threshold = 16

insertionSortRange :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE insertionSortRange #-}
insertionSortRange !arr !start !end = go start
  where
  go :: Int -> ST s ()
  go !ix = if ix < end
    then do
      !a <- C.read arr ix
      insertElement arr (a :: a) start ix
      go (ix + 1)
    else return ()
    
insertElement :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> a
  -> Int
  -> Int
  -> ST s ()
{-# INLINABLE insertElement #-}
insertElement !arr !a !start !end = go end
  where
  go :: Int -> ST s ()
  go !ix = if ix > start
    then do
      !b <- C.read arr (ix - 1)
      if b <= a
        then do
          C.copyMutable arr (ix + 1) arr ix (end - ix)
          C.write arr ix a
        else go (ix - 1)
    else do
      C.copyMutable arr (ix + 1) arr ix (end - ix)
      C.write arr ix a

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

