{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{-# OPTIONS_GHC -Wall #-}

-- | Sort primitive arrays with a stable sorting algorithm. All functions
-- in this module are marked as @INLINABLE@, so they will specialize
-- when used in a monomorphic setting.
module Data.Primitive.Sort
  ( -- * Immutable
    sort
  , sortUnique
  , sortTagged
  , sortUniqueTagged
    -- * Mutable
  , sortMutable
  , sortUniqueMutable
  , sortTaggedMutable
  , sortUniqueTaggedMutable
  ) where

import Control.Monad.ST
import Control.Applicative
import GHC.ST (ST(..))
import GHC.IO (IO(..))
import GHC.Int (Int(..))
import Control.Monad
import GHC.Prim
import Data.Primitive.MVar (takeMVar,putMVar,newEmptyMVar)
import Control.Concurrent (getNumCapabilities)
import Data.Primitive.Contiguous (Contiguous,Mutable,Element)
import qualified Data.Primitive.Contiguous as C

-- | Sort an immutable array. Duplicate elements are preserved.
--
-- >>> sort ([5,6,7,9,5,4,5,7] :: Array Int)
-- fromListN 8 [4,5,5,5,6,7,7,9]
sort :: (Contiguous arr, Element arr a, Ord a)
  => arr a
  -> arr a
{-# INLINABLE sort #-}
sort !src = runST $ do
  let len = C.size src
  dst <- C.new (C.size src)
  C.copy dst 0 src 0 len
  res <- sortMutable dst
  C.unsafeFreeze res

-- | Sort a tagged immutable array. Each element from the @keys@ array is
-- paired up with an element from the @values@ array at the matching
-- index. The sort permutes the @values@ array so that a value end up
-- in the same position as its corresponding key. The two argument array
-- should be of the same length, but if one is shorter than the other,
-- the longer one will be truncated so that the lengths match.
--
-- >>> sortTagged ([5,6,7,5,5,7] :: Array Int) ([1,2,3,4,5,6] :: Array Int)
-- (fromListN 6 [5,5,5,6,7,7],fromListN 6 [1,4,5,2,3,6])
--
-- Since the sort is stable, the values corresponding to a key that
-- appears multiple times have their original order preserved.
sortTagged :: forall k v karr varr. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => karr k -- ^ keys
  -> varr v -- ^ values
  -> (karr k,varr v)
{-# INLINABLE sortTagged #-}
sortTagged !src !srcTags = runST $ do
  let len = min (C.size src) (C.size srcTags)
  dst <- C.new len
  C.copy dst 0 src 0 len
  dstTags <- C.new len
  C.copy dstTags 0 srcTags 0 len
  (res,resTags) <- sortTaggedMutableN len dst dstTags
  res' <- C.unsafeFreeze res
  resTags' <- C.unsafeFreeze resTags
  return (res',resTags')

-- | Sort a tagged immutable array. Only a single copy of each
-- duplicate key is preserved, along with the last value from @values@
-- that corresponded to it. The two argument arrays
-- should be of the same length, but if one is shorter than the other,
-- the longer one will be truncated so that the lengths match.
--
-- >>> sortUniqueTagged ([5,6,7,5,5,7] :: Array Int) ([1,2,3,4,5,6] :: Array Int)
-- (fromListN 3 [5,6,7],fromListN 3 [5,2,6])
sortUniqueTagged :: forall k v karr varr. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => karr k -- ^ keys
  -> varr v -- ^ values
  -> (karr k,varr v)
{-# INLINABLE sortUniqueTagged #-}
sortUniqueTagged !src !srcTags = runST $ do
  let len = min (C.size src) (C.size srcTags)
  dst <- C.new len
  C.copy dst 0 src 0 len
  dstTags <- C.new len
  C.copy dstTags 0 srcTags 0 len
  (res0,resTags0) <- sortTaggedMutableN len dst dstTags
  (res1,resTags1) <- uniqueTaggedMutableN len res0 resTags0
  res' <- C.unsafeFreeze res1
  resTags' <- C.unsafeFreeze resTags1
  return (res',resTags')

-- | Sort the mutable array. This operation preserves duplicate
-- elements. The argument may either be modified in-place, or another
-- array may be allocated and returned. The argument
-- may not be reused after being passed to this function.
sortMutable :: (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> ST s (Mutable arr s a)
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
          maxThreads = unsafeQuot len minElemsPerThread
          preThreads = min caps maxThreads
          threads = if preThreads == 1 then 1 else preThreads * 8
      -- I cannot understand why, but GHC's runtime does better
      -- when we let this schedule 8 times as many threads as
      -- we have capabilities. However, we only get this benefit
      -- when we actually have more than one capability.
      splitMergeParallel dst work threads 0 len
  return dst

-- | Sort an array of a key type @k@, rearranging the values of
-- type @v@ according to the element they correspond to in the
-- key array. The argument arrays may not be reused after they
-- are passed to the function.
sortTaggedMutable :: (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k
  -> Mutable varr s v
  -> ST s (Mutable karr s k, Mutable varr s v)
{-# INLINABLE sortTaggedMutable #-}
sortTaggedMutable !dst0 !dstTags0 = do
  (!dst,!dstTags,!len) <- alignArrays dst0 dstTags0
  sortTaggedMutableN len dst dstTags

alignArrays :: (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k
  -> Mutable varr s v
  -> ST s (Mutable karr s k, Mutable varr s v,Int)
{-# INLINABLE alignArrays #-}
alignArrays dst0 dstTags0 = do
  lenDst <- C.sizeMutable dst0
  lenDstTags <- C.sizeMutable dstTags0
  -- This cleans up mismatched lengths.
  if lenDst == lenDstTags
    then return (dst0,dstTags0,lenDst)
    else if lenDst < lenDstTags
      then do
        dstTags <- C.resize dstTags0 lenDst
        return (dst0,dstTags,lenDst)
      else do
        dst <- C.resize dst0 lenDstTags
        return (dst,dstTags0,lenDstTags)

sortUniqueTaggedMutable :: (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k -- ^ keys
  -> Mutable varr s v -- ^ values
  -> ST s (Mutable karr s k, Mutable varr s v)
{-# INLINABLE sortUniqueTaggedMutable #-}
sortUniqueTaggedMutable dst0 dstTags0 = do
  (!dst1,!dstTags1,!len) <- alignArrays dst0 dstTags0
  (!dst2,!dstTags2) <- sortTaggedMutableN len dst1 dstTags1
  uniqueTaggedMutableN len dst2 dstTags2

sortTaggedMutableN :: (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Int
  -> Mutable karr s k
  -> Mutable varr s v
  -> ST s (Mutable karr s k, Mutable varr s v)
{-# INLINABLE sortTaggedMutableN #-}
sortTaggedMutableN !len !dst !dstTags = if len < thresholdTagged
  then do
    insertionSortTaggedRange dst dstTags 0 len
    return (dst,dstTags)
  else do
    work <- C.cloneMutable dst 0 len 
    workTags <- C.cloneMutable dstTags 0 len 
    caps <- unsafeEmbedIO getNumCapabilities
    let minElemsPerThread = 20000
        maxThreads = unsafeQuot len minElemsPerThread
        preThreads = min caps maxThreads
        threads = if preThreads == 1 then 1 else preThreads * 8
    splitMergeParallelTagged dst work dstTags workTags threads 0 len
    return (dst,dstTags)

-- | Sort an immutable array. Only a single copy of each duplicated
-- element is preserved.
--
-- >>> sortUnique ([5,6,7,9,5,4,5,7] :: Array Int)
-- fromListN 5 [4,5,6,7,9]
sortUnique :: (Contiguous arr, Element arr a, Ord a)
  => arr a -> arr a
{-# INLINABLE sortUnique #-}
sortUnique src = runST $ do
  let len = C.size src
  dst <- C.new len
  C.copy dst 0 src 0 len
  res <- sortUniqueMutable dst
  C.unsafeFreeze res

-- | Sort an immutable array. Only a single copy of each duplicated
-- element is preserved. This operation may run in-place, or it may
-- need to allocate a new array, so the argument may not be reused
-- after this function is applied to it. 
sortUniqueMutable :: (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a
  -> ST s (Mutable arr s a)
{-# INLINABLE sortUniqueMutable #-}
sortUniqueMutable marr = do
  res <- sortMutable marr
  uniqueMutable res

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

uniqueTaggedMutableN :: forall karr varr s k v. (Contiguous karr, Element karr k, Eq k, Contiguous varr, Element varr v)
  => Int
  -> Mutable karr s k
  -> Mutable varr s v
  -> ST s (Mutable karr s k, Mutable varr s v)
{-# INLINABLE uniqueTaggedMutableN #-}
uniqueTaggedMutableN !len !marr !marrTags = if len > 1
  then do
    !a0 <- C.read marr 0
    let findFirstDuplicate :: k -> Int -> ST s Int
        findFirstDuplicate !prev !ix = if ix < len
          then do
            a <- C.read marr ix
            if a == prev
              then return ix
              else findFirstDuplicate a (ix + 1)
          else return ix
    dupIx <- findFirstDuplicate a0 1
    if dupIx == len
      then return (marr,marrTags)
      else do
        C.read marrTags dupIx >>= C.write marrTags (dupIx - 1)
        let deduplicate :: k -> Int -> Int -> ST s Int
            deduplicate !prev !srcIx !dstIx = if srcIx < len
              then do
                a <- C.read marr srcIx
                if a == prev
                  then do
                    C.read marrTags srcIx >>= C.write marrTags (dstIx - 1)
                    deduplicate a (srcIx + 1) dstIx
                  else do
                    C.read marrTags srcIx >>= C.write marrTags dstIx
                    C.write marr dstIx a
                    deduplicate a (srcIx + 1) (dstIx + 1)
              else return dstIx
        !a <- C.read marr dupIx
        !reducedLen <- deduplicate a (dupIx + 1) dupIx
        liftA2 (,) (C.resize marr reducedLen) (C.resize marrTags reducedLen)
  else return (marr,marrTags)

unsafeEmbedIO :: IO a -> ST s a
unsafeEmbedIO (IO f) = ST (unsafeCoerce# f)

half :: Int -> Int
half x = unsafeQuot x 2

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
      let !mid = unsafeQuot (end + start) 2
          !levelDown = half level
      tandem 
        (splitMergeParallel work arr levelDown start mid)
        (splitMergeParallel work arr levelDown mid end)
      mergeParallel work arr level start mid end
  else splitMerge arr work start end

splitMergeParallelTagged :: forall karr varr s k v. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k -- source and destination
  -> Mutable karr s k -- work array
  -> Mutable varr s v -- source and destination tags
  -> Mutable varr s v -- work tags
  -> Int -- spark limit, should be power of two
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE splitMergeParallelTagged #-}
splitMergeParallelTagged !arr !work !arrTags !workTags !level !start !end = if level > 1
  then do
    let !mid = unsafeQuot (end + start) 2
        !levelDown = half level
    tandem 
      (splitMergeParallelTagged work arr workTags arrTags levelDown start mid)
      (splitMergeParallelTagged work arr workTags arrTags levelDown mid end)
    mergeParallelTagged work arr workTags arrTags level start mid end
  else splitMergeTagged arr work arrTags workTags start end

splitMerge :: forall arr s a. (Contiguous arr, Element arr a, Ord a)
  => Mutable arr s a -- source and destination
  -> Mutable arr s a -- work array
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE splitMerge #-}
splitMerge !arr !work !start !end = if end - start < 2
  then return ()
  else if end - start > threshold
    then do
      let !mid = unsafeQuot (end + start) 2
      splitMerge work arr start mid
      splitMerge work arr mid end
      mergeNonContiguous work arr start mid mid end start
    else insertionSortRange arr start end

splitMergeTagged :: (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k -- source and destination
  -> Mutable karr s k -- work array
  -> Mutable varr s v
  -> Mutable varr s v
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE splitMergeTagged #-}
splitMergeTagged !arr !work !arrTags !workTags !start !end = if end - start < 2
  then return ()
  else if end - start > thresholdTagged
    then do
      let !mid = unsafeQuot (end + start) 2
      splitMergeTagged work arr workTags arrTags start mid
      splitMergeTagged work arr workTags arrTags mid end
      mergeNonContiguousTagged work arr workTags arrTags start mid mid end start
    else insertionSortTaggedRange arr arrTags start end

-- Precondition: threads is greater than 0
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
  !lock <- newEmptyMVar
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
                 putMVar lock ()
               go mid end (ix + 1)
           | prevEndB == end -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = end
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putMVar lock ()
               go mid end (ix + 1)
           | ix == threads - 1 -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = prevEndB
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguous src dst startA endA startB endB startDst
                 putMVar lock ()
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
                 putMVar lock ()
               go endA endB (ix + 1)
  !endElem <- C.read src (start + chunk) 
  !endA <- findIndexOfGtElem src (endElem :: a) start mid
  !endB <- findIndexOfGtElem src endElem mid end
  forkST_ $ do
    let !startA = start
        !startB = mid
        !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguous src dst startA endA startB endB startDst
    putMVar lock ()
  total <- go endA endB 1
  replicateM_ total (takeMVar lock)
  where
  !chunk = unsafeQuot (end - start) threads

-- Precondition: threads is greater than 0
-- This function is just a copy of mergeParallel but with
-- the tags arrays passed to mergeNonContiguousTagged
mergeParallelTagged :: forall karr varr s k v. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k -- source
  -> Mutable karr s k -- dest
  -> Mutable varr s v -- source tags
  -> Mutable varr s v -- dest tags
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
{-# INLINABLE mergeParallelTagged #-}
mergeParallelTagged !src !dst !srcTags !dstTags !threads !start !mid !end = do
  !lock <- newEmptyMVar
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
                 mergeNonContiguousTagged src dst srcTags dstTags startA endA startB endB startDst
                 putMVar lock ()
               go mid end (ix + 1)
           | prevEndB == end -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = end
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguousTagged src dst srcTags dstTags startA endA startB endB startDst
                 putMVar lock ()
               go mid end (ix + 1)
           | ix == threads - 1 -> do
               forkST_ $ do
                 let !startA = prevEndA
                     !endA = mid
                     !startB = prevEndB
                     !endB = end
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguousTagged src dst srcTags dstTags startA endA startB endB startDst
                 putMVar lock ()
               return (ix + 1)
           | otherwise -> do
               -- We use the left half for this lookup. We could instead
               -- use both halves and take the median.
               !endElem <- C.read src (start + chunk * (ix + 1))
               !endA <- findIndexOfGtElem src (endElem :: k) prevEndA mid
               !endB <- findIndexOfGtElem src endElem prevEndB end
               forkST_ $ do
                 let !startA = prevEndA
                     !startB = prevEndB
                     !startDst = (startA - start) + (startB - mid) + start
                 mergeNonContiguousTagged src dst srcTags dstTags startA endA startB endB startDst
                 putMVar lock ()
               go endA endB (ix + 1)
  !endElem <- C.read src (start + chunk) 
  !endA <- findIndexOfGtElem src (endElem :: k) start mid
  !endB <- findIndexOfGtElem src endElem mid end
  forkST_ $ do
    let !startA = start
        !startB = mid
        !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguousTagged src dst srcTags dstTags startA endA startB endB startDst
    putMVar lock ()
  total <- go endA endB 1
  replicateM_ total (takeMVar lock)
  where
  !chunk = unsafeQuot (end - start) threads

unsafeQuot :: Int -> Int -> Int
unsafeQuot (I# a) (I# b) = I# (quotInt# a b)

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

mergeNonContiguousTagged :: forall karr varr k v s. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k -- source
  -> Mutable karr s k -- dest
  -> Mutable varr s v -- source tags
  -> Mutable varr s v -- dest tags
  -> Int -- start A
  -> Int -- end A
  -> Int -- start B
  -> Int -- end B
  -> Int -- start destination
  -> ST s ()
{-# INLINABLE mergeNonContiguousTagged #-}
mergeNonContiguousTagged !src !dst !srcTags !dstTags !startA !endA !startB !endB !startDst =
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
    if a <= b
      then do
        C.write dst ixDst a
        (C.read srcTags ixA :: ST s v) >>= C.write dstTags ixDst
        stepA (ixA + 1) ixB (ixDst + 1)
      else do
        C.write dst ixDst b
        (C.read srcTags ixB :: ST s v) >>= C.write dstTags ixDst
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
    C.copyMutable dst ixDst src ixB (endB - ixB)
    C.copyMutable dstTags ixDst srcTags ixB (endB - ixB)
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = do
    C.copyMutable dst ixDst src ixA (endA - ixA)
    C.copyMutable dstTags ixDst srcTags ixA (endA - ixA)

threshold :: Int
threshold = 16

thresholdTagged :: Int
thresholdTagged = 16

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

insertionSortTaggedRange :: forall karr varr s k v. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k
  -> Mutable varr s v
  -> Int -- start
  -> Int -- end
  -> ST s ()
{-# INLINABLE insertionSortTaggedRange #-}
insertionSortTaggedRange !karr !varr !start !end = go start
  where
  go :: Int -> ST s ()
  go !ix = if ix < end
    then do
      !a <- C.read karr ix
      !v <- C.read varr ix
      insertElementTagged karr varr a v start ix
      go (ix + 1)
    else return ()
    
insertElementTagged :: forall karr varr s k v. (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v)
  => Mutable karr s k
  -> Mutable varr s v
  -> k
  -> v
  -> Int
  -> Int
  -> ST s ()
{-# INLINABLE insertElementTagged #-}
insertElementTagged !karr !varr !a !v !start !end = go end
  where
  go :: Int -> ST s ()
  go !ix = if ix > start
    then do
      !b <- C.read karr (ix - 1)
      if b <= a
        then do
          C.copyMutable karr (ix + 1) karr ix (end - ix)
          C.write karr ix a
          C.copyMutable varr (ix + 1) varr ix (end - ix)
          C.write varr ix v
        else go (ix - 1)
    else do
      C.copyMutable karr (ix + 1) karr ix (end - ix)
      C.write karr ix a
      C.copyMutable varr (ix + 1) varr ix (end - ix)
      C.write varr ix v


forkST_ :: ST s a -> ST s ()
forkST_ action = ST $ \s1 -> case forkST# action s1 of
  (# s2, _ #) -> (# s2, () #)

forkST# :: a -> State# s -> (# State# s, ThreadId# #)
forkST# = unsafeCoerce# fork#

-- | Execute the first computation on the main thread and
--   the second one on another thread in parallel. Blocks
--   until both are finished.
tandem :: ST s () -> ST s () -> ST s ()
tandem a b = do
  lock <- newEmptyMVar
  forkST_ (b >> putMVar lock ())
  a
  takeMVar lock

-- $setup
--
-- These are to make doctest work correctly.
--
-- >>> :set -XOverloadedLists
-- >>> import Data.Primitive.Array (Array)
--

