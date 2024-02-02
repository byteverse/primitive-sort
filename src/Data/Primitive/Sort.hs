{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Sort primitive arrays with a stable sorting algorithm. All functions
in this module are marked as @INLINABLE@, so they will specialize
when used in a monomorphic setting.
-}
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

import Control.Applicative
import Control.Monad.ST
import Data.Int
import Data.Primitive (MutablePrimArray, Prim, PrimArray)
import Data.Primitive.Contiguous (Contiguous, ContiguousU, Element, Mutable)
import qualified Data.Primitive.Contiguous as C
import Data.Word
import GHC.Int (Int (..))
import GHC.Exts

-- | Sort an immutable array. Duplicate elements are preserved.
sort ::
  (Prim a, Ord a) =>
  C.PrimArray a ->
  C.PrimArray a
{-# INLINEABLE sort #-}
{-# SPECIALIZE sort :: C.PrimArray Double -> C.PrimArray Double #-}
{-# SPECIALIZE sort :: C.PrimArray Int -> C.PrimArray Int #-}
{-# SPECIALIZE sort :: C.PrimArray Int64 -> C.PrimArray Int64 #-}
{-# SPECIALIZE sort :: C.PrimArray Int32 -> C.PrimArray Int32 #-}
{-# SPECIALIZE sort :: C.PrimArray Int16 -> C.PrimArray Int16 #-}
{-# SPECIALIZE sort :: C.PrimArray Int8 -> C.PrimArray Int8 #-}
{-# SPECIALIZE sort :: C.PrimArray Word -> C.PrimArray Word #-}
{-# SPECIALIZE sort :: C.PrimArray Word64 -> C.PrimArray Word64 #-}
{-# SPECIALIZE sort :: C.PrimArray Word32 -> C.PrimArray Word32 #-}
{-# SPECIALIZE sort :: C.PrimArray Word16 -> C.PrimArray Word16 #-}
{-# SPECIALIZE sort :: C.PrimArray Word8 -> C.PrimArray Word8 #-}
sort !src = runST $ do
  let len = C.size src
  dst <- C.new (C.size src)
  C.copy dst 0 (C.slice src 0 len)
  res <- sortMutable dst
  C.unsafeFreeze res

{- | Sort a tagged immutable array. Each element from the @keys@ array is
paired up with an element from the @values@ array at the matching
index. The sort permutes the @values@ array so that a value end up
in the same position as its corresponding key. The two argument array
should be of the same length, but if one is shorter than the other,
the longer one will be truncated so that the lengths match.

Since the sort is stable, the values corresponding to a key that
appears multiple times have their original order preserved.
-}
sortTagged ::
  forall k v karr varr.
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  -- | keys
  karr k ->
  -- | values
  varr v ->
  (karr k, varr v)
{-# INLINEABLE sortTagged #-}
sortTagged !src !srcTags = runST $ do
  let len = min (C.size src) (C.size srcTags)
  dst <- C.new len
  C.copy dst 0 (C.slice src 0 len)
  dstTags <- C.new len
  C.copy dstTags 0 (C.slice srcTags 0 len)
  (res, resTags) <- sortTaggedMutableN len dst dstTags
  res' <- C.unsafeFreeze res
  resTags' <- C.unsafeFreeze resTags
  return (res', resTags')

{- | Sort a tagged immutable array. Only a single copy of each
duplicate key is preserved, along with the last value from @values@
that corresponded to it. The two argument arrays
should be of the same length, but if one is shorter than the other,
the longer one will be truncated so that the lengths match.
-}
sortUniqueTagged ::
  forall k v karr varr.
  (ContiguousU karr, Element karr k, Ord k, ContiguousU varr, Element varr v) =>
  -- | keys
  karr k ->
  -- | values
  varr v ->
  (karr k, varr v)
{-# INLINEABLE sortUniqueTagged #-}
sortUniqueTagged !src !srcTags = runST $ do
  let len = min (C.size src) (C.size srcTags)
  dst <- C.new len
  C.copy dst 0 (C.slice src 0 len)
  dstTags <- C.new len
  C.copy dstTags 0 (C.slice srcTags 0 len)
  (res0, resTags0) <- sortTaggedMutableN len dst dstTags
  (res1, resTags1) <- uniqueTaggedMutableN len res0 resTags0
  res' <- C.unsafeFreeze res1
  resTags' <- C.unsafeFreeze resTags1
  return (res', resTags')

{- | Sort the mutable array. This operation preserves duplicate
elements. The argument may either be modified in-place, or another
array may be allocated and returned. The argument
may not be reused after being passed to this function.
-}
sortMutable ::
  (Prim a, Ord a) =>
  MutablePrimArray s a ->
  ST s (MutablePrimArray s a)
{-# INLINEABLE sortMutable #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Double -> ST s (C.MutablePrimArray s Double) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Int -> ST s (C.MutablePrimArray s Int) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Int64 -> ST s (C.MutablePrimArray s Int64) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Int32 -> ST s (C.MutablePrimArray s Int32) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Int16 -> ST s (C.MutablePrimArray s Int16) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Int8 -> ST s (C.MutablePrimArray s Int8) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Word -> ST s (C.MutablePrimArray s Word) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Word64 -> ST s (C.MutablePrimArray s Word64) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Word32 -> ST s (C.MutablePrimArray s Word32) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Word16 -> ST s (C.MutablePrimArray s Word16) #-}
{-# SPECIALIZE sortMutable :: forall s. C.MutablePrimArray s Word8 -> ST s (C.MutablePrimArray s Word8) #-}
sortMutable !dst = do
  len <- C.sizeMut dst
  if len < threshold
    then insertionSortRange dst 0 len
    else do
      work <- C.new len
      C.copyMut work 0 (C.sliceMut dst 0 len)
      splitMerge dst work 0 len
  return dst

{- | Sort an array of a key type @k@, rearranging the values of
type @v@ according to the element they correspond to in the
key array. The argument arrays may not be reused after they
are passed to the function.
-}
sortTaggedMutable ::
  (ContiguousU karr, Element karr k, Ord k, ContiguousU varr, Element varr v) =>
  Mutable karr s k ->
  Mutable varr s v ->
  ST s (Mutable karr s k, Mutable varr s v)
{-# INLINEABLE sortTaggedMutable #-}
sortTaggedMutable !dst0 !dstTags0 = do
  (!dst, !dstTags, !len) <- alignArrays dst0 dstTags0
  sortTaggedMutableN len dst dstTags

alignArrays ::
  (ContiguousU karr, Element karr k, Ord k, ContiguousU varr, Element varr v) =>
  Mutable karr s k ->
  Mutable varr s v ->
  ST s (Mutable karr s k, Mutable varr s v, Int)
{-# INLINEABLE alignArrays #-}
alignArrays dst0 dstTags0 = do
  lenDst <- C.sizeMut dst0
  lenDstTags <- C.sizeMut dstTags0
  -- This cleans up mismatched lengths.
  if lenDst == lenDstTags
    then return (dst0, dstTags0, lenDst)
    else
      if lenDst < lenDstTags
        then do
          dstTags <- C.resize dstTags0 lenDst
          return (dst0, dstTags, lenDst)
        else do
          dst <- C.resize dst0 lenDstTags
          return (dst, dstTags0, lenDstTags)

sortUniqueTaggedMutable ::
  (ContiguousU karr, Element karr k, Ord k, ContiguousU varr, Element varr v) =>
  -- | keys
  Mutable karr s k ->
  -- | values
  Mutable varr s v ->
  ST s (Mutable karr s k, Mutable varr s v)
{-# INLINEABLE sortUniqueTaggedMutable #-}
sortUniqueTaggedMutable dst0 dstTags0 = do
  (!dst1, !dstTags1, !len) <- alignArrays dst0 dstTags0
  (!dst2, !dstTags2) <- sortTaggedMutableN len dst1 dstTags1
  uniqueTaggedMutableN len dst2 dstTags2

sortTaggedMutableN ::
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  Int ->
  Mutable karr s k ->
  Mutable varr s v ->
  ST s (Mutable karr s k, Mutable varr s v)
{-# INLINEABLE sortTaggedMutableN #-}
sortTaggedMutableN !len !dst !dstTags =
  if len < thresholdTagged
    then do
      insertionSortTaggedRange dst dstTags 0 len
      return (dst, dstTags)
    else do
      work <- C.cloneMut (C.sliceMut dst 0 len)
      workTags <- C.cloneMut (C.sliceMut dstTags 0 len)
      splitMergeTagged dst work dstTags workTags 0 len
      return (dst, dstTags)

{- | Sort an immutable array. Only a single copy of each duplicated
element is preserved.
-}
sortUnique :: (Prim a, Ord a) => PrimArray a -> PrimArray a
{-# INLINEABLE sortUnique #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Double -> C.PrimArray Double #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Int -> C.PrimArray Int #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Int64 -> C.PrimArray Int64 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Int32 -> C.PrimArray Int32 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Int16 -> C.PrimArray Int16 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Int8 -> C.PrimArray Int8 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Word -> C.PrimArray Word #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Word64 -> C.PrimArray Word64 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Word32 -> C.PrimArray Word32 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Word16 -> C.PrimArray Word16 #-}
{-# SPECIALIZE sortUnique :: C.PrimArray Word8 -> C.PrimArray Word8 #-}
sortUnique src = runST $ do
  let len = C.size src
  dst <- C.new len
  C.copy dst 0 (C.slice src 0 len)
  res <- sortUniqueMutable dst
  C.unsafeFreeze res

{- | Sort an immutable array. Only a single copy of each duplicated
element is preserved. This operation may run in-place, or it may
need to allocate a new array, so the argument may not be reused
after this function is applied to it.
-}
sortUniqueMutable ::
  forall s a.
  (Prim a, Ord a) =>
  MutablePrimArray s a ->
  ST s (MutablePrimArray s a)
{-# INLINEABLE sortUniqueMutable #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Double -> ST s (C.MutablePrimArray s Double) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Int -> ST s (C.MutablePrimArray s Int) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Int64 -> ST s (C.MutablePrimArray s Int64) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Int32 -> ST s (C.MutablePrimArray s Int32) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Int16 -> ST s (C.MutablePrimArray s Int16) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Int8 -> ST s (C.MutablePrimArray s Int8) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Word -> ST s (C.MutablePrimArray s Word) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Word64 -> ST s (C.MutablePrimArray s Word64) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Word32 -> ST s (C.MutablePrimArray s Word32) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Word16 -> ST s (C.MutablePrimArray s Word16) #-}
{-# SPECIALIZE sortUniqueMutable :: forall s. C.MutablePrimArray s Word8 -> ST s (C.MutablePrimArray s Word8) #-}
sortUniqueMutable marr = do
  res <- sortMutable marr
  uniqueMutable res

{- | Discards adjacent equal elements from an array. This operation
may run in-place, or it may need to allocate a new array, so the
argument may not be reused after this function is applied to it.
-}
uniqueMutable ::
  forall arr s a.
  (ContiguousU arr, Element arr a, Eq a) =>
  Mutable arr s a ->
  ST s (Mutable arr s a)
{-# INLINEABLE uniqueMutable #-}
uniqueMutable !marr = do
  !len <- C.sizeMut marr
  if len > 1
    then do
      !a0 <- C.read marr 0
      let findFirstDuplicate :: a -> Int -> ST s Int
          findFirstDuplicate !prev !ix =
            if ix < len
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
              deduplicate !prev !srcIx !dstIx =
                if srcIx < len
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

uniqueTaggedMutableN ::
  forall karr varr s k v.
  (ContiguousU karr, Element karr k, Eq k, ContiguousU varr, Element varr v) =>
  Int ->
  Mutable karr s k ->
  Mutable varr s v ->
  ST s (Mutable karr s k, Mutable varr s v)
{-# INLINEABLE uniqueTaggedMutableN #-}
uniqueTaggedMutableN !len !marr !marrTags =
  if len > 1
    then do
      !a0 <- C.read marr 0
      let findFirstDuplicate :: k -> Int -> ST s Int
          findFirstDuplicate !prev !ix =
            if ix < len
              then do
                a <- C.read marr ix
                if a == prev
                  then return ix
                  else findFirstDuplicate a (ix + 1)
              else return ix
      dupIx <- findFirstDuplicate a0 1
      if dupIx == len
        then return (marr, marrTags)
        else do
          C.read marrTags dupIx >>= C.write marrTags (dupIx - 1)
          let deduplicate :: k -> Int -> Int -> ST s Int
              deduplicate !prev !srcIx !dstIx =
                if srcIx < len
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
    else return (marr, marrTags)

splitMerge ::
  forall s a.
  (Prim a, Ord a) =>
  MutablePrimArray s a -> -- source and destination
  MutablePrimArray s a -> -- work array
  Int -> -- start
  Int -> -- end
  ST s ()
{-# INLINEABLE splitMerge #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Double -> C.MutablePrimArray s Double -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Int -> C.MutablePrimArray s Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Int64 -> C.MutablePrimArray s Int64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Int32 -> C.MutablePrimArray s Int32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Int16 -> C.MutablePrimArray s Int16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Int8 -> C.MutablePrimArray s Int8 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Word -> C.MutablePrimArray s Word -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Word64 -> C.MutablePrimArray s Word64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Word32 -> C.MutablePrimArray s Word32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Word16 -> C.MutablePrimArray s Word16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE splitMerge :: forall s. C.MutablePrimArray s Word8 -> C.MutablePrimArray s Word8 -> Int -> Int -> ST s () #-}
splitMerge !arr !work !start !end =
  if end - start < 2
    then return ()
    else
      if end - start > threshold
        then do
          let !mid = unsafeQuot (end + start) 2
          splitMerge work arr start mid
          splitMerge work arr mid end
          mergeNonContiguous work arr start mid mid end start
        else insertionSortRange arr start end

splitMergeTagged ::
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  Mutable karr s k -> -- source and destination
  Mutable karr s k -> -- work array
  Mutable varr s v ->
  Mutable varr s v ->
  Int -> -- start
  Int -> -- end
  ST s ()
{-# INLINEABLE splitMergeTagged #-}
splitMergeTagged !arr !work !arrTags !workTags !start !end =
  if end - start < 2
    then return ()
    else
      if end - start > thresholdTagged
        then do
          let !mid = unsafeQuot (end + start) 2
          splitMergeTagged work arr workTags arrTags start mid
          splitMergeTagged work arr workTags arrTags mid end
          mergeNonContiguousTagged work arr workTags arrTags start mid mid end start
        else insertionSortTaggedRange arr arrTags start end

unsafeQuot :: Int -> Int -> Int
unsafeQuot (I# a) (I# b) = I# (quotInt# a b)
{-# INLINE unsafeQuot #-}

-- stepA assumes that we previously incremented ixA.
-- Consequently, we do not need to check that ixB
-- is still in bounds. As a precondition, both
-- indices are guarenteed to start in bounds.
mergeNonContiguous ::
  forall arr s a.
  (Contiguous arr, Element arr a, Ord a) =>
  Mutable arr s a -> -- source
  Mutable arr s a -> -- dest
  Int -> -- start A
  Int -> -- end A
  Int -> -- start B
  Int -> -- end B
  Int -> -- start destination
  ST s ()
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Double -> C.MutablePrimArray s Double -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Int -> C.MutablePrimArray s Int -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Int64 -> C.MutablePrimArray s Int64 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Int32 -> C.MutablePrimArray s Int32 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Int16 -> C.MutablePrimArray s Int16 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Int8 -> C.MutablePrimArray s Int8 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Word -> C.MutablePrimArray s Word -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Word64 -> C.MutablePrimArray s Word64 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Word32 -> C.MutablePrimArray s Word32 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Word16 -> C.MutablePrimArray s Word16 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE mergeNonContiguous :: forall s. C.MutablePrimArray s Word8 -> C.MutablePrimArray s Word8 -> Int -> Int -> Int -> Int -> Int -> ST s () #-}
mergeNonContiguous !src !dst !startA !endA !startB !endB !startDst =
  if startB < endB
    then stepA startA startB startDst
    else
      if startA < endA
        then stepB startA startB startDst
        else return ()
 where
  continue :: Int -> Int -> Int -> ST s ()
  continue !ixA !ixB !ixDst = do
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
  stepB !ixA !ixB !ixDst =
    if ixB < endB
      then continue ixA ixB ixDst
      else finishA ixA ixDst
  stepA :: Int -> Int -> Int -> ST s ()
  stepA !ixA !ixB !ixDst =
    if ixA < endA
      then continue ixA ixB ixDst
      else finishB ixB ixDst
  finishB :: Int -> Int -> ST s ()
  finishB !ixB !ixDst = C.copyMut dst ixDst (C.sliceMut src ixB (endB - ixB))
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = C.copyMut dst ixDst (C.sliceMut src ixA (endA - ixA))

mergeNonContiguousTagged ::
  forall karr varr k v s.
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  Mutable karr s k -> -- source
  Mutable karr s k -> -- dest
  Mutable varr s v -> -- source tags
  Mutable varr s v -> -- dest tags
  Int -> -- start A
  Int -> -- end A
  Int -> -- start B
  Int -> -- end B
  Int -> -- start destination
  ST s ()
{-# INLINEABLE mergeNonContiguousTagged #-}
mergeNonContiguousTagged !src !dst !srcTags !dstTags !startA !endA !startB !endB !startDst =
  if startB < endB
    then stepA startA startB startDst
    else
      if startA < endA
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
  stepB !ixA !ixB !ixDst =
    if ixB < endB
      then continue ixA ixB ixDst
      else finishA ixA ixDst
  stepA :: Int -> Int -> Int -> ST s ()
  stepA !ixA !ixB !ixDst =
    if ixA < endA
      then continue ixA ixB ixDst
      else finishB ixB ixDst
  finishB :: Int -> Int -> ST s ()
  finishB !ixB !ixDst = do
    C.copyMut dst ixDst (C.sliceMut src ixB (endB - ixB))
    C.copyMut dstTags ixDst (C.sliceMut srcTags ixB (endB - ixB))
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = do
    C.copyMut dst ixDst (C.sliceMut src ixA (endA - ixA))
    C.copyMut dstTags ixDst (C.sliceMut srcTags ixA (endA - ixA))

threshold :: Int
threshold = 16

thresholdTagged :: Int
thresholdTagged = 16

insertionSortRange ::
  forall s a.
  (Prim a, Ord a) =>
  MutablePrimArray s a ->
  Int -> -- start
  Int -> -- end
  ST s ()
{-# INLINEABLE insertionSortRange #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Double -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Int64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Int32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Int16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Int8 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Word -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Word64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Word32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Word16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertionSortRange :: forall s. C.MutablePrimArray s Word8 -> Int -> Int -> ST s () #-}
insertionSortRange !arr !start !end = go start
 where
  go :: Int -> ST s ()
  go !ix =
    if ix < end
      then do
        !a <- C.read arr ix
        insertElement arr (a :: a) start ix
        go (ix + 1)
      else return ()

insertElement ::
  forall arr s a.
  (Contiguous arr, Element arr a, Ord a) =>
  Mutable arr s a ->
  a ->
  Int ->
  Int ->
  ST s ()
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Double -> Double -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Int -> Int -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Int64 -> Int64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Int32 -> Int32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Int16 -> Int16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Int8 -> Int8 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Word -> Word -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Word64 -> Word64 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Word32 -> Word32 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Word16 -> Word16 -> Int -> Int -> ST s () #-}
{-# SPECIALIZE insertElement :: forall s. C.MutablePrimArray s Word8 -> Word8 -> Int -> Int -> ST s () #-}
insertElement !arr !a !start !end = go end
 where
  go :: Int -> ST s ()
  go !ix =
    if ix > start
      then do
        !b <- C.read arr (ix - 1)
        if b <= a
          then do
            C.copyMut arr (ix + 1) (C.sliceMut arr ix (end - ix))
            C.write arr ix a
          else go (ix - 1)
      else do
        C.copyMut arr (ix + 1) (C.sliceMut arr ix (end - ix))
        C.write arr ix a

insertionSortTaggedRange ::
  forall karr varr s k v.
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  Mutable karr s k ->
  Mutable varr s v ->
  Int -> -- start
  Int -> -- end
  ST s ()
{-# INLINEABLE insertionSortTaggedRange #-}
insertionSortTaggedRange !karr !varr !start !end = go start
 where
  go :: Int -> ST s ()
  go !ix =
    if ix < end
      then do
        !a <- C.read karr ix
        !v <- C.read varr ix
        insertElementTagged karr varr a v start ix
        go (ix + 1)
      else return ()

insertElementTagged ::
  forall karr varr s k v.
  (Contiguous karr, Element karr k, Ord k, Contiguous varr, Element varr v) =>
  Mutable karr s k ->
  Mutable varr s v ->
  k ->
  v ->
  Int ->
  Int ->
  ST s ()
{-# INLINEABLE insertElementTagged #-}
insertElementTagged !karr !varr !a !v !start !end = go end
 where
  go :: Int -> ST s ()
  go !ix =
    if ix > start
      then do
        !b <- C.read karr (ix - 1)
        if b <= a
          then do
            C.copyMut karr (ix + 1) (C.sliceMut karr ix (end - ix))
            C.write karr ix a
            C.copyMut varr (ix + 1) (C.sliceMut varr ix (end - ix))
            C.write varr ix v
          else go (ix - 1)
      else do
        C.copyMut karr (ix + 1) (C.sliceMut karr ix (end - ix))
        C.write karr ix a
        C.copyMut varr (ix + 1) (C.sliceMut varr ix (end - ix))
        C.write varr ix v
