{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{-# OPTIONS_GHC -O2 -Wall #-}

module Sort.Merge.Indef
  ( sort
  ) where

import Element
import qualified Element as E
import Control.Monad.ST
import GHC.ST (ST(..))
import GHC.IO (IO(..))
import Data.Primitive (ByteArray,MutableByteArray,unsafeFreezeByteArray)
import Control.Monad
import GHC.Prim
import Control.Applicative (liftA2)
import Control.Concurrent (getNumCapabilities)
import qualified Data.Vector as V

sort :: ByteArray -> ByteArray
sort src = runST $ do
  let len = sizeofByteArray src
  dst <- newByteArray len
  work <- newByteArray len
  -- if odd (logBase2 len) && len > threshold
  --   then copyByteArray work 0 src 0 len 
  --   else copyByteArray dst 0 src 0 len
  copyByteArray work 0 src 0 len 
  copyByteArray dst 0 src 0 len
  caps <- unsafeEmbedIO getNumCapabilities
  let minElemsPerThread = 20000
      maxThreads = div len minElemsPerThread
      preThreads = min caps maxThreads
      -- threads = preThreads * 8
      threads = if preThreads == 1 then 1 else preThreads * 8
  -- I cannot understand why, but GHC's runtime does better
  -- when we let this schedule 8 times as many threads as
  -- we have capabilities. However, we only get this benefit
  -- when we actually have more than one capability.
  splitMergeParallel dst work threads 0 len
  unsafeFreezeByteArray dst

-- this could be way faster
logBase2 :: Int -> Int
logBase2 = ceiling . logBase 2.0 . (fromIntegral :: Int -> Double)

unsafeEmbedIO :: IO a -> ST s a
unsafeEmbedIO (IO f) = ST (unsafeCoerce# f)

mergesortInPlace :: MutableByteArray s -> ST s ()
mergesortInPlace arr = do
  len <- getSizeofMutableByteArray arr
  work <- newByteArray len
  copyMutableByteArray work 0 arr 0 len
  splitMerge arr work 0 len

half :: Int -> Int
half x = div x 2

splitMergeParallel :: 
     MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> Int -- spark limit, should be power of two
  -> Int -- start
  -> Int -- end
  -> ST s ()
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
      -- merge work arr start mid end
  else splitMerge arr work start end

replicateIxed :: Monad m => Int -> (Int -> m a) -> m ()
replicateIxed !n f = go (n - 1) where
  go !ix = if ix >= 0
    then f ix >> go (ix - 1)
    else return ()

splitMerge :: 
     MutableByteArray s -- source and destination
  -> MutableByteArray s -- work array
  -> Int -- start
  -> Int -- end
  -> ST s ()
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

mergeParallel ::
     MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- threads
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
mergeParallel !src !dst !threads !start !mid !end = do
  v <- V.unfoldrNM threads (\x -> case x of
      BuildBegin -> do
        !endElem <- readByteArray src (start + chunk) 
        !endA <- findIndexOfGtElem src endElem start mid
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
                 !endA <- findIndexOfGtElem src endElem prevEndA mid
                 !endB <- findIndexOfGtElem src endElem prevEndB end
                 return (Just (Indices prevEndA endA prevEndB endB, BuildStep endA endB (ix + 1)))
    ) BuildBegin
  forConcurrently_ v $ \(Indices startA endA startB endB) -> do
    let !startDst = (startA - start) + (startB - mid) + start
    mergeNonContiguous src dst startA endA startB endB startDst
    -- replicateIxed threads $ \ix -> do
    --   let !padding = ix * chunk
    --       !startChunk = ix * chunk
    --       !endChunk = startChunk + chunk
    --   !startElem <- readByteArray src startChunk
    --   !possibleStartA <- findIndexOfGtElem src startElem start mid
    --   !possibleStartB <- findIndexOfGtElem src startElem mid end
    --   let !startA = if ix == 0
    --         then start
    --         else possibleStartA
    --       !startB = if ix == 0
    --         then mid
    --         else possibleStartB
    --   (!endA,!endB) <- if ix == threads - 1
    --     then return (mid,end)
    --     else do
    --       !endElem <- readByteArray src endChunk 
    --       liftA2 (,) (findIndexOfGtElem src endElem start mid) (findIndexOfGtElem src endElem mid end)
  where
  !chunk = div (end - start) threads

-- This is not really a rounded up div, but it will work.
divRoundedUp :: Int -> Int -> Int
divRoundedUp a b = div a b + 1

-- If the needle is bigger than everything in the slice
-- of the array, this returns the end index (which is out
-- of bounds). Callers of this function should be able
-- to handle that.
findIndexOfGtElem :: forall s. MutableByteArray s -> T -> Int -> Int -> ST s Int
findIndexOfGtElem !v !needle !start !end = go start end
  where
  go :: Int -> Int -> ST s Int
  go !lo !hi = if lo < hi
    then do
      let !mid = lo + half (hi - lo)
      !val <- readByteArray v mid
      if | eq val needle -> gallopToGtIndex v needle (mid + 1) hi
         | lt val needle -> go (mid + 1) hi
         | otherwise -> go lo mid
    else return lo

-- | TODO: should probably turn this into a real galloping search
gallopToGtIndex :: forall s. MutableByteArray s -> T -> Int -> Int -> ST s Int
gallopToGtIndex !v !val !start !end = go start
  where
  go :: Int -> ST s Int
  go !ix = if ix < end
    then do
      !a <- readByteArray v ix
      if gt a val
        then return ix
        else go (ix + 1)
    else return end

-- stepA assumes that we previously incremented ixA.
-- Consequently, we do not need to check that ixB
-- is still in bounds. As a precondition, both
-- indices are guarenteed to start in bounds.
merge :: forall s.
     MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- start
  -> Int -- middle
  -> Int -- end
  -> ST s ()
merge !src !dst !start !mid !end = mergeNonContiguous src dst start mid mid end start

mergeNonContiguous :: forall s.
     MutableByteArray s -- source
  -> MutableByteArray s -- dest
  -> Int -- start A
  -> Int -- end A
  -> Int -- start B
  -> Int -- end B
  -> Int -- start destination
  -> ST s ()
mergeNonContiguous !src !dst !startA !endA !startB !endB !startDst =
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
    if lte a b
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
  finishB !ixB !ixDst = copyMutableByteArray dst ixDst src ixB (endB - ixB)
  finishA :: Int -> Int -> ST s ()
  finishA !ixA !ixDst = copyMutableByteArray dst ixDst src ixA (endA - ixA)

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

insertionSortRange :: forall s.
     MutableByteArray s
  -> Int -- start
  -> Int -- end
  -> ST s ()
insertionSortRange !arr !start !end = go start
  where
  go :: Int -> ST s ()
  go !ix = if ix < end
    then do
      !a <- readByteArray arr ix
      insertElement arr a start ix
      go (ix + 1)
    else return ()
    
insertElement :: forall s. MutableByteArray s -> T -> Int -> Int -> ST s ()
insertElement !arr !a !start !end = go end
  where
  go :: Int -> ST s ()
  go !ix = if ix > start
    then do
      !b <- readByteArray arr (ix - 1)
      if lte b a
        then do
          copyMutableByteArray arr (ix + 1) arr ix (end - ix)
          writeByteArray arr ix a
        else go (ix - 1)
    else do
      copyMutableByteArray arr (ix + 1) arr ix (end - ix)
      writeByteArray arr ix a

forkST_ :: ST s a -> ST s ()
{-# INLINE forkST_ #-}
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

