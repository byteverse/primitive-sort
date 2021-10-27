# primitive-sort

Sorting of contiguous data structures. Implementation uses mergesort
and switches to insertion sort once the arrays are small. Some of the
benchmark results on a Intel Xeon CPU E3-1505M v5 2.80GHz (GHC 8.10.4):

    contiguous/Int8/unsorted/mini      time  91.59 ns  
    contiguous/Int8/unsorted/tiny      time  1.236 μs  
    contiguous/Int8/unsorted/small     time  40.68 μs  
    contiguous/Int8/unsorted/medium    time  614.1 μs  
    contiguous/Int8/unsorted/large     time  6.580 ms  
    contiguous/Int8/unsorted/gigantic  time  68.16 ms  

Results may vary across processors, but you observe results that are 10x
slower, it's probably an issue with the compiler, and please open an issue.

## GHC Specialization Problems

Sadly, the most permissive signature for `sort` causes GHC's specialization
to break. This permissive signature is:

    sort :: (Contiguous arr, Element arr a, Ord a) => arr a -> arr a

And the less permissive variant used by this library is:

    sort :: (Prim a, Ord a) => PrimArray a -> PrimArray a

The latter works nicely with `SPECIALIZE` and `INLINEABLE`, but the former
does not work at all with these. The issue is believed to be related to the
use of an associated constraint type `Element`, which introduced coercions
that might confuse the specializer. However, this has not been confirmed.
