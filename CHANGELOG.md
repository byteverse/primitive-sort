# Revision history for primitive-sort

## 0.1.2.4 -- 2024-02-07

* Restore import for `liftA2` that caused tests to fail for GHC 9.4.

## 0.1.2.3 -- 2024-02-06

* Restore support for GHC 9.2 and GHC 9.4, which had been broken by
  a assuming that `liftA2` was available from the prelude.

## 0.1.2.2 -- 2024-02-02

* Replace imports of `GHC.Prim` with `GHC.Exts` so that the dependency
  on ghc-prim can be dropped.

## 0.1.2.1 -- 2024-02-01

* Updated package metadata.
