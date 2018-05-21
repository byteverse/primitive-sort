import Test.DocTest

main :: IO ()
main = doctest
  [ "src/Data/Primitive/Sort.hs"
  ]
