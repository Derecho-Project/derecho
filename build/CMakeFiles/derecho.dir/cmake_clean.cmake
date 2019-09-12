file(REMOVE_RECURSE
  "libderecho.pdb"
  "libderecho.so.0.9.1"
  "libderecho.so"
  "libderecho.so.0.9"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/derecho.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
