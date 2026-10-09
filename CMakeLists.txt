# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16)
project(parquet_footer_bench CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra)

# The pfb layout harness (synthetic footer shapes). No third-party dependencies: the
# Thrift Compact codec is vendored in pfb-harness/thrift_compact.h. The harness headers
# live beside its sources in pfb-harness/; the shared bit packing is in include/pfb/.
add_library(pfb_layouts STATIC pfb-harness/layouts.cc)
target_include_directories(pfb_layouts PUBLIC include pfb-harness)

add_executable(pfb_bench pfb-harness/main.cc)
target_link_libraries(pfb_bench PRIVATE pfb_layouts)

# Footer -> modular-footer converter (reads a real Parquet footer). Uses a
# vendored compact codec + include/pfb/bitpack.h; no third-party dependencies.
add_executable(modular_footer_convert modular-footer/parquet_to_modular.cc)
target_include_directories(modular_footer_convert PRIVATE include)

add_executable(jumptable_footer_convert jumptable-footer/parquet_to_jumptable.cc)
add_executable(footer_decode_bench footer-decode-bench/footer_decode_bench.cc)
set(PFB_RUNTIME_DBP_LIBRARY "" CACHE FILEPATH "Runtime dbl/parquet DBP bridge shared library")
if(PFB_RUNTIME_DBP_LIBRARY)
  target_compile_definitions(footer_decode_bench PRIVATE PFB_HAVE_RUNTIME_DBP=1)
  target_link_libraries(footer_decode_bench PRIVATE "${PFB_RUNTIME_DBP_LIBRARY}")
  add_executable(dbp_throughput footer-decode-bench/dbp_throughput.cc)
  target_compile_definitions(dbp_throughput PRIVATE PFB_HAVE_RUNTIME_DBP=1)
  target_link_libraries(dbp_throughput PRIVATE "${PFB_RUNTIME_DBP_LIBRARY}")
endif()

# Name-resolution benchmark (walk the schema vs a persisted name hash) and the wide
# synthetic-footer generator that feeds it. The bench reads a jump-table footer or a
# modular footer carrying a SCHEMA_INDEX module.
add_executable(name_resolve_bench footer-decode-bench/name_resolve_bench.cc)
add_executable(make_wide_footer footer-decode-bench/make_wide_footer.cc)

# Reference reader + correctness check for the modular SCHEMA_INDEX module.
add_executable(modular_schema_index_check modular-footer/schema_index_check.cc)
target_include_directories(modular_schema_index_check PRIVATE include)

enable_testing()
add_executable(pfb_fidelity_test pfb-harness/fidelity_test.cc)
target_link_libraries(pfb_fidelity_test PRIVATE pfb_layouts)
add_test(NAME fidelity COMMAND pfb_fidelity_test)
