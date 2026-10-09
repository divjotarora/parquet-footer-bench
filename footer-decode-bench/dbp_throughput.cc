// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include "footer_formats.h"

namespace {

constexpr int kLegacyRegionMapOffsets = 1;

std::string ReadWhole(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input)
    throw std::runtime_error("cannot open " + path);
  return std::string((std::istreambuf_iterator<char>(input)),
                     std::istreambuf_iterator<char>());
}

fdb::ArrayPage ReadRegionOffsets(const std::string &file) {
  constexpr size_t kTrailerSize = 36;
  if (file.size() < kTrailerSize ||
      std::memcmp(file.data() + file.size() - 4, "PMF1", 4) != 0)
    throw std::runtime_error("page footer missing PMF1 trailer");

  int64_t map_length = 0;
  int64_t root_length = 0;
  const char *trailer = file.data() + file.size() - kTrailerSize;
  std::memcpy(&map_length, trailer + 8, 8);
  std::memcpy(&root_length, trailer + 24, 8);
  if (map_length <= 0 || root_length <= 0 ||
      static_cast<uint64_t>(map_length + root_length) >
          file.size() - kTrailerSize)
    throw std::runtime_error("invalid PMF1 trailer lengths");

  size_t map_position = file.size() - kTrailerSize -
                        static_cast<size_t>(root_length + map_length);
  fdb::Reader reader(file.data() + map_position,
                     static_cast<size_t>(map_length));
  for (fdb::Reader::Field field = reader.NextField(); field.type != fdb::T_STOP;
       field = reader.NextField()) {
    if (field.id == kLegacyRegionMapOffsets && field.type == fdb::T_STRUCT)
      return fdb::ParseArrayPage(reader);
    reader.Skip(field.type);
  }
  throw std::runtime_error("region map has no offsets");
}

void Run(const std::string &path) {
  std::string file = ReadWhole(path);
  fdb::ArrayPage offsets = ReadRegionOffsets(file);
  std::vector<int64_t> output(offsets.num_values);
  size_t output_size = 0;
  const uint8_t *data = reinterpret_cast<const uint8_t *>(offsets.values.data);
  if (fdb::PfbRuntimeDbpDecodeInt64(data, offsets.values.size, output.data(),
                                    output.size(), &output_size) != 0 ||
      output_size != output.size())
    throw std::runtime_error("runtime DBP decoder rejected region map");

  using Clock = std::chrono::steady_clock;
  int64_t iterations = 1;
  double seconds = 0;
  for (;;) {
    auto start = Clock::now();
    for (int64_t iteration = 0; iteration < iterations; ++iteration) {
      if (fdb::PfbRuntimeDbpDecodeInt64(data, offsets.values.size,
                                        output.data(), output.size(),
                                        &output_size) != 0)
        std::abort();
    }
    seconds = std::chrono::duration<double>(Clock::now() - start).count();
    if (seconds >= 1.0)
      break;
    iterations *= 2;
  }

  double ns_per_op = seconds * 1e9 / iterations;
  double values_per_second = offsets.num_values * 1e9 / ns_per_op;
  double bytes_per_second = offsets.values.size * 1e9 / ns_per_op;
  std::printf("%s,%d,%u,%.1f,%.3f,%.3f\n", path.c_str(), offsets.num_values,
              offsets.values.size, ns_per_op, values_per_second / 1e6,
              bytes_per_second / 1e9);
}

} // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s file.page-modular...\n", argv[0]);
    return 2;
  }
  try {
    std::printf("file,offset_count,encoded_bytes,ns_per_op,million_offsets_per_"
                "s,encoded_GB_per_s\n");
    for (int argument = 1; argument < argc; ++argument)
      Run(argv[argument]);
    return 0;
  } catch (const std::exception &error) {
    std::fprintf(stderr, "error: %s\n", error.what());
    return 1;
  }
}
