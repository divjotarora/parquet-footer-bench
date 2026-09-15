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

// parquet_to_modular: read a Parquet file's footer and translate it, in full,
// into the modular-footer layout (see ModularFooter.thrift), writing the modular
// bytes and a per-module size breakdown.
//
// Dependency-free, like the rest of this repo: a small Thrift Compact codec is
// vendored below (a superset of include/pfb/thrift_compact.h -- it also handles
// the binary/bool/i16/double/map/set wire types a real FileMetaData contains),
// and the bit packing comes from include/pfb/bitpack.h. Builds with the repo
// (CMakeLists.txt target modular_footer_convert) and any C++17 compiler.
//
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
//   ./build/modular_footer_convert [--page-index] [--full-file] [--schema-index]
//       input.parquet [output.modular]
//
// Translates every module the FileMetaData footer carries:
//   * schema             (copied verbatim -- read in full anyway)
//   * placement          (per-chunk offsets/sizes/codecs/types, dense BITSET)
//   * row-group stats     (per-column ColumnStatistics via a column_offsets
//                          directory; null counts and min/max as PRESENT_INDEX,
//                          min/max with common-prefix stripping)
//   * file metadata      (created_by + key_value_metadata)
//
// With --schema-index it also emits the optional SCHEMA_INDEX module: an
// open-addressed FNV-1a-64 name -> leaf-ordinal hash table (power-of-two slots at
// ~0.7 load, 8 discriminator bits) plus per-element byte offsets into the schema
// module, so a reader resolves K queried column names in O(K) instead of walking
// every SchemaElement. leaf_element_indexes is omitted for a flat schema (leaf
// ordinal c is element c + 1). Purely additive: readers that do not understand the
// kind, or footers written without the flag, fall back to walking the schema.
//
// With --page-index it also emits the OFFSET_INDEX and COLUMN_INDEX modules, but
// only when that page-index data is actually present and reachable: the blobs
// live at absolute file offsets before the footer, so this needs a full file
// (leading PAR1 magic) whose page-index ranges lie within it. Nothing is
// invented -- a bare tail, or a file without a page index, emits none.
//
// With --truncate-minmax[=N] (default N=16) the stored min/max suffixes are cut
// to at most N bytes: the min suffix to a prefix (still a valid lower bound) and
// the max suffix rounded up (still a valid upper bound), clearing the exactness
// bit when truncated. Applies to row-group statistics and, with --page-index,
// the column index.
//
// With --full-file the output remains a self-contained data file: everything
// before the original Thrift footer is copied verbatim, followed by the modular
// footer and a 20-byte trailer:
//
//   [unchanged PAR1 header and data pages][modular footer]
//   [modular_start: LE i64][root_offset: LE i64][MFP1]
//
// root_offset is relative to modular_start, just like module offsets in the
// root directory. Without --full-file, the existing metadata-only MFT1 format
// is retained.
//
// Core footer sections are ordered from low offsets toward the tail as:
//
//   [statistics][statistics directory][placement][schema][ModularFooter root]
//
// This puts the most important sections closest to the tail.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "pfb/bitpack.h"  // BitWidth, PackBits (fixed-width LSB-first packing)

namespace {

// ------------------------------------------------- Thrift Compact wire types
enum : uint8_t {
  T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
  T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10, T_MAP = 11,
  T_STRUCT = 12,
};

// Modular-footer ArrayEncoding values (ModularFooter.thrift).
enum : int32_t { ENC_BITSET = 0, ENC_PRESENT_INDEX = 1 };

// A forward cursor that can read every compact type (so it can navigate/skip a
// real FileMetaData) and hand back the raw byte span of a value it does not
// decode (used to copy the schema and key/value lists verbatim).
class Reader {
 public:
  Reader(const char* d, size_t n) : p_(d), end_(d + n) {}
  const char* cur() const { return p_; }
  bool ok() const { return ok_; }

  struct Field { int16_t id; uint8_t type; };
  Field NextField() {
    if (!Avail(1)) return Fail();
    uint8_t b = static_cast<uint8_t>(*p_++);
    if (b == 0) return {0, T_STOP};
    uint8_t type = b & 0x0F;
    int delta = b >> 4;
    int16_t id = delta ? static_cast<int16_t>(last_id_ + delta) : static_cast<int16_t>(ZigZag());
    last_id_ = id;
    return {id, type};
  }
  struct ListHdr { uint8_t elem; int32_t size; };
  ListHdr List() {
    if (!Avail(1)) { ok_ = false; return {0, 0}; }
    uint8_t b = static_cast<uint8_t>(*p_++);
    int32_t size = b >> 4;
    if (size == 15) size = static_cast<int32_t>(Varint());
    return {static_cast<uint8_t>(b & 0x0F), size};
  }
  int32_t I32() { return static_cast<int32_t>(ZigZag()); }
  int64_t I64() { return ZigZag(); }
  std::string Binary() {
    uint64_t n = Varint();
    if (!Avail(n)) { ok_ = false; return {}; }
    std::string s(p_, static_cast<size_t>(n));
    p_ += n;
    return s;
  }
  uint8_t Byte() { if (!Avail(1)) { ok_ = false; return 0; } return static_cast<uint8_t>(*p_++); }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }

  void Skip(uint8_t type) {
    switch (type) {
      case T_TRUE: case T_FALSE: break;
      case T_I8: Advance(1); break;
      case T_I16: case T_I32: case T_I64: Varint(); break;
      case T_DOUBLE: Advance(8); break;
      case T_BINARY: Advance(static_cast<size_t>(Varint())); break;
      case T_LIST: case T_SET: SkipList(); break;
      case T_MAP: {
        uint64_t n = Varint();
        if (n) {
          if (!Avail(1)) { ok_ = false; break; }
          uint8_t kv = static_cast<uint8_t>(*p_++);
          for (uint64_t i = 0; i < n && ok_; ++i) { Skip(kv >> 4); Skip(kv & 0x0F); }
        }
        break;
      }
      case T_STRUCT: {
        int16_t s = StructBegin();
        for (Field f = NextField(); f.type != T_STOP && ok_; f = NextField()) Skip(f.type);
        StructEnd(s);
        break;
      }
      default: ok_ = false; break;
    }
  }
  void SkipList() {
    ListHdr h = List();
    if (h.elem == T_TRUE || h.elem == T_FALSE) { Advance(static_cast<size_t>(h.size)); return; }
    for (int32_t i = 0; i < h.size && ok_; ++i) Skip(h.elem);
  }

 private:
  bool Avail(size_t n) const { return static_cast<size_t>(end_ - p_) >= n; }
  void Advance(size_t n) { if (Avail(n)) p_ += n; else ok_ = false; }
  Field Fail() { ok_ = false; return {0, T_STOP}; }
  uint64_t Varint() {
    uint64_t v = 0; int shift = 0;
    while (Avail(1) && shift < 64) {
      uint8_t b = static_cast<uint8_t>(*p_++);
      v |= static_cast<uint64_t>(b & 0x7F) << shift;
      if (!(b & 0x80)) return v;
      shift += 7;
    }
    ok_ = false; return v;
  }
  int64_t ZigZag() { uint64_t v = Varint(); return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); }

  const char* p_;
  const char* end_;
  int16_t last_id_ = 0;
  bool ok_ = true;
};

// Compact writer: field framing, i8/i32/i64, binary, lists, nested structs.
class Writer {
 public:
  void Field(int16_t id, uint8_t type) {
    int delta = id - last_id_;
    if (delta > 0 && delta <= 15) Byte(static_cast<uint8_t>((delta << 4) | type));
    else { Byte(type); ZigZag(id); }
    last_id_ = id;
  }
  void I8(int8_t v) { buf_.push_back(static_cast<char>(v)); }
  void I32(int32_t v) { ZigZag(v); }
  void I64(int64_t v) { ZigZag(v); }
  void Binary(int16_t id, const std::string& s) {
    Field(id, T_BINARY); Varint(s.size()); buf_.append(s);
  }
  void ListField(int16_t id, uint8_t elem, int32_t size) {
    Field(id, T_LIST);
    if (size <= 14) Byte(static_cast<uint8_t>((size << 4) | elem));
    else { Byte(static_cast<uint8_t>(0xF0 | elem)); Varint(static_cast<uint32_t>(size)); }
  }
  void AppendRaw(const char* d, size_t n) { buf_.append(d, n); }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }
  void Stop() { buf_.push_back(0); }
  const std::string& bytes() const { return buf_; }

 private:
  void Byte(uint8_t b) { buf_.push_back(static_cast<char>(b)); }
  void Varint(uint64_t v) {
    while (v >= 0x80) { buf_.push_back(static_cast<char>((v & 0x7F) | 0x80)); v >>= 7; }
    buf_.push_back(static_cast<char>(v));
  }
  void ZigZag(int64_t v) { Varint((static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63)); }

  std::string buf_;
  int16_t last_id_ = 0;
};

// ------------------------------------------------- EncodedArray emitters
static int Width(uint64_t maxval) { return pfb::BitWidth(maxval); }
static uint64_t Max(const std::vector<uint64_t>& v) {
  uint64_t m = 0;
  for (uint64_t x : v) m = x > m ? x : m;
  return m;
}

// Append an unsigned LEB128 varint to a raw byte string (num_present in `presence`).
static void Uleb(std::string& s, uint64_t v) {
  while (v >= 0x80) { s.push_back(static_cast<char>((v & 0x7F) | 0x80)); v >>= 7; }
  s.push_back(static_cast<char>(v));
}

// EncodedArray field ids: 1 num_values, 2 values, 3 encoding (omit for BITSET), 4 presence.
// Parameters (widths, counts) live inline in the byte blobs, not as Thrift fields.

// Dense BITSET integer/boolean EncodedArray: every position present.
//   num_values = N; values = [u8 value_bit_width] ++ PackBits(vals). encoding & presence omitted.
static void PutIntDense(Writer& w, int16_t fid, const std::vector<uint64_t>& vals) {
  int width = Width(Max(vals));
  std::string values(1, static_cast<char>(width));
  values += pfb::PackBits(vals, width);
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Field(1, T_I32); w.I32(static_cast<int32_t>(vals.size()));  // num_values
  w.Binary(2, values);                                          // values
  w.Stop(); w.StructEnd(s);
}

// PRESENT_INDEX integer EncodedArray.
//   values   = [u8 value_bit_width] ++ PackBits(values)
//   presence = ULEB128(num_present) ++ [u8 position_bit_width] ++ PackBits(positions)
static void PutIntSparse(Writer& w, int16_t fid, int32_t domain,
                         const std::vector<uint64_t>& positions,
                         const std::vector<uint64_t>& values) {
  int wpos = Width(domain > 0 ? static_cast<uint64_t>(domain - 1) : 0);
  int wval = Width(Max(values));
  std::string vblob(1, static_cast<char>(wval));
  vblob += pfb::PackBits(values, wval);
  std::string pblob;
  Uleb(pblob, positions.size());
  pblob.push_back(static_cast<char>(wpos));
  pblob += pfb::PackBits(positions, wpos);
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Field(1, T_I32); w.I32(domain);            // num_values (logical domain, not num_present)
  w.Binary(2, vblob);                          // values
  w.Field(3, T_I32); w.I32(ENC_PRESENT_INDEX); // encoding
  w.Binary(4, pblob);                          // presence
  w.Stop(); w.StructEnd(s);
}

// PRESENT_INDEX BYTE_ARRAY EncodedArray.
//   values   = [u8 offset_bit_width] ++ PackBits(cum[num_present+1]) ++ concatenated bytes
//   presence = ULEB128(num_present) ++ [u8 position_bit_width] ++ PackBits(positions)
static void PutBytesSparse(Writer& w, int16_t fid, int32_t domain,
                           const std::vector<uint64_t>& positions,
                           const std::vector<std::string>& values) {
  int wpos = Width(domain > 0 ? static_cast<uint64_t>(domain - 1) : 0);
  std::vector<uint64_t> cum(values.size() + 1, 0);
  for (size_t i = 0; i < values.size(); ++i) cum[i + 1] = cum[i] + values[i].size();
  int woff = Width(cum.back());
  std::string vblob(1, static_cast<char>(woff));
  vblob += pfb::PackBits(cum, woff);
  for (const std::string& v : values) vblob += v;
  std::string pblob;
  Uleb(pblob, positions.size());
  pblob.push_back(static_cast<char>(wpos));
  pblob += pfb::PackBits(positions, wpos);
  w.Field(fid, T_STRUCT);
  int16_t s = w.StructBegin();
  w.Field(1, T_I32); w.I32(domain);            // num_values
  w.Binary(2, vblob);                          // values
  w.Field(3, T_I32); w.I32(ENC_PRESENT_INDEX); // encoding
  w.Binary(4, pblob);                          // presence
  w.Stop(); w.StructEnd(s);
}

// ------------------------------------------------- parsed FileMetaData subset
struct Stat {
  bool has_null = false; int64_t null_count = 0;
  bool has_minmax = false; std::string minv, maxv;
  bool min_exact = true, max_exact = true;
};
struct Chunk {
  int64_t data_page_offset = 0, dict_offset = 0;
  int64_t total_compressed = 0, total_uncompressed = 0, num_values = 0;
  int32_t codec = 0, type = 0;
  bool has_dict = false;
  bool fully_dict = false;   // every data page dictionary-encoded (from encoding_stats)
  int64_t offset_index_offset = 0, column_index_offset = 0;  // absolute file offsets
  int32_t offset_index_length = 0, column_index_length = 0;
  bool has_offset_index = false, has_column_index = false;
  Stat stat;
};
struct RowGroup { int64_t num_rows = 0; std::vector<Chunk> columns; };
struct FileMeta {
  int32_t version = 1;
  int64_t num_rows = 0;
  const char* schema_span = nullptr; size_t schema_len = 0;   // list<SchemaElement> verbatim
  const char* kv_span = nullptr; size_t kv_len = 0;           // list<KeyValue> verbatim
  bool has_created_by = false; std::string created_by;
  std::vector<RowGroup> row_groups;
};

static Stat ParseStatistics(Reader& r) {
  Stat st;
  std::string min_dep, max_dep, min_val, max_val;
  bool has_min_dep = false, has_max_dep = false, has_min_val = false, has_max_val = false;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      case 1: max_dep = r.Binary(); has_max_dep = true; break;    // deprecated max
      case 2: min_dep = r.Binary(); has_min_dep = true; break;    // deprecated min
      case 3: st.null_count = r.I64(); st.has_null = true; break;
      case 5: max_val = r.Binary(); has_max_val = true; break;    // max_value (current)
      case 6: min_val = r.Binary(); has_min_val = true; break;    // min_value (current)
      case 7: st.max_exact = (f.type == T_TRUE); break;           // bool: value in header
      case 8: st.min_exact = (f.type == T_TRUE); break;
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  const bool have_min = has_min_val || has_min_dep;
  const bool have_max = has_max_val || has_max_dep;
  if (have_min && have_max) {
    st.has_minmax = true;
    st.minv = has_min_val ? min_val : min_dep;
    st.maxv = has_max_val ? max_val : max_dep;
  }
  return st;
}

static Chunk ParseColumnMetaData(Reader& r) {
  Chunk c;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      case 1:  c.type = r.I32(); break;
      case 4:  c.codec = r.I32(); break;
      case 5:  c.num_values = r.I64(); break;
      case 6:  c.total_uncompressed = r.I64(); break;
      case 7:  c.total_compressed = r.I64(); break;
      case 9:  c.data_page_offset = r.I64(); break;
      case 11: c.dict_offset = r.I64(); c.has_dict = true; break;
      case 12: c.stat = ParseStatistics(r); break;
      case 13: {  // encoding_stats: list<PageEncodingStats{1:page_type,2:encoding,3:count}>
        Reader::ListHdr h = r.List();
        bool any_data = false, all_dict = true;
        for (int32_t i = 0; i < h.size && r.ok(); ++i) {
          int32_t pt = 0, enc = 0;
          int16_t ss = r.StructBegin();
          for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
            if (g.id == 1) pt = r.I32();
            else if (g.id == 2) enc = r.I32();
            else r.Skip(g.type);
          }
          r.StructEnd(ss);
          if (pt == 0 || pt == 3) {                 // DATA_PAGE / DATA_PAGE_V2
            any_data = true;
            if (enc != 2 && enc != 8) all_dict = false;  // PLAIN_DICTIONARY=2, RLE_DICTIONARY=8
          }
        }
        c.fully_dict = any_data && all_dict;
        break;
      }
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return c;
}

static Chunk ParseColumnChunk(Reader& r) {
  Chunk c;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      // meta_data (field 3) precedes the index locators, so assigning c here is safe.
      case 3: if (f.type == T_STRUCT) c = ParseColumnMetaData(r); else r.Skip(f.type); break;
      case 4: c.offset_index_offset = r.I64(); c.has_offset_index = true; break;
      case 5: c.offset_index_length = r.I32(); break;
      case 6: c.column_index_offset = r.I64(); c.has_column_index = true; break;
      case 7: c.column_index_length = r.I32(); break;
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return c;
}

static RowGroup ParseRowGroup(Reader& r) {
  RowGroup rg;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_LIST) {
      Reader::ListHdr h = r.List();
      rg.columns.reserve(h.size);
      for (int32_t i = 0; i < h.size && r.ok(); ++i) rg.columns.push_back(ParseColumnChunk(r));
    } else if (f.id == 3 && f.type == T_I64) {
      rg.num_rows = r.I64();
    } else {
      r.Skip(f.type);
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return rg;
}

static FileMeta ParseFileMetaData(Reader& r) {
  FileMeta fm;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_I32) {
      fm.version = r.I32();
    } else if (f.id == 2 && f.type == T_LIST) {                  // schema (verbatim)
      fm.schema_span = r.cur(); r.SkipList(); fm.schema_len = static_cast<size_t>(r.cur() - fm.schema_span);
    } else if (f.id == 3 && f.type == T_I64) {
      fm.num_rows = r.I64();
    } else if (f.id == 4 && f.type == T_LIST) {                  // row_groups
      Reader::ListHdr h = r.List();
      fm.row_groups.reserve(h.size);
      for (int32_t i = 0; i < h.size && r.ok(); ++i) fm.row_groups.push_back(ParseRowGroup(r));
    } else if (f.id == 5 && f.type == T_LIST) {                  // key_value_metadata (verbatim)
      fm.kv_span = r.cur(); r.SkipList(); fm.kv_len = static_cast<size_t>(r.cur() - fm.kv_span);
    } else if (f.id == 6 && f.type == T_BINARY) {                // created_by
      fm.created_by = r.Binary(); fm.has_created_by = true;
    } else {
      r.Skip(f.type);
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  return fm;
}

static size_t CommonPrefix(const std::string& a, const std::string& b) {
  size_t n = a.size() < b.size() ? a.size() : b.size(), i = 0;
  while (i < n && a[i] == b[i]) ++i;
  return i;
}

// Smallest byte string > p that shares p's leading bytes: increment the last
// byte below 0xFF and drop the rest. Empty result means p is all 0xFF (no finite
// rounded-up bound exists).
static std::string RoundUp(std::string p) {
  for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(p[i]) != 0xFF) {
      p[i] = static_cast<char>(static_cast<uint8_t>(p[i]) + 1);
      p.resize(i + 1);
      return p;
    }
  }
  return std::string();
}

// Truncate a (prefix-stripped) min/max suffix pair to at most `limit` bytes,
// preserving pruning bounds: the min suffix is cut to a prefix (still <= the true
// min), the max suffix is cut and rounded up (still >= the true max). Clears the
// corresponding exactness flag when a value is actually truncated. limit <= 0
// disables truncation.
static void TruncateSuffix(std::string& min_suf, std::string& max_suf,
                           uint64_t& min_exact, uint64_t& max_exact, int limit) {
  if (limit <= 0) return;
  if (static_cast<int>(min_suf.size()) > limit) {
    min_suf.resize(limit);
    min_exact = 0;
  }
  if (static_cast<int>(max_suf.size()) > limit) {
    std::string up = RoundUp(max_suf.substr(0, limit));
    if (!up.empty()) { max_suf = up; max_exact = 0; }  // else keep the exact max
  }
}

// Build one leaf column's ColumnStatistics descriptor across row groups; empty
// string if the column has no statistics at all.
static std::string BuildColumnStatistics(const FileMeta& fm, int c, int G, int trunc) {
  std::vector<uint64_t> null_pos, null_val;
  std::vector<uint64_t> mm_pos, min_ex, max_ex;
  std::vector<std::string> pref, min_suf, max_suf;
  for (int g = 0; g < G; ++g) {
    const Stat& st = fm.row_groups[g].columns[c].stat;
    if (st.has_null) { null_pos.push_back(g); null_val.push_back(static_cast<uint64_t>(st.null_count)); }
    if (st.has_minmax) {
      size_t lcp = CommonPrefix(st.minv, st.maxv);
      std::string ms = st.minv.substr(lcp), xs = st.maxv.substr(lcp);
      uint64_t me = st.min_exact ? 1 : 0, xe = st.max_exact ? 1 : 0;
      TruncateSuffix(ms, xs, me, xe, trunc);
      mm_pos.push_back(g);
      pref.push_back(st.minv.substr(0, lcp));
      min_suf.push_back(ms);
      max_suf.push_back(xs);
      min_ex.push_back(me);
      max_ex.push_back(xe);
    }
  }
  if (null_pos.empty() && mm_pos.empty()) return {};
  Writer d;
  if (!null_pos.empty()) PutIntSparse(d, 1, G, null_pos, null_val);          // null_counts
  if (!mm_pos.empty()) {
    PutBytesSparse(d, 2, G, mm_pos, pref);                                    // minmax_prefixes
    PutBytesSparse(d, 3, G, mm_pos, min_suf);                                 // min_suffixes
    PutBytesSparse(d, 4, G, mm_pos, max_suf);                                 // max_suffixes
    bool any_inexact = false;
    for (size_t i = 0; i < mm_pos.size(); ++i)
      if (!min_ex[i] || !max_ex[i]) { any_inexact = true; break; }
    if (any_inexact) {  // absent exactness arrays mean every present bound is exact
      PutIntSparse(d, 5, G, mm_pos, min_ex);                                  // min_is_exact
      PutIntSparse(d, 6, G, mm_pos, max_ex);                                  // max_is_exact
    }
  }
  d.Stop();
  return d.bytes();
}

static std::string ReadRange(std::ifstream& in, int64_t off, int32_t len) {
  std::string s(static_cast<size_t>(len), '\0');
  in.seekg(off);
  in.read(&s[0], len);
  if (!in) throw std::runtime_error("failed reading page-index range");
  return s;
}

// Parse an OffsetIndex blob (list<PageLocation{offset,compressed_page_size,
// first_row_index}>) into an OffsetIndexChunk descriptor (dense per-page arrays).
static std::string BuildOffsetIndexChunk(const std::string& blob) {
  Reader r(blob.data(), blob.size());
  std::vector<uint64_t> offsets, sizes, rows;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_LIST) {
      Reader::ListHdr h = r.List();
      for (int32_t i = 0; i < h.size && r.ok(); ++i) {
        int64_t off = 0, row = 0; int32_t sz = 0;
        int16_t ss = r.StructBegin();
        for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
          if (g.id == 1) off = r.I64();
          else if (g.id == 2) sz = r.I32();
          else if (g.id == 3) row = r.I64();
          else r.Skip(g.type);
        }
        r.StructEnd(ss);
        offsets.push_back(static_cast<uint64_t>(off));
        sizes.push_back(static_cast<uint64_t>(sz));
        rows.push_back(static_cast<uint64_t>(row));
      }
    } else {
      r.Skip(f.type);
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);
  Writer w;
  PutIntDense(w, 1, offsets);
  PutIntDense(w, 2, sizes);
  PutIntDense(w, 3, rows);
  w.Stop();
  return w.bytes();
}

// Parse a ColumnIndex blob (null_pages, min_values, max_values, boundary_order,
// optional null_counts) into a ColumnIndexChunk descriptor. Min/max are present
// only for non-null pages and use common-prefix stripping (PRESENT_INDEX).
static std::string BuildColumnIndexChunk(const std::string& blob, int trunc) {
  Reader r(blob.data(), blob.size());
  std::vector<uint64_t> null_pages;                 // 0/1 per page
  std::vector<std::string> mins, maxs;
  std::vector<uint64_t> null_counts;
  bool has_null_counts = false;
  int32_t boundary_order = 0;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    switch (f.id) {
      case 1: {  // null_pages: list<bool> (one byte per element: 1=true, 2=false)
        Reader::ListHdr h = r.List();
        for (int32_t i = 0; i < h.size && r.ok(); ++i) null_pages.push_back(r.Byte() == 1 ? 1 : 0);
        break;
      }
      case 2: {  // min_values: list<binary>
        Reader::ListHdr h = r.List();
        for (int32_t i = 0; i < h.size && r.ok(); ++i) mins.push_back(r.Binary());
        break;
      }
      case 3: {  // max_values: list<binary>
        Reader::ListHdr h = r.List();
        for (int32_t i = 0; i < h.size && r.ok(); ++i) maxs.push_back(r.Binary());
        break;
      }
      case 4: boundary_order = r.I32(); break;
      case 5: {  // null_counts: list<i64>
        Reader::ListHdr h = r.List();
        for (int32_t i = 0; i < h.size && r.ok(); ++i) null_counts.push_back(static_cast<uint64_t>(r.I64()));
        has_null_counts = true;
        break;
      }
      default: r.Skip(f.type); break;
    }
    if (!r.ok()) break;
  }
  r.StructEnd(s);

  const int32_t P = static_cast<int32_t>(null_pages.size());
  std::vector<uint64_t> mm_pos, min_ex, max_ex;
  std::vector<std::string> pref, min_suf, max_suf;
  for (int32_t p = 0; p < P; ++p) {
    if (null_pages[p]) continue;                    // null page has no min/max
    if (p >= static_cast<int32_t>(mins.size()) || p >= static_cast<int32_t>(maxs.size())) break;
    size_t lcp = CommonPrefix(mins[p], maxs[p]);
    std::string ms = mins[p].substr(lcp), xs = maxs[p].substr(lcp);
    uint64_t me = 1, xe = 1;                         // ColumnIndex bounds are exact until truncated
    TruncateSuffix(ms, xs, me, xe, trunc);
    mm_pos.push_back(static_cast<uint64_t>(p));
    pref.push_back(mins[p].substr(0, lcp));
    min_suf.push_back(ms);
    max_suf.push_back(xs);
    min_ex.push_back(me);
    max_ex.push_back(xe);
  }

  Writer w;
  w.Field(1, T_I32); w.I32(boundary_order);         // boundary_order
  PutIntDense(w, 2, null_pages);                    // null_pages (dense bool)
  if (has_null_counts && !null_counts.empty()) {
    std::vector<uint64_t> pos(null_counts.size());
    for (size_t i = 0; i < pos.size(); ++i) pos[i] = i;
    PutIntSparse(w, 3, P, pos, null_counts);         // null_counts (per page)
  }
  if (!mm_pos.empty()) {
    PutBytesSparse(w, 4, P, mm_pos, pref);           // minmax_prefixes
    PutBytesSparse(w, 5, P, mm_pos, min_suf);        // min_suffixes
    PutBytesSparse(w, 6, P, mm_pos, max_suf);        // max_suffixes
    bool any_inexact = false;
    for (size_t i = 0; i < mm_pos.size(); ++i)
      if (!min_ex[i] || !max_ex[i]) { any_inexact = true; break; }
    if (any_inexact) {  // only stored when truncation made a bound inexact
      PutIntSparse(w, 7, P, mm_pos, min_ex);         // min_is_exact
      PutIntSparse(w, 8, P, mm_pos, max_ex);         // max_is_exact
    }
  }
  // 9 (nan_counts): not present in ColumnIndex.
  w.Stop();
  return w.bytes();
}

// Modular-footer ModuleKind values (ModularFooter.thrift).
enum : int32_t { K_SCHEMA = 0, K_PLACEMENT = 1, K_ROW_GROUP_STATISTICS = 2,
                 K_OFFSET_INDEX = 3, K_COLUMN_INDEX = 4, K_FILE_METADATA = 5,
                 K_SCHEMA_INDEX = 6 };
struct DirEntry { int32_t kind; int64_t off; int64_t len; };

// ------------------------------------------------- schema index (SchemaIndexModule)
// Per-element view of the verbatim schema list, in DFS order: each element's byte
// offset within the schema_span, its name, and num_children (0 == leaf).
struct SchemaLayout {
  std::vector<int64_t> span_off;
  std::vector<std::string> name;
  std::vector<int32_t> num_children;
};

static SchemaLayout ParseSchemaLayout(const char* span, size_t len) {
  SchemaLayout sl;
  Reader r(span, len);
  Reader::ListHdr h = r.List();               // span begins with the list header
  for (int32_t i = 0; i < h.size && r.ok(); ++i) {
    sl.span_off.push_back(static_cast<int64_t>(r.cur() - span));
    std::string nm; int32_t nc = 0;
    int16_t s = r.StructBegin();
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == 4 && f.type == T_BINARY) nm = r.Binary();       // name
      else if (f.id == 5 && f.type == T_I32) nc = r.I32();        // num_children
      else r.Skip(f.type);
    }
    r.StructEnd(s);
    sl.name.push_back(std::move(nm));
    sl.num_children.push_back(nc);
  }
  return sl;
}

static uint64_t Fnv1a64(const std::string& s) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (unsigned char c : s) { h ^= c; h *= 0x100000001b3ULL; }
  return h;
}
static uint64_t NextPow2(uint64_t n) { uint64_t p = 1; while (p < n) p <<= 1; return p; }

// Metrics reported for a built schema index.
struct SchemaIndexInfo { bool flat = false; int slots = 0; int slot_bits = 0; bool non_ascii = false; };

// Build a SchemaIndexModule from the schema layout. elements_base is the byte offset
// of the first schema element within the SchemaModule blob (its field-1 list header
// precedes the elements). Returns "" when there are no leaf columns to index.
static std::string BuildSchemaIndex(const SchemaLayout& sl, int64_t elements_base,
                                    SchemaIndexInfo* info) {
  const int N = static_cast<int>(sl.name.size());
  std::vector<int> parent(N, -1);             // DFS parent via the num_children stack
  {
    std::vector<std::pair<int, int>> st;
    for (int i = 0; i < N; ++i) {
      if (!st.empty()) { parent[i] = st.back().first; st.back().second--; }
      if (sl.num_children[i] > 0) st.push_back({i, sl.num_children[i]});
      while (!st.empty() && st.back().second == 0) st.pop_back();
    }
  }
  auto lower = [](std::string x) {
    for (char& c : x) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
    return x;
  };
  bool non_ascii = false;
  for (const std::string& nm : sl.name)
    for (unsigned char ch : nm) if (ch & 0x80) non_ascii = true;
  std::vector<std::string> path(N);           // lowercase NUL-joined path, root excluded
  for (int i = 1; i < N; ++i) {
    std::string seg = lower(sl.name[i]);
    int p = parent[i];
    path[i] = (p <= 0) ? seg : path[p] + std::string(1, '\0') + seg;
  }
  std::vector<int> leaf_elem;                 // leaf ordinal -> element index (DFS order)
  std::vector<uint64_t> leaf_hash;
  for (int i = 1; i < N; ++i)
    if (sl.num_children[i] == 0) { leaf_elem.push_back(i); leaf_hash.push_back(Fnv1a64(path[i])); }
  const int leaves = static_cast<int>(leaf_elem.size());
  if (leaves == 0) return {};

  const int disc_bits = 8;                    // top hash bits held in each slot
  const int ord_bits = pfb::BitWidth(static_cast<uint64_t>(leaves));  // holds leaf_ordinal + 1
  const uint64_t target = (static_cast<uint64_t>(leaves) * 10 + 6) / 7;  // ceil(leaves / 0.7)
  const uint64_t num_slots = NextPow2(target > 0 ? target : 1);
  std::vector<uint64_t> table(num_slots, 0);  // 0 == empty slot
  for (int ord = 0; ord < leaves; ++ord) {
    const uint64_t h = leaf_hash[ord];
    const uint64_t slotval =
        ((h >> (64 - disc_bits)) << ord_bits) | (static_cast<uint64_t>(ord) + 1);
    uint64_t idx = h & (num_slots - 1);
    while (table[idx] != 0) idx = (idx + 1) & (num_slots - 1);
    table[idx] = slotval;
  }
  const bool flat = (N == leaves + 1);        // only the root is a non-leaf

  Writer w;
  PutIntDense(w, 1, table);                   // hash_table (dense BITSET, UINT slots)
  w.Field(2, T_I8); w.I8(static_cast<int8_t>(disc_bits));
  w.Field(3, T_I8); w.I8(static_cast<int8_t>(ord_bits));
  std::vector<uint64_t> eoff(N);
  for (int i = 0; i < N; ++i) eoff[i] = static_cast<uint64_t>(elements_base + sl.span_off[i]);
  PutIntDense(w, 4, eoff);                    // element_offsets (relative to SchemaModule start)
  if (!flat) {
    std::vector<uint64_t> lei(leaves);
    for (int c = 0; c < leaves; ++c) lei[c] = static_cast<uint64_t>(leaf_elem[c]);
    PutIntDense(w, 5, lei);                   // leaf_element_indexes (omitted when flat)
    std::vector<uint64_t> par(N);             // element -> parent element (root and its children -> 0)
    for (int i = 0; i < N; ++i) par[i] = parent[i] > 0 ? static_cast<uint64_t>(parent[i]) : 0;
    PutIntDense(w, 6, par);                   // parent_ordinals (omitted when flat)
  }
  w.Field(7, non_ascii ? T_TRUE : T_FALSE);   // has_non_ascii_names
  w.Stop();

  if (info) {
    info->flat = flat;
    info->slots = static_cast<int>(num_slots);
    info->slot_bits = pfb::BitWidth(Max(table));
    info->non_ascii = non_ascii;
  }
  return w.bytes();
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::string> pos;
  bool emit_pi = false;
  bool emit_full_file = false;
  bool emit_si = false;  // schema index: opt-in, off by default
  int trunc = 0;  // max min/max suffix bytes; 0 = no truncation
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--page-index" || a == "-p") emit_pi = true;
    else if (a == "--full-file") emit_full_file = true;
    else if (a == "--schema-index" || a == "-s") emit_si = true;
    else if (a == "--truncate-minmax") trunc = 16;
    else if (a.rfind("--truncate-minmax=", 0) == 0) trunc = std::atoi(a.c_str() + 18);
    else pos.push_back(a);
  }
  if (pos.empty()) {
    std::fprintf(stderr,
        "usage: %s [--page-index] [--full-file] [--schema-index] "
        "[--truncate-minmax[=N]] input.parquet [output.modular]\n",
        argv[0]);
    return 2;
  }
  const std::string in_path = pos[0];
  const std::string out_path = pos.size() >= 2 ? pos[1] : in_path + ".modular";
  try {
    std::ifstream in(in_path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + in_path);
    in.seekg(0, std::ios::end);
    std::streamoff fsize = in.tellg();
    if (fsize < 8) throw std::runtime_error("file too small to be Parquet");
    char head[4] = {0};
    in.seekg(0); in.read(head, 4);
    const bool full_file = std::memcmp(head, "PAR1", 4) == 0;  // else a bare tail
    if (emit_full_file && !full_file)
      throw std::runtime_error("--full-file requires a complete Parquet input");
    char tail[8];
    in.seekg(fsize - 8); in.read(tail, 8);
    if (std::memcmp(tail + 4, "PAR1", 4) != 0)
      throw std::runtime_error("missing PAR1 magic (encrypted or not a Parquet file)");
    uint32_t footer_len = static_cast<uint8_t>(tail[0]) | (static_cast<uint8_t>(tail[1]) << 8) |
                          (static_cast<uint8_t>(tail[2]) << 16) |
                          (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
    if (static_cast<std::streamoff>(footer_len) + 8 > fsize)
      throw std::runtime_error("footer length exceeds file size");
    const int64_t footer_start = fsize - 8 - static_cast<std::streamoff>(footer_len);
    const size_t oss_footer_bytes = footer_len;
    std::string footer(footer_len, '\0');
    in.seekg(footer_start); in.read(&footer[0], footer_len);
    if (!in) throw std::runtime_error("short read of footer");

    Reader r(footer.data(), footer.size());
    FileMeta fm = ParseFileMetaData(r);
    if (!r.ok()) throw std::runtime_error("failed to parse FileMetaData footer");
    if (fm.row_groups.empty()) throw std::runtime_error("footer has no row groups");
    const int G = static_cast<int>(fm.row_groups.size());
    const int C = static_cast<int>(fm.row_groups[0].columns.size());
    const int64_t N = static_cast<int64_t>(C) * G;

    // ---- placement, column-major: chunk (column c, row group g) is at c*G + g.
    std::vector<uint64_t> dpo(N), tcs(N), tus(N), nv(N), cod(N), fdict_flag(N, 0);
    std::vector<uint64_t> row_group_num_rows(G);
    std::vector<uint64_t> ptypes(C), first_dict(N + 1, 0), dict_off;
    for (int g = 0; g < G; ++g)
      row_group_num_rows[g] = static_cast<uint64_t>(fm.row_groups[g].num_rows);
    for (int c = 0; c < C; ++c) {
      ptypes[c] = static_cast<uint64_t>(fm.row_groups[0].columns[c].type);
      for (int g = 0; g < G; ++g) {
        if (c >= static_cast<int>(fm.row_groups[g].columns.size()))
          throw std::runtime_error("ragged row groups (column count differs)");
        const Chunk& ch = fm.row_groups[g].columns[c];
        int64_t idx = static_cast<int64_t>(c) * G + g;
        dpo[idx] = static_cast<uint64_t>(ch.data_page_offset);
        tcs[idx] = static_cast<uint64_t>(ch.total_compressed);
        tus[idx] = static_cast<uint64_t>(ch.total_uncompressed);
        nv[idx] = static_cast<uint64_t>(ch.num_values);
        cod[idx] = static_cast<uint64_t>(ch.codec);
        first_dict[idx + 1] = first_dict[idx] + (ch.has_dict ? 1 : 0);
        if (ch.has_dict) dict_off.push_back(static_cast<uint64_t>(ch.dict_offset));
        fdict_flag[idx] = ch.fully_dict ? 1 : 0;
      }
    }
    Writer placement;
    PutIntDense(placement, 1, dpo);
    PutIntDense(placement, 2, first_dict);
    PutIntDense(placement, 3, dict_off);
    PutIntDense(placement, 4, tcs);
    PutIntDense(placement, 5, tus);
    PutIntDense(placement, 6, nv);
    PutIntDense(placement, 7, cod);
    PutIntDense(placement, 8, ptypes);
    PutIntDense(placement, 9, fdict_flag);  // is_fully_dictionary_encoded
    PutIntDense(placement, 10, row_group_num_rows);
    placement.Stop();

    // ---- schema: copy the parsed list<SchemaElement> verbatim under field 1.
    Writer schema;
    schema.Field(1, T_LIST);
    if (fm.schema_span) schema.AppendRaw(fm.schema_span, fm.schema_len);
    else schema.AppendRaw("\x0c", 1);        // empty list<struct>
    schema.Stop();

    // ---- schema index (optional, --schema-index): name -> leaf-ordinal hash plus
    // per-element offsets into the schema module. Field(1, T_LIST) above is one byte,
    // so the first schema element sits at offset 1 within the SchemaModule blob.
    std::string schema_index;
    bool have_schema_index = false;
    SchemaIndexInfo si_info;
    if (emit_si && fm.schema_span) {
      SchemaLayout sl = ParseSchemaLayout(fm.schema_span, fm.schema_len);
      schema_index = BuildSchemaIndex(sl, /*elements_base=*/1, &si_info);
      have_schema_index = !schema_index.empty();
    }

    // ---- file metadata: created_by + key_value_metadata (verbatim list).
    Writer filemeta;
    bool have_filemeta = false;
    if (fm.has_created_by) { filemeta.Binary(1, fm.created_by); have_filemeta = true; }
    if (fm.kv_span) { filemeta.Field(2, T_LIST); filemeta.AppendRaw(fm.kv_span, fm.kv_len); have_filemeta = true; }
    if (have_filemeta) filemeta.Stop();

    // ---- row-group statistics: per-column ColumnStatistics descriptors located
    // by a column_offsets directory (absolute offsets into the output buffer).
    std::vector<std::string> col_desc(C);
    bool have_stats = false;
    for (int c = 0; c < C; ++c) {
      col_desc[c] = BuildColumnStatistics(fm, c, G, trunc);
      if (!col_desc[c].empty()) have_stats = true;
    }

    // ---- Lay out the output and collect the directory. Core sections are
    // ordered from least to most important so the most important bytes are
    // closest to the tail: stats payload, stats directory, placement, schema,
    // and finally the ModularFooter root. Optional supporting modules precede
    // that core suffix.
    std::string out;
    std::vector<DirEntry> dir;
    auto place = [&](const std::string& blob) {
      int64_t off = static_cast<int64_t>(out.size());
      out.append(blob);
      return off;
    };
    // ---- page index (optional): only when --page-index is set AND the data is
    // actually reachable in the input -- a full file whose page-index byte ranges
    // lie before the footer. A bare tail, or a file without a page index, emits
    // none (nothing is invented).
    if (emit_pi && full_file) {
      auto build_index = [&](bool column, int32_t kind) {
        std::vector<uint64_t> chunk_off(N + 1, 0);
        std::vector<std::string> desc(N);
        bool any = false;
        for (int c = 0; c < C; ++c)
          for (int g = 0; g < G; ++g) {
            int64_t cc = static_cast<int64_t>(c) * G + g;
            const Chunk& ch = fm.row_groups[g].columns[c];
            const int64_t off = column ? ch.column_index_offset : ch.offset_index_offset;
            const int32_t len = column ? ch.column_index_length : ch.offset_index_length;
            const bool has = column ? ch.has_column_index : ch.has_offset_index;
            if (has && len > 0 && off >= 0 && off + len <= footer_start) {
              std::string blob = ReadRange(in, off, len);
              desc[cc] = column ? BuildColumnIndexChunk(blob, trunc) : BuildOffsetIndexChunk(blob);
              any = true;
            }
          }
        if (!any) return;
        for (int64_t cc = 0; cc < N; ++cc) { chunk_off[cc] = out.size(); out.append(desc[cc]); }
        chunk_off[N] = out.size();
        Writer pim;
        PutIntDense(pim, 1, chunk_off);  // chunk_offsets
        pim.Stop();
        dir.push_back({kind, place(pim.bytes()), static_cast<int64_t>(pim.bytes().size())});
      };
      build_index(false, K_OFFSET_INDEX);
      build_index(true, K_COLUMN_INDEX);
    }

    if (have_filemeta)
      dir.push_back({K_FILE_METADATA, place(filemeta.bytes()),
                     static_cast<int64_t>(filemeta.bytes().size())});
    if (have_schema_index)
      dir.push_back({K_SCHEMA_INDEX, place(schema_index),
                     static_cast<int64_t>(schema_index.size())});

    if (have_stats) {
      std::vector<uint64_t> col_off(C + 1, 0);
      for (int c = 0; c < C; ++c) {
        col_off[c] = static_cast<uint64_t>(out.size());
        out.append(col_desc[c]);                 // empty descriptor => col_off[c]==col_off[c+1]
      }
      col_off[C] = static_cast<uint64_t>(out.size());
      Writer rgstats;
      PutIntDense(rgstats, 1, col_off);           // column_offsets
      rgstats.Stop();
      dir.push_back({K_ROW_GROUP_STATISTICS, place(rgstats.bytes()),
                     static_cast<int64_t>(rgstats.bytes().size())});
    }

    dir.push_back({K_PLACEMENT, place(placement.bytes()),
                   static_cast<int64_t>(placement.bytes().size())});
    dir.push_back({K_SCHEMA, place(schema.bytes()),
                   static_cast<int64_t>(schema.bytes().size())});

    // ---- ModularFooter directory root, appended last.
    Writer root;
    root.Field(1, T_I32); root.I32(fm.version);
    root.Field(2, T_I32); root.I32(G);
    root.Field(3, T_I32); root.I32(C);
    root.Field(4, T_I64); root.I64(fm.num_rows);
    root.ListField(5, T_STRUCT, static_cast<int32_t>(dir.size()));
    for (const DirEntry& e : dir) {
      int16_t s = root.StructBegin();
      root.Field(1, T_I32); root.I32(e.kind);
      root.Field(2, T_STRUCT);
      int16_t s2 = root.StructBegin();
      root.Field(1, T_I64); root.I64(e.off);
      root.Field(2, T_I64); root.I64(e.len);
      root.Stop(); root.StructEnd(s2);
      root.Stop(); root.StructEnd(s);
    }
    root.Stop();
    int64_t root_off = place(root.bytes());

    std::ofstream out_file(out_path, std::ios::binary | std::ios::trunc);
    if (!out_file) throw std::runtime_error("cannot open " + out_path + " for writing");
    if (emit_full_file) {
      in.clear();
      in.seekg(0);
      constexpr size_t kCopyBufferBytes = 1024 * 1024;
      std::vector<char> copy_buffer(kCopyBufferBytes);
      int64_t remaining = footer_start;
      while (remaining > 0) {
        const size_t count = static_cast<size_t>(
            std::min<int64_t>(remaining, static_cast<int64_t>(copy_buffer.size())));
        in.read(copy_buffer.data(), static_cast<std::streamsize>(count));
        if (in.gcount() != static_cast<std::streamsize>(count))
          throw std::runtime_error("short read while copying Parquet data prefix");
        out_file.write(copy_buffer.data(), static_cast<std::streamsize>(count));
        remaining -= static_cast<int64_t>(count);
      }
    }
    const int64_t modular_start = emit_full_file ? footer_start : 0;
    out_file.write(out.data(), out.size());
    // Navigability trailer: the root directory is written last, so record its
    // offset within the modular blob (LE i64) plus a magic. Full-file output also
    // records where that blob begins. The module layout and modular_total_bytes
    // are unchanged.
    char trailer[20];
    const int trailer_size = emit_full_file ? 20 : 12;
    const int root_position = emit_full_file ? 8 : 0;
    if (emit_full_file) {
      for (int b = 0; b < 8; ++b)
        trailer[b] = static_cast<char>(
            (static_cast<uint64_t>(modular_start) >> (8 * b)) & 0xFF);
    }
    for (int b = 0; b < 8; ++b)
      trailer[root_position + b] = static_cast<char>(
          (static_cast<uint64_t>(root_off) >> (8 * b)) & 0xFF);
    std::memcpy(trailer + root_position + 8, emit_full_file ? "MFP1" : "MFT1", 4);
    out_file.write(trailer, trailer_size);
    out_file.close();

    auto kind_name = [](int32_t k) -> const char* {
      switch (k) {
        case K_SCHEMA: return "schema";
        case K_PLACEMENT: return "placement";
        case K_ROW_GROUP_STATISTICS: return "row_group_stats";
        case K_OFFSET_INDEX: return "offset_index";
        case K_COLUMN_INDEX: return "column_index";
        case K_FILE_METADATA: return "file_metadata";
        case K_SCHEMA_INDEX: return "schema_index";
        default: return "?";
      }
    };
    std::printf("input                 %s\n", in_path.c_str());
    std::printf("oss_footer_bytes      %zu\n", oss_footer_bytes);
    std::printf("columns               %d\n", C);
    std::printf("row_groups            %d\n", G);
    std::printf("column_chunks         %lld\n", static_cast<long long>(N));
    std::printf("rows                  %lld\n", static_cast<long long>(fm.num_rows));
    std::printf("modular_total_bytes   %zu\n", out.size());
    std::printf("modules (kind: directory location; per-chunk/column descriptors counted in total):\n");
    for (const DirEntry& e : dir)
      std::printf("  %-16s off=%lld len=%lld\n", kind_name(e.kind),
                  static_cast<long long>(e.off), static_cast<long long>(e.len));
    if (have_schema_index)
      std::printf("  schema_index      %d slots x %d bits (0.7 load, 8 disc bits), "
                  "leaf_element_indexes %s, non_ascii %s\n",
                  si_info.slots, si_info.slot_bits, si_info.flat ? "omitted (flat)" : "present",
                  si_info.non_ascii ? "true" : "false");
    const uint64_t output_bytes = static_cast<uint64_t>(modular_start) + out.size() +
                                  static_cast<uint64_t>(trailer_size);
    std::printf("output_mode           %s\n", emit_full_file ? "full_file" : "metadata_only");
    if (emit_full_file)
      std::printf("unchanged_prefix      %lld bytes\n", static_cast<long long>(footer_start));
    std::printf("wrote                 %s (%llu bytes)\n", out_path.c_str(),
                static_cast<unsigned long long>(output_bytes));
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
