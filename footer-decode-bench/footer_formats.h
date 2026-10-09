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

// footer_formats.h -- the format-decoding layer, built on thrift_codec.h.
//
// Every footer variant is a Resolver: given a column projection, return the
// {placement + statistics} each projected chunk resolves to. All four share the
// same API so the driver treats them uniformly.
//
// min/max are returned as spans (min = min_a ++ min_b) into a persistent buffer,
// not owned strings -- so the lean readers (walk / index / modular) never copy
// or reconstruct stat bytes, and the benchmark measures decode work, not the
// allocator. A modular bound is two segments (prefix + suffix); the others are
// one. `standard` deliberately materializes everything (its modeled cost) into a
// stable member and points its spans there.

#ifndef PFB_FOOTER_FORMATS_H_
#define PFB_FOOTER_FORMATS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "thrift_codec.h"

namespace fdb {

#ifdef PFB_HAVE_RUNTIME_DBP
extern "C" int PfbRuntimeDbpDecodeInt64(const uint8_t *data, size_t size,
                                        int64_t *output, size_t output_capacity,
                                        size_t *output_size);
#endif

// Compare two segmented byte values (a1 ++ b1) vs (a2 ++ b2) without materializing.
inline bool SegEqual(Span a1, Span b1, Span a2, Span b2) {
  if (static_cast<size_t>(a1.size) + b1.size != static_cast<size_t>(a2.size) + b2.size) return false;
  size_t n = static_cast<size_t>(a1.size) + b1.size;
  for (size_t i = 0; i < n; ++i) {
    char c1 = i < a1.size ? a1.data[i] : b1.data[i - a1.size];
    char c2 = i < a2.size ? a2.data[i] : b2.data[i - a2.size];
    if (c1 != c2) return false;
  }
  return true;
}

// What every resolver returns per projected chunk: placement + statistics.
// min/max are spans into a buffer the resolver keeps alive.
struct Loc {
  int64_t off = 0, size = 0;
  bool has_null = false; int64_t null_count = 0;
  bool has_mm = false;
  Span min_a, min_b, max_a, max_b;  // min = min_a ++ min_b, max = max_a ++ max_b
  bool operator==(const Loc& o) const {
    if (off != o.off || size != o.size || has_null != o.has_null) return false;
    if (has_null && null_count != o.null_count) return false;
    if (has_mm != o.has_mm) return false;
    if (has_mm && (!SegEqual(min_a, min_b, o.min_a, o.min_b) ||
                   !SegEqual(max_a, max_b, o.max_a, o.max_b)))
      return false;
    return true;
  }
};
using Placement = std::vector<Loc>;

// Field ids we navigate.
enum { // FileMetaData / RowGroup / ColumnChunk / ColumnMetaData / Statistics
  FMD_SCHEMA = 2,
  FMD_ROW_GROUPS = 4,
  FMD_FOOTER_INDEX_POINTER = 10,
  RG_COLUMNS = 1,
  RG_NUM_ROWS = 3,
  CC_META_DATA = 3,
  CM_TOTAL_COMPRESSED = 7,
  CM_DATA_PAGE_OFFSET = 9,
  CM_STATISTICS = 12,
  ST_MAX_DEP = 1,
  ST_MIN_DEP = 2,
  ST_NULL_COUNT = 3,
  ST_MAX_VALUE = 5,
  ST_MIN_VALUE = 6,
};
enum {  // FileMetadataFooterIndex (jump table)
  IDX_NUM_LEAF = 1, IDX_NUM_RG = 2, IDX_CHUNK_OFFSETS = 3,
};
enum {  // ModularFooter
  K_PLACEMENT = 1, K_ROW_GROUP_STATISTICS = 2, MOD_DIRECTORY = 5,
  DIR_KIND = 1, DIR_LOCATION = 2, LOC_OFFSET = 1,
  PLACE_DATA_PAGE_OFFSETS = 1, PLACE_TOTAL_COMPRESSED = 4,
  CS_NULL_COUNTS = 1, CS_MINMAX_PREFIXES = 2, CS_MIN_SUFFIXES = 3, CS_MAX_SUFFIXES = 4,
  RGS_COLUMN_OFFSETS = 1,
  // EncodedArray field ids: 1 num_values, 2 values, 3 encoding, 4 presence.
  EA_NUM_VALUES = 1, EA_VALUES = 2, EA_ENCODING = 3, EA_PRESENCE = 4,
};
enum { // PageFirstModularFooter
  PAGE_MOD_DIRECTORY = 4,
  PAGE_DIR_KIND = 1,
  PAGE_DIR_REGION = 2,
  PK_PAGE_DIRECTORY = 1,
  REGION_MAP_OFFSETS = 1,
  PAGE_DIRECTORY_REGIONS = 1,
  COLUMN_PAGE_COUNT = 1,
  COLUMN_PAGE_REGIONS = 2,
  COLUMN_PAGE_FIRST_ROWS = 3,
  COLUMN_FULLY_DICTIONARY = 4,
  COLUMN_PAGE_CODECS = 5,
  COLUMN_PAGE_DICTIONARIES = 6,
  COLUMN_PAGE_NUM_VALUES = 7,
  COLUMN_PAGE_UNCOMPRESSED_SIZES = 8,
  PAGE_ARRAY_BITSET = 0,
  PAGE_ARRAY_DELTA_BINARY_PACKED = 2,
};

// The common interface. C x G = the chunk grid; a projection is a per-column mask.
class Resolver {
 public:
  Resolver(int columns, int row_groups) : C(columns), G(row_groups) {}
  virtual ~Resolver() = default;
  virtual std::string name() const = 0;
  virtual Placement Resolve(const std::vector<char> &want,
                            bool include_stats = true) const = 0;

protected:
  int C, G;
};

// Emit projected chunks in canonical (column-major, row-group-minor) order.
inline Placement Gather(const std::vector<Loc>& grid, const std::vector<char>& want, int C, int G) {
  Placement out;
  for (int c = 0; c < C; ++c)
    if (want[c]) for (int g = 0; g < G; ++g) out.push_back(grid[static_cast<size_t>(c) * G + g]);
  return out;
}

// ============================================================ nested-footer decode
// Decode a Statistics struct into a Loc's stat fields (min/max as spans into the
// reader's buffer). Deprecated (1/2) are used only when the current fields (5/6)
// are absent, matching how footers are written.
inline void ReadStatistics(Reader& r, Loc& L) {
  Span mnd, mxd, mnv, mxv;
  bool hmnd = false, hmxd = false, hmnv = false, hmxv = false;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == ST_MAX_DEP) { mxd = r.BinarySpan(); hmxd = true; }
    else if (f.id == ST_MIN_DEP) { mnd = r.BinarySpan(); hmnd = true; }
    else if (f.id == ST_NULL_COUNT) { L.null_count = r.I64(); L.has_null = true; }
    else if (f.id == ST_MAX_VALUE) { mxv = r.BinarySpan(); hmxv = true; }
    else if (f.id == ST_MIN_VALUE) { mnv = r.BinarySpan(); hmnv = true; }
    else r.Skip(f.type);
  }
  r.StructEnd(s);
  if ((hmnv || hmnd) && (hmxv || hmxd)) {
    L.has_mm = true;
    L.min_a = hmnv ? mnv : mnd;
    L.max_a = hmxv ? mxv : mxd;
  }
}
// Decode one ColumnChunk (cursor at the struct start) -> placement + stats.
inline Loc ReadColumnInfo(Reader &r, bool include_stats = true) {
  Loc L;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == CC_META_DATA && f.type == T_STRUCT) {
      int16_t s2 = r.StructBegin();
      for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
        if (g.id == CM_TOTAL_COMPRESSED) L.size = r.I64();
        else if (g.id == CM_DATA_PAGE_OFFSET) L.off = r.I64();
        else if (g.id == CM_STATISTICS && g.type == T_STRUCT && include_stats)
          ReadStatistics(r, L);
        else r.Skip(g.type);
      }
      r.StructEnd(s2);
    } else {
      r.Skip(f.type);
    }
  }
  r.StructEnd(s);
  return L;
}

inline std::vector<int64_t> ReadRowGroupEnds(const std::string &footer) {
  std::vector<int64_t> ends;
  int64_t total = 0;
  Reader reader(footer.data(), footer.size());
  for (Reader::Field field = reader.NextField(); field.type != T_STOP;
       field = reader.NextField()) {
    if (field.id != FMD_ROW_GROUPS || field.type != T_LIST) {
      reader.Skip(field.type);
      continue;
    }
    Reader::ListHdr groups = reader.List();
    ends.reserve(groups.size);
    for (int32_t group = 0; group < groups.size; ++group) {
      int64_t rows = -1;
      int16_t state = reader.StructBegin();
      for (Reader::Field item = reader.NextField(); item.type != T_STOP;
           item = reader.NextField()) {
        if (item.id == RG_NUM_ROWS && item.type == T_I64)
          rows = reader.I64();
        else
          reader.Skip(item.type);
      }
      reader.StructEnd(state);
      if (rows < 0 || rows > std::numeric_limits<int64_t>::max() - total)
        throw std::runtime_error("invalid row-group row count");
      total += rows;
      ends.push_back(total);
    }
  }
  return ends;
}

// ------- standard: full Thrift materialization (a generated-Thrift reader, e.g.
// parquet-java's). Decodes the ENTIRE FileMetaData into an owned value tree kept
// in a member, then points its spans there. Materializes everything regardless
// of projection -- that allocation IS the cost it models.
class StandardResolver : public Resolver {
 public:
  StandardResolver(const std::string& footer, int C, int G) : Resolver(C, G), footer_(footer) {}
  std::string name() const override { return "standard"; }
  Placement Resolve(const std::vector<char> &want,
                    bool include_stats = true) const override {
    Reader r(footer_.data(), footer_.size());
    tree_ = DecodeValue(r, T_STRUCT);  // materialized; stays alive as a member
    std::vector<Loc> grid(static_cast<size_t>(C) * G);
    if (const Value* rgs = tree_.field(FMD_ROW_GROUPS)) {
      for (int g = 0; g < static_cast<int>(rgs->items.size()) && g < G; ++g) {
        const Value* cols = rgs->items[g].field(RG_COLUMNS);
        if (!cols) continue;
        for (int c = 0; c < static_cast<int>(cols->items.size()) && c < C; ++c) {
          const Value* md = cols->items[c].field(CC_META_DATA);
          Loc L;
          if (md) {
            if (const Value* v = md->field(CM_DATA_PAGE_OFFSET)) L.off = v->num;
            if (const Value* v = md->field(CM_TOTAL_COMPRESSED)) L.size = v->num;
            if (include_stats) {
              if (const Value *st = md->field(CM_STATISTICS)) {
                if (const Value *nc = st->field(ST_NULL_COUNT)) {
                  L.has_null = true;
                  L.null_count = nc->num;
                }
                const Value *mn = st->field(ST_MIN_VALUE);
                if (!mn)
                  mn = st->field(ST_MIN_DEP);
                const Value *mx = st->field(ST_MAX_VALUE);
                if (!mx)
                  mx = st->field(ST_MAX_DEP);
                if (mn && mx) {
                  L.has_mm = true;
                  L.min_a = {mn->bin.data(),
                             static_cast<uint32_t>(mn->bin.size())};
                  L.max_a = {mx->bin.data(),
                             static_cast<uint32_t>(mx->bin.size())};
                }
              }
            }
          }
          grid[static_cast<size_t>(c) * G + g] = L;
        }
      }
    }
    return Gather(grid, want, C, G);
  }

 private:
  const std::string& footer_;
  mutable Value tree_;  // holds the materialized values the spans point into
};

// ------- walk: today's reader -- walk every row group and column chunk, decode
// the projected ones, skip the rest.
class WalkResolver : public Resolver {
 public:
  WalkResolver(const std::string& footer, int C, int G) : Resolver(C, G), footer_(footer) {}
  std::string name() const override { return "walk"; }
  Placement Resolve(const std::vector<char> &want,
                    bool include_stats = true) const override {
    std::vector<Loc> grid(static_cast<size_t>(C) * G);
    Reader r(footer_.data(), footer_.size());
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id != FMD_ROW_GROUPS || f.type != T_LIST) { r.Skip(f.type); continue; }
      Reader::ListHdr rgs = r.List();
      for (int32_t g = 0; g < rgs.size; ++g) {
        int16_t sg = r.StructBegin();
        for (Reader::Field gf = r.NextField(); gf.type != T_STOP; gf = r.NextField()) {
          if (gf.id != RG_COLUMNS || gf.type != T_LIST) { r.Skip(gf.type); continue; }
          Reader::ListHdr cols = r.List();
          for (int32_t c = 0; c < cols.size; ++c) {
            if (want[c])
              grid[static_cast<size_t>(c) * G + g] =
                  ReadColumnInfo(r, include_stats);
            else r.Skip(T_STRUCT);
          }
        }
        r.StructEnd(sg);
      }
    }
    return Gather(grid, want, C, G);
  }

 private:
  const std::string& footer_;
};

// The jump-table index (FileMetaData.footer_index_pointer -> FileMetadataFooterIndex).
struct FooterIndex {
  int columns = 0, row_groups = 0, bytes_per_entry = 0;
  int64_t fmd_length = 0;
  std::string column_chunk_offsets;
};
inline FooterIndex ReadFooterIndex(const std::string& footer) {
  FooterIndex fi;
  {  // footer_index_pointer (field 10): version + 3 LE i64; take file_meta_data_length.
    Reader r(footer.data(), footer.size());
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == FMD_FOOTER_INDEX_POINTER && f.type == T_BINARY) {
        std::string p = r.Binary();
        if (p.size() < 25 || static_cast<uint8_t>(p[0]) != 1)
          throw std::runtime_error("unsupported footer_index_pointer version");
        std::memcpy(&fi.fmd_length, p.data() + 17, 8);
        break;
      }
      r.Skip(f.type);
    }
  }
  if (fi.fmd_length <= 0)
    throw std::runtime_error("no footer_index_pointer -- run parquet_to_jumptable first");
  Reader r(footer.data() + fi.fmd_length, footer.size() - static_cast<size_t>(fi.fmd_length));
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == IDX_NUM_LEAF && f.type == T_I32) fi.columns = r.I32();
    else if (f.id == IDX_NUM_RG && f.type == T_I32) fi.row_groups = r.I32();
    else if (f.id == IDX_CHUNK_OFFSETS && f.type == T_BINARY) fi.column_chunk_offsets = r.Binary();
    else r.Skip(f.type);
  }
  if (fi.column_chunk_offsets.empty() || fi.columns == 0 || fi.row_groups == 0)
    throw std::runtime_error("failed to decode FileMetadataFooterIndex");
  fi.bytes_per_entry = static_cast<int>(fi.column_chunk_offsets.size() /
                                        (static_cast<size_t>(fi.row_groups) * (fi.columns + 1)));
  return fi;
}

// ------- index: seek via column_chunk_offsets to each projected chunk, decode
// only those (placement + stats spans into the footer).
class IndexResolver : public Resolver {
 public:
  IndexResolver(const std::string& footer, const FooterIndex& fi)
      : Resolver(fi.columns, fi.row_groups), footer_(footer),
        cco_(fi.column_chunk_offsets), bpe_(fi.bytes_per_entry) {}
  std::string name() const override { return "index"; }
  Placement Resolve(const std::vector<char> &want,
                    bool include_stats = true) const override {
    const uint8_t* cco = reinterpret_cast<const uint8_t*>(cco_.data());
    int stride = C + 1;
    Placement out;
    for (int c = 0; c < C; ++c) {
      if (!want[c]) continue;
      for (int g = 0; g < G; ++g) {
        int64_t bo = 0;
        const uint8_t* p = cco + (static_cast<size_t>(g) * stride + c) * bpe_;
        for (int b = 0; b < bpe_; ++b) bo |= static_cast<int64_t>(p[b]) << (8 * b);
        Reader r(footer_.data() + bo, footer_.size() - static_cast<size_t>(bo));
        out.push_back(ReadColumnInfo(r, include_stats));
      }
    }
    return out;
  }

 private:
  const std::string& footer_;
  std::string cco_;
  int bpe_;
};

// ============================================================== modular decode
// A parsed EncodedArray. `values` = [u8 width][packed values...]; `presence` is
// empty for a dense BITSET, else [ULEB128 num_present][u8 pos_width][packed positions].
// The element type (int vs bytes) is implied by the field, as in the spec. ExtractBits
// over-reads up to 9 bytes past the last bit, which stays inside the buffer (packed
// arrays are always followed by more modules / the root / the trailer).
struct ArrayPage { Span values; Span presence; int encoding = 0; int num_values = 0; };
inline ArrayPage ParseArrayPage(Reader& r) {
  ArrayPage ap;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_I32) ap.num_values = r.I32();
    else if (f.id == 2 && f.type == T_BINARY) ap.values = r.BinarySpan();
    else if (f.id == 3 && f.type == T_I32) ap.encoding = r.I32();       // omitted => BITSET (0)
    else if (f.id == 4 && f.type == T_BINARY) ap.presence = r.BinarySpan();
    else r.Skip(f.type);
  }
  r.StructEnd(s);
  return ap;
}

inline uint64_t ExtractBitsBounded(const uint8_t *data, size_t size,
                                   size_t index, int width) {
  if (width == 0)
    return 0;
  size_t bit = index * static_cast<size_t>(width);
  size_t byte = bit >> 3;
  int shift = bit & 7;
  unsigned __int128 value = 0;
  for (int i = 0; i < 9 && byte + i < size; ++i)
    value |= static_cast<unsigned __int128>(data[byte + i]) << (8 * i);
  return static_cast<uint64_t>((value >> shift) & LowMask(width));
}

// Decode the standard Parquet DELTA_BINARY_PACKED payload used by PageArray.
inline std::vector<int64_t> DecodePageArray(const ArrayPage &ap) {
  if (ap.encoding == PAGE_ARRAY_BITSET) {
    std::vector<int64_t> values(ap.num_values);
    int width = ap.values.size ? static_cast<uint8_t>(ap.values.data[0]) : 0;
    const uint8_t *packed =
        reinterpret_cast<const uint8_t *>(ap.values.data) + 1;
    size_t bytes = ap.values.size ? ap.values.size - 1 : 0;
    for (int i = 0; i < ap.num_values; ++i)
      values[i] =
          static_cast<int64_t>(ExtractBitsBounded(packed, bytes, i, width));
    return values;
  }
  if (ap.encoding != PAGE_ARRAY_DELTA_BINARY_PACKED)
    throw std::runtime_error("unsupported PageArray encoding");
#ifndef PFB_HAVE_RUNTIME_DBP
  throw std::runtime_error(
      "page decode benchmark requires the runtime DBP bridge");
#else
  std::vector<int64_t> values(ap.num_values);
  size_t output_size = 0;
  int status = PfbRuntimeDbpDecodeInt64(
      reinterpret_cast<const uint8_t *>(ap.values.data), ap.values.size,
      values.data(), values.size(), &output_size);
  if (status != 0 || output_size != values.size())
    throw std::runtime_error("runtime DBP decoder rejected PageArray");
  return values;
#endif
}

// value/offset width is the first byte of `values`; the packed stream follows it.
inline int APWidth(const ArrayPage& ap) {
  return ap.values.size > 0 ? static_cast<uint8_t>(ap.values.data[0]) : 0;
}
inline const uint8_t* APData(const ArrayPage& ap) {
  return reinterpret_cast<const uint8_t*>(ap.values.data) + 1;
}
// Decode `presence` = [ULEB128 num_present][u8 pos_width][packed positions].
struct APPresence { int np; int wpos; const uint8_t* pos; };
inline APPresence ParsePresence(const ArrayPage& ap) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(ap.presence.data);
  uint64_t np = 0; int shift = 0;
  for (;;) { uint8_t b = *p++; np |= static_cast<uint64_t>(b & 0x7F) << shift; if (!(b & 0x80)) break; shift += 7; }
  int wpos = *p++;
  return {static_cast<int>(np), wpos, p};
}
// Dense BITSET: value i read directly from the packed value stream.
inline int64_t BitsetAt(const ArrayPage& ap, size_t i) {
  return static_cast<int64_t>(ExtractBits(APData(ap), i, APWidth(ap)));
}
inline void PresentInt(const ArrayPage& ap, int G, std::vector<char>& has, std::vector<int64_t>& val) {
  APPresence pr = ParsePresence(ap);
  int wval = APWidth(ap);
  const uint8_t* vd = APData(ap);
  for (int i = 0; i < pr.np; ++i) {
    int p = static_cast<int>(ExtractBits(pr.pos, i, pr.wpos));
    if (p >= 0 && p < G) { has[p] = 1; val[p] = static_cast<int64_t>(ExtractBits(vd, i, wval)); }
  }
}
// Fill has[G]/out[G] with spans into the modular buffer (no copy).
inline void PresentBytes(const ArrayPage& ap, int G, std::vector<char>& has, std::vector<Span>& out) {
  APPresence pr = ParsePresence(ap);
  int woff = APWidth(ap);
  const uint8_t* cd = APData(ap);                                       // packed cumulative offsets (np+1)
  size_t cum_bytes = ((static_cast<size_t>(pr.np) + 1) * woff + 7) / 8;
  const char* bytes = ap.values.data + 1 + cum_bytes;                  // concatenated bytes after offsets
  for (int i = 0; i < pr.np; ++i) {
    int p = static_cast<int>(ExtractBits(pr.pos, i, pr.wpos));
    uint64_t c0 = ExtractBits(cd, i, woff), c1 = ExtractBits(cd, i + 1, woff);
    if (p >= 0 && p < G) { has[p] = 1; out[p] = {bytes + c0, static_cast<uint32_t>(c1 - c0)}; }
  }
}

// ------- modular: placement from the column-major bit-packed PlacementModule,
// statistics from a separate ColumnStatistics module. min/max are prefix + suffix
// spans into the modular buffer -- reconstructed only logically, never copied.
class ModularResolver : public Resolver {
 public:
  ModularResolver(const std::string& mod, int C, int G) : Resolver(C, G), mod_(mod) {
    if (mod.size() < 12 || std::memcmp(mod.data() + mod.size() - 4, "MFT1", 4) != 0)
      throw std::runtime_error("modular file missing MFT1 trailer -- rebuild parquet_to_modular");
    int64_t root_off = 0;
    std::memcpy(&root_off, mod.data() + mod.size() - 12, 8);
    int64_t place_off = -1, stats_off = -1;
    Reader rr(mod.data() + root_off, mod.size() - static_cast<size_t>(root_off));
    for (Reader::Field f = rr.NextField(); f.type != T_STOP; f = rr.NextField()) {
      if (f.id != MOD_DIRECTORY || f.type != T_LIST) { rr.Skip(f.type); continue; }
      Reader::ListHdr d = rr.List();
      for (int32_t i = 0; i < d.size; ++i) {
        int32_t kind = -1; int64_t off = 0;
        int16_t s = rr.StructBegin();
        for (Reader::Field g = rr.NextField(); g.type != T_STOP; g = rr.NextField()) {
          if (g.id == DIR_KIND) kind = rr.I32();
          else if (g.id == DIR_LOCATION && g.type == T_STRUCT) {
            int16_t s2 = rr.StructBegin();
            for (Reader::Field h = rr.NextField(); h.type != T_STOP; h = rr.NextField()) {
              if (h.id == LOC_OFFSET) off = rr.I64(); else rr.Skip(h.type);
            }
            rr.StructEnd(s2);
          } else rr.Skip(g.type);
        }
        rr.StructEnd(s);
        if (kind == K_PLACEMENT) place_off = off;
        else if (kind == K_ROW_GROUP_STATISTICS) stats_off = off;
      }
    }
    if (place_off < 0) throw std::runtime_error("modular footer has no placement module");
    Reader pr(mod.data() + place_off, mod.size() - static_cast<size_t>(place_off));
    for (Reader::Field f = pr.NextField(); f.type != T_STOP; f = pr.NextField()) {
      if (f.id == PLACE_DATA_PAGE_OFFSETS && f.type == T_STRUCT) dpo_ = ParseArrayPage(pr);
      else if (f.id == PLACE_TOTAL_COMPRESSED && f.type == T_STRUCT) tcs_ = ParseArrayPage(pr);
      else pr.Skip(f.type);
    }
    col_off_.assign(C + 1, 0);
    // Reusable per-column stat buffers, sized once (no per-column allocation).
    s_has_null_.assign(G, 0); s_null_count_.assign(G, 0); s_has_mm_.assign(G, 0);
    s_pref_.assign(G, Span{}); s_minsuf_.assign(G, Span{}); s_maxsuf_.assign(G, Span{});
    s_hp_.assign(G, 0); s_hms_.assign(G, 0); s_hxs_.assign(G, 0);
    if (stats_off >= 0) {
      have_stats_ = true;
      Reader sr(mod.data() + stats_off, mod.size() - static_cast<size_t>(stats_off));
      for (Reader::Field f = sr.NextField(); f.type != T_STOP; f = sr.NextField()) {
        if (f.id == RGS_COLUMN_OFFSETS && f.type == T_STRUCT) {
          ArrayPage co = ParseArrayPage(sr);
          for (int c = 0; c <= C; ++c) col_off_[c] = BitsetAt(co, c);
        } else sr.Skip(f.type);
      }
    }
  }
  std::string name() const override { return "modular"; }
  Placement Resolve(const std::vector<char> &want,
                    bool include_stats = true) const override {
    Placement out;
    for (int c = 0; c < C; ++c) {
      if (!want[c]) continue;
      if (have_stats_ && include_stats)
        DecodeColumnStatistics(col_off_[c], col_off_[c + 1] - col_off_[c]);
      for (int g = 0; g < G; ++g) {
        size_t cc = static_cast<size_t>(c) * G + g;
        Loc L;
        L.off = BitsetAt(dpo_, cc);
        L.size = BitsetAt(tcs_, cc);
        if (have_stats_ && include_stats) {
          L.has_null = s_has_null_[g]; L.null_count = s_null_count_[g];
          if (s_has_mm_[g]) {
            L.has_mm = true;
            L.min_a = s_pref_[g]; L.min_b = s_minsuf_[g];  // min = prefix ++ min_suffix
            L.max_a = s_pref_[g]; L.max_b = s_maxsuf_[g];  // max = prefix ++ max_suffix
          }
        }
        out.push_back(L);
      }
    }
    return out;
  }

 private:
  // Fill the reusable per-column buffers from one ColumnStatistics module. No
  // allocation per column -- only the present-flags need resetting (the value
  // buffers are gated by them). Spans point into mod_, so emitted Locs stay valid
  // regardless of the next column's decode.
  void DecodeColumnStatistics(int64_t off, int64_t len) const {
    std::fill(s_has_null_.begin(), s_has_null_.end(), 0);
    std::fill(s_has_mm_.begin(), s_has_mm_.end(), 0);
    std::fill(s_hp_.begin(), s_hp_.end(), 0);
    std::fill(s_hms_.begin(), s_hms_.end(), 0);
    std::fill(s_hxs_.begin(), s_hxs_.end(), 0);
    if (len <= 0) return;
    Reader r(mod_.data() + off, static_cast<size_t>(len));
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.type != T_STRUCT) { r.Skip(f.type); continue; }
      if (f.id == CS_NULL_COUNTS) { ArrayPage ap = ParseArrayPage(r); PresentInt(ap, G, s_has_null_, s_null_count_); }
      else if (f.id == CS_MINMAX_PREFIXES) { ArrayPage ap = ParseArrayPage(r); PresentBytes(ap, G, s_hp_, s_pref_); }
      else if (f.id == CS_MIN_SUFFIXES) { ArrayPage ap = ParseArrayPage(r); PresentBytes(ap, G, s_hms_, s_minsuf_); }
      else if (f.id == CS_MAX_SUFFIXES) { ArrayPage ap = ParseArrayPage(r); PresentBytes(ap, G, s_hxs_, s_maxsuf_); }
      else r.Skip(f.type);
    }
    for (int g = 0; g < G; ++g) s_has_mm_[g] = s_hp_[g] && s_hms_[g] && s_hxs_[g];
  }

  const std::string& mod_;
  ArrayPage dpo_, tcs_;
  std::vector<int64_t> col_off_;
  bool have_stats_ = false;
  mutable std::vector<char> s_has_null_, s_has_mm_, s_hp_, s_hms_, s_hxs_;
  mutable std::vector<int64_t> s_null_count_;
  mutable std::vector<Span> s_pref_, s_minsuf_, s_maxsuf_;
};

// ------- page-first: decode the mandatory physical region map, then resolve
// each projected column's logical pages to those regions. The cold variant
// charges the sequential region-offset DBP decode to every operation; the warm
// variant models a reader retaining the always-read map.
class PageResolver : public Resolver {
public:
  PageResolver(const std::string &mod, int C, int G, bool cold,
               const std::vector<int64_t> &range_ends)
      : Resolver(C, G), mod_(mod), cold_(cold), range_ends_(range_ends) {
    constexpr size_t kTrailerSize = 36;
    if (mod.size() < kTrailerSize ||
        std::memcmp(mod.data() + mod.size() - 4, "PMF1", 4) != 0)
      throw std::runtime_error("page footer missing PMF1 trailer");
    int64_t map_offset = 0, map_length = 0, root_offset = 0, root_length = 0;
    const char *trailer = mod.data() + mod.size() - kTrailerSize;
    std::memcpy(&map_offset, trailer, 8);
    std::memcpy(&map_length, trailer + 8, 8);
    std::memcpy(&root_offset, trailer + 16, 8);
    std::memcpy(&root_length, trailer + 24, 8);
    if (map_length <= 0 || root_length <= 0 ||
        static_cast<uint64_t>(map_length + root_length) >
            mod.size() - kTrailerSize)
      throw std::runtime_error("invalid PMF1 trailer lengths");
    size_t root_position =
        mod.size() - kTrailerSize - static_cast<size_t>(root_length);
    size_t map_position = root_position - static_cast<size_t>(map_length);
    base_ = map_offset - static_cast<int64_t>(map_position);
    if (root_offset - base_ != static_cast<int64_t>(root_position))
      throw std::runtime_error("PMF1 trailer offsets disagree with lengths");

    Reader map_reader(mod.data() + map_position,
                      static_cast<size_t>(map_length));
    for (Reader::Field f = map_reader.NextField(); f.type != T_STOP;
         f = map_reader.NextField()) {
      if (f.id == REGION_MAP_OFFSETS && f.type == T_STRUCT)
        encoded_region_offsets_ = ParseArrayPage(map_reader);
      else
        map_reader.Skip(f.type);
    }
    region_offsets_ = DecodePageArray(encoded_region_offsets_);

    int64_t page_directory = -1;
    Reader root(mod.data() + root_position, static_cast<size_t>(root_length));
    for (Reader::Field f = root.NextField(); f.type != T_STOP;
         f = root.NextField()) {
      if (f.id != PAGE_MOD_DIRECTORY || f.type != T_LIST) {
        root.Skip(f.type);
        continue;
      }
      Reader::ListHdr entries = root.List();
      for (int32_t i = 0; i < entries.size; ++i) {
        int32_t kind = -1;
        int64_t region = -1;
        int16_t state = root.StructBegin();
        for (Reader::Field e = root.NextField(); e.type != T_STOP;
             e = root.NextField()) {
          if (e.id == PAGE_DIR_KIND)
            kind = root.I32();
          else if (e.id == PAGE_DIR_REGION)
            region = root.I64();
          else
            root.Skip(e.type);
        }
        root.StructEnd(state);
        if (kind == PK_PAGE_DIRECTORY)
          page_directory = region;
      }
    }
    if (page_directory < 0)
      throw std::runtime_error("page footer is missing a required directory");
    page_regions_ = ReadDenseDirectory(page_directory, PAGE_DIRECTORY_REGIONS);
    if (static_cast<int>(page_regions_.size()) != C)
      throw std::runtime_error(
          "page footer directory has the wrong column count");
    if (static_cast<int>(range_ends_.size()) != G)
      throw std::runtime_error(
          "legacy row-range count disagrees with page benchmark");
  }

  std::string name() const override {
    return cold_ ? "page_cold" : "page_warm";
  }

  Placement Resolve(const std::vector<char> &want,
                    bool include_stats = true) const override {
    if (include_stats)
      throw std::runtime_error(
          "page resolver supports placement-only benchmarks");
    std::vector<int64_t> decoded_offsets;
    const std::vector<int64_t> *offsets = &region_offsets_;
    if (cold_) {
      decoded_offsets = DecodePageArray(encoded_region_offsets_);
      offsets = &decoded_offsets;
    }
    Placement out;
    for (int c = 0; c < C; ++c) {
      if (!want[c])
        continue;
      Span page_region = Region(page_regions_[c], *offsets);
      int page_count = -1;
      ArrayPage encoded_page_regions, encoded_first_rows, dictionary_regions,
          page_num_values, page_uncompressed_sizes;
      Span encoded_page_codecs;
      int fully_dictionary_encoded = -1;
      Reader pages(page_region.data, page_region.size);
      for (Reader::Field f = pages.NextField(); f.type != T_STOP;
           f = pages.NextField()) {
        if (f.id == COLUMN_PAGE_COUNT && f.type == T_I32)
          page_count = pages.I32();
        else if (f.id == COLUMN_PAGE_REGIONS && f.type == T_BINARY)
          encoded_page_regions.values = pages.BinarySpan();
        else if (f.id == COLUMN_PAGE_FIRST_ROWS && f.type == T_BINARY)
          encoded_first_rows.values = pages.BinarySpan();
        else if (f.id == COLUMN_FULLY_DICTIONARY &&
                 (f.type == T_TRUE || f.type == T_FALSE))
          fully_dictionary_encoded = f.type == T_TRUE;
        else if (f.id == COLUMN_PAGE_CODECS && f.type == T_BINARY)
          encoded_page_codecs = pages.BinarySpan();
        else if (f.id == COLUMN_PAGE_DICTIONARIES && f.type == T_BINARY)
          dictionary_regions.values = pages.BinarySpan();
        else if (f.id == COLUMN_PAGE_NUM_VALUES && f.type == T_BINARY)
          page_num_values.values = pages.BinarySpan();
        else if (f.id == COLUMN_PAGE_UNCOMPRESSED_SIZES && f.type == T_BINARY)
          page_uncompressed_sizes.values = pages.BinarySpan();
        else
          pages.Skip(f.type);
      }
      if (page_count < 0 || fully_dictionary_encoded < 0 ||
          encoded_page_codecs.size == 0)
        throw std::runtime_error(
            "column data pages are missing required fields");
      for (ArrayPage *array :
           {&encoded_page_regions, &encoded_first_rows, &dictionary_regions,
            &page_num_values, &page_uncompressed_sizes}) {
        if (array->values.size == 0)
          throw std::runtime_error(
              "column data pages are missing a packed array");
        array->num_values = page_count;
      }

      std::vector<Loc> column(G);
      std::vector<int64_t> starts(G, std::numeric_limits<int64_t>::max());
      std::vector<int64_t> ends(G, 0);
      std::vector<char> seen(G, 0);
      int group = 0;
      for (int page = 0; page < page_count; ++page) {
        int64_t first_row = BitsetAt(encoded_first_rows, page);
        while (group + 1 < G && first_row >= range_ends_[group])
          ++group;
        int64_t region = BitsetAt(encoded_page_regions, page);
        if (region < 0 || static_cast<size_t>(region + 1) >= offsets->size())
          throw std::runtime_error("data page has an invalid region ordinal");
        int64_t start = (*offsets)[region], end = (*offsets)[region + 1];
        if (!seen[group])
          column[group].off = start;
        seen[group] = 1;
        starts[group] = std::min(starts[group], start);
        ends[group] = std::max(ends[group], end);
        int64_t dictionary_plus_one = BitsetAt(dictionary_regions, page);
        if (dictionary_plus_one > 0) {
          int64_t dictionary = dictionary_plus_one - 1;
          if (static_cast<size_t>(dictionary + 1) >= offsets->size())
            throw std::runtime_error(
                "dictionary page has an invalid region ordinal");
          starts[group] = std::min(starts[group], (*offsets)[dictionary]);
          ends[group] = std::max(ends[group], (*offsets)[dictionary + 1]);
        }
      }
      for (int g = 0; g < G; ++g)
        if (seen[g])
          column[g].size = ends[g] - starts[g];
      out.insert(out.end(), column.begin(), column.end());
    }
    return out;
  }

private:
  Span Region(int64_t ordinal, const std::vector<int64_t> &offsets) const {
    if (ordinal < 0 || static_cast<size_t>(ordinal + 1) >= offsets.size())
      throw std::runtime_error("invalid metadata region ordinal");
    int64_t start = offsets[ordinal] - base_;
    int64_t end = offsets[ordinal + 1] - base_;
    if (start < 0 || end < start || static_cast<uint64_t>(end) > mod_.size())
      throw std::runtime_error(
          "metadata region is outside the page footer buffer");
    return {mod_.data() + start, static_cast<uint32_t>(end - start)};
  }

  std::vector<int64_t> ReadDenseDirectory(int64_t ordinal, int field_id) const {
    Span region = Region(ordinal, region_offsets_);
    Reader reader(region.data, region.size);
    for (Reader::Field f = reader.NextField(); f.type != T_STOP;
         f = reader.NextField()) {
      if (f.id == field_id && f.type == T_STRUCT)
        return DecodePageArray(ParseArrayPage(reader));
      reader.Skip(f.type);
    }
    throw std::runtime_error("page footer directory has no region array");
  }

  const std::string &mod_;
  bool cold_;
  int64_t base_ = 0;
  ArrayPage encoded_region_offsets_;
  std::vector<int64_t> region_offsets_, page_regions_, range_ends_;
};

}  // namespace fdb

#endif  // PFB_FOOTER_FORMATS_H_
