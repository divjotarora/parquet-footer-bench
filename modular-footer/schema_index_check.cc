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

// schema_index_check: reference reader and correctness check for the SCHEMA_INDEX
// module written by parquet_to_modular --schema-index. It reads a .modular file,
// locates the SCHEMA and SCHEMA_INDEX modules through the root directory, then for
// every leaf column resolves its name to an ordinal through the hash table and
// confirms the resolved ordinal against the leaf's own ordinal -- exercising exactly
// the O(projected) reader path (FNV probe -> element_offsets seek -> name compare).
// It also checks that a handful of absent names resolve to "not found".
//
//   cmake --build build -j        # target modular_schema_index_check
//   ./build/modular_schema_index_check file.si.modular
//
// Exit 0 and "OK N/N" on success; non-zero on any mismatch.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "pfb/bitpack.h"  // BitWidth, LowMask, ExtractBits

namespace {

enum : uint8_t { T_STOP = 0, T_TRUE = 1, T_FALSE = 2, T_I8 = 3, T_I16 = 4, T_I32 = 5,
                 T_I64 = 6, T_DOUBLE = 7, T_BINARY = 8, T_LIST = 9, T_SET = 10,
                 T_MAP = 11, T_STRUCT = 12 };
enum : int32_t { K_SCHEMA = 0, K_SCHEMA_INDEX = 6 };

// Minimal forward compact-Thrift reader (a trimmed copy of the one in
// parquet_to_modular.cc, enough to navigate the modules this tool reads).
class Reader {
 public:
  Reader(const char* d, size_t n) : p_(d), end_(d + n) {}
  const char* cur() const { return p_; }
  bool ok() const { return ok_; }
  struct Field { int16_t id; uint8_t type; };
  Field NextField() {
    if (!Avail(1)) { ok_ = false; return {0, T_STOP}; }
    uint8_t b = static_cast<uint8_t>(*p_++);
    if (b == 0) return {0, T_STOP};
    int delta = b >> 4;
    int16_t id = delta ? static_cast<int16_t>(last_id_ + delta) : static_cast<int16_t>(ZigZag());
    last_id_ = id;
    return {id, static_cast<uint8_t>(b & 0x0F)};
  }
  struct ListHdr { uint8_t elem; int32_t size; };
  ListHdr List() {
    uint8_t b = Byte();
    int32_t size = b >> 4;
    if (size == 15) size = static_cast<int32_t>(Varint());
    return {static_cast<uint8_t>(b & 0x0F), size};
  }
  int32_t I32() { return static_cast<int32_t>(ZigZag()); }
  int64_t I64() { return ZigZag(); }
  const char* BinarySpan(uint32_t* len) {
    uint64_t n = Varint();
    const char* s = p_;
    if (!Avail(n)) { ok_ = false; *len = 0; return s; }
    p_ += n; *len = static_cast<uint32_t>(n);
    return s;
  }
  std::string Binary() { uint32_t n; const char* s = BinarySpan(&n); return std::string(s, n); }
  uint8_t Byte() { if (!Avail(1)) { ok_ = false; return 0; } return static_cast<uint8_t>(*p_++); }
  int16_t StructBegin() { int16_t s = last_id_; last_id_ = 0; return s; }
  void StructEnd(int16_t s) { last_id_ = s; }
  void Skip(uint8_t type) {
    switch (type) {
      case T_TRUE: case T_FALSE: break;
      case T_I8: Adv(1); break;
      case T_I16: case T_I32: case T_I64: Varint(); break;
      case T_DOUBLE: Adv(8); break;
      case T_BINARY: Adv(static_cast<size_t>(Varint())); break;
      case T_LIST: case T_SET: {
        ListHdr h = List();
        if (h.elem == T_TRUE || h.elem == T_FALSE) { Adv(static_cast<size_t>(h.size)); break; }
        for (int32_t i = 0; i < h.size && ok_; ++i) Skip(h.elem);
        break;
      }
      case T_MAP: {
        uint64_t n = Varint();
        if (n) { uint8_t kv = Byte(); for (uint64_t i = 0; i < n && ok_; ++i) { Skip(kv >> 4); Skip(kv & 0x0F); } }
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
 private:
  bool Avail(size_t n) const { return static_cast<size_t>(end_ - p_) >= n; }
  void Adv(size_t n) { if (Avail(n)) p_ += n; else ok_ = false; }
  uint64_t Varint() {
    uint64_t v = 0; int s = 0;
    while (Avail(1) && s < 64) { uint8_t b = static_cast<uint8_t>(*p_++); v |= uint64_t(b & 0x7F) << s; if (!(b & 0x80)) return v; s += 7; }
    ok_ = false; return v;
  }
  int64_t ZigZag() { uint64_t v = Varint(); return int64_t(v >> 1) ^ -int64_t(v & 1); }
  const char* p_; const char* end_; int16_t last_id_ = 0; bool ok_ = true;
};

// A dense BITSET ArrayPage decoded to a bit-packed value stream.
struct DenseArray {
  const uint8_t* data = nullptr;
  int32_t num_values = 0;
  int value_bit_width = 0;
  uint64_t At(size_t i) const { return pfb::ExtractBits(data, i, value_bit_width); }
};

// Decode an EncodedArray struct (cursor at struct start). Only the dense-BITSET shape
// this module uses is needed: num_values (1), values (2) = [u8 value_bit_width][packed].
DenseArray ReadDenseArray(Reader& r) {
  DenseArray a;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_I32) {
      a.num_values = r.I32();                               // num_values
    } else if (f.id == 2 && f.type == T_BINARY) {           // values = [u8 width][packed]
      uint32_t n; const uint8_t* d = reinterpret_cast<const uint8_t*>(r.BinarySpan(&n));
      a.value_bit_width = n > 0 ? d[0] : 0;
      a.data = d + 1;
    } else r.Skip(f.type);                                  // encoding(3)/presence(4) ignored
  }
  r.StructEnd(s);
  return a;
}

std::string Lower(std::string x) {
  for (char& c : x) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
  return x;
}
uint64_t Fnv1a64(const std::string& s) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (unsigned char c : s) { h ^= c; h *= 0x100000001b3ULL; }
  return h;
}

// Read one SchemaElement's name (cursor at struct start).
std::string ElemName(Reader& r) {
  std::string name;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 4 && f.type == T_BINARY) name = r.Binary();
    else r.Skip(f.type);
  }
  r.StructEnd(s);
  return name;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) { std::fprintf(stderr, "usage: %s file.si.modular\n", argv[0]); return 2; }
  try {
    std::ifstream in(argv[1], std::ios::binary);
    if (!in) throw std::runtime_error("cannot open input");
    std::string buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (buf.size() < 12) throw std::runtime_error("file too small");

    // Navigability trailer: MFT1 (12 bytes) or MFP1 (20 bytes, with modular_start).
    int64_t modular_start = 0, root_off = 0;
    auto le64 = [&](size_t at) {
      uint64_t v = 0; for (int b = 0; b < 8; ++b) v |= uint64_t(uint8_t(buf[at + b])) << (8 * b);
      return static_cast<int64_t>(v);
    };
    if (!std::memcmp(buf.data() + buf.size() - 4, "MFT1", 4)) {
      root_off = le64(buf.size() - 12);
    } else if (!std::memcmp(buf.data() + buf.size() - 4, "MFP1", 4)) {
      modular_start = le64(buf.size() - 20);
      root_off = le64(buf.size() - 12);
    } else {
      throw std::runtime_error("no MFT1/MFP1 trailer");
    }
    const char* base = buf.data() + modular_start;

    // Root directory: field 6 = list<ModuleDirectoryEntry{1:kind, 2:location{1:off,2:len}}>.
    int64_t schema_off = -1, si_off = -1, schema_len = 0, si_len = 0;
    {
      Reader r(base + root_off, buf.size() - modular_start - root_off);
      int16_t s = r.StructBegin();
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == 6 && f.type == T_LIST) {
          Reader::ListHdr h = r.List();
          for (int32_t i = 0; i < h.size; ++i) {
            int32_t kind = -1; int64_t off = 0, len = 0;
            int16_t s2 = r.StructBegin();
            for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
              if (g.id == 1 && g.type == T_I32) kind = r.I32();
              else if (g.id == 2 && g.type == T_STRUCT) {
                int16_t s3 = r.StructBegin();
                for (Reader::Field e = r.NextField(); e.type != T_STOP; e = r.NextField()) {
                  if (e.id == 1) off = r.I64(); else if (e.id == 2) len = r.I64(); else r.Skip(e.type);
                }
                r.StructEnd(s3);
              } else r.Skip(g.type);
            }
            r.StructEnd(s2);
            if (kind == K_SCHEMA) { schema_off = off; schema_len = len; }
            else if (kind == K_SCHEMA_INDEX) { si_off = off; si_len = len; }
          }
        } else r.Skip(f.type);
      }
      r.StructEnd(s);
    }
    if (si_off < 0) throw std::runtime_error("no SCHEMA_INDEX module (was --schema-index used?)");
    if (schema_off < 0) throw std::runtime_error("no SCHEMA module");
    const char* schema_blob = base + schema_off;

    // Decode the SCHEMA_INDEX module.
    DenseArray table, eoff, lei, par;
    int disc_bits = 0, ord_bits = 0; bool has_lei = false, has_par = false;
    {
      Reader r(base + si_off, static_cast<size_t>(si_len));
      int16_t s = r.StructBegin();
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == 1 && f.type == T_STRUCT) table = ReadDenseArray(r);
        else if (f.id == 2 && f.type == T_I8) disc_bits = r.Byte();
        else if (f.id == 3 && f.type == T_I8) ord_bits = r.Byte();
        else if (f.id == 4 && f.type == T_STRUCT) eoff = ReadDenseArray(r);
        else if (f.id == 5 && f.type == T_STRUCT) { lei = ReadDenseArray(r); has_lei = true; }
        else if (f.id == 6 && f.type == T_STRUCT) { par = ReadDenseArray(r); has_par = true; }
        else r.Skip(f.type);
      }
      r.StructEnd(s);
    }
    const uint64_t num_slots = static_cast<uint64_t>(table.num_values);
    const uint64_t ordmask = pfb::LowMask(ord_bits);

    // Rebuild leaf paths + ordinals by walking the SCHEMA module directly.
    std::vector<std::string> names; std::vector<int> nchild;
    {
      Reader r(schema_blob, static_cast<size_t>(schema_len));
      for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
        if (f.id == 1 && f.type == T_LIST) {
          Reader::ListHdr h = r.List();
          for (int32_t i = 0; i < h.size; ++i) {
            std::string nm; int nc = 0;
            int16_t s = r.StructBegin();
            for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
              if (g.id == 4 && g.type == T_BINARY) nm = r.Binary();
              else if (g.id == 5 && g.type == T_I32) nc = r.I32();
              else r.Skip(g.type);
            }
            r.StructEnd(s);
            names.push_back(std::move(nm)); nchild.push_back(nc);
          }
          break;
        } else r.Skip(f.type);
      }
    }
    const int Nel = static_cast<int>(names.size());
    std::vector<int> parent(Nel, -1);
    { std::vector<std::pair<int, int>> st;
      for (int i = 0; i < Nel; ++i) {
        if (!st.empty()) { parent[i] = st.back().first; st.back().second--; }
        if (nchild[i] > 0) st.push_back({i, nchild[i]});
        while (!st.empty() && st.back().second == 0) st.pop_back();
      } }
    std::vector<std::string> path(Nel);
    for (int i = 1; i < Nel; ++i) {
      std::string seg = Lower(names[i]); int p = parent[i];
      path[i] = (p <= 0) ? seg : path[p] + std::string(1, '\0') + seg;
    }
    std::vector<int> leaf_elem; std::vector<std::string> leaf_path;
    for (int i = 1; i < Nel; ++i)
      if (nchild[i] == 0) { leaf_elem.push_back(i); leaf_path.push_back(path[i]); }
    const int leaves = static_cast<int>(leaf_elem.size());

    // Reconstruct a leaf element's full dotted path: walk parent_ordinals up from the
    // leaf, reading each ancestor's name via element_offsets, stopping at a direct
    // child of the root (root excluded). Flat schema: the leaf's own name is the path.
    auto build_path = [&](int elem) {
      std::vector<std::string> segs;
      for (int cur = elem;;) {
        Reader er(schema_blob + eoff.At(cur), static_cast<size_t>(schema_len) - eoff.At(cur));
        segs.push_back(Lower(ElemName(er)));
        int p = has_par ? static_cast<int>(par.At(cur)) : 0;
        if (p == 0) break;
        cur = p;
      }
      std::string out;
      for (int i = static_cast<int>(segs.size()) - 1; i >= 0; --i) {
        if (!out.empty()) out += '\0';
        out += segs[i];
      }
      return out;
    };

    // Resolve one path via the hash table; returns leaf ordinal or -1.
    auto resolve = [&](const std::string& q) -> int {
      uint64_t h = Fnv1a64(q);
      uint64_t home = h & (num_slots - 1);
      uint64_t want_disc = h >> (64 - disc_bits);
      for (uint64_t probe = 0; probe < num_slots; ++probe) {
        uint64_t v = table.At((home + probe) & (num_slots - 1));
        if (v == 0) return -1;                          // empty slot: absent
        if ((v >> ord_bits) != want_disc) continue;     // discriminator mismatch
        int ord = static_cast<int>((v & ordmask) - 1);
        int idx = has_lei ? static_cast<int>(lei.At(ord)) : ord + 1;  // flat: element ord+1
        if (build_path(idx) == q) return ord;           // confirmed via full path
      }
      return -1;
    };

    // Every leaf must resolve to its own ordinal.
    int ok = 0;
    for (int ord = 0; ord < leaves; ++ord) {
      if (resolve(leaf_path[ord]) == ord) ++ok;
      else std::fprintf(stderr, "MISMATCH leaf ordinal %d (%s)\n", ord, leaf_path[ord].c_str());
    }
    // A few absent names must not resolve.
    int absent_ok = 0;
    const char* absent[] = {"__no_such_column__", "zzz_missing", "col_999999999"};
    for (const char* a : absent) if (resolve(a) == -1) ++absent_ok;

    std::printf("file            %s\n", argv[1]);
    std::printf("slots           %llu (%d disc bits, %d ord bits)\n",
                static_cast<unsigned long long>(num_slots), disc_bits, ord_bits);
    std::printf("schema_elements %d, leaves %d, leaf_element_indexes %s\n",
                Nel, leaves, has_lei ? "present" : "omitted (flat)");
    std::printf("leaf resolve    OK %d/%d\n", ok, leaves);
    std::printf("absent resolve  OK %d/3\n", absent_ok);
    bool pass = (ok == leaves) && (absent_ok == 3) && leaves > 0;
    std::printf("%s\n", pass ? "PASS" : "FAIL");
    return pass ? 0 : 1;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
