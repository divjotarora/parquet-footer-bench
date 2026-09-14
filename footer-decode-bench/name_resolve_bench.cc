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

// name_resolve_bench -- does a persisted schema name hash earn its place?
//
// Resolving a query's column *names* to ordinals is the step before placement /
// stats resolution. This times it two ways on a footer that carries a persisted
// name hash -- either a jump-table footer (Will's SchemaLayout.NameHashTable) or a
// modular footer with a SCHEMA_INDEX module (parquet_to_modular --schema-index):
//
//   walk  no persisted hash: parse the whole schema (every SchemaElement) to build
//         name -> ordinal, then look up the K queried names. O(all columns).
//   hash  persisted table: FNV-1a-64 probe per queried name, confirming the candidate
//         by reconstructing its full dotted path from the schema via the per-element
//         offsets (and parent chain, for nested schemas). O(projected) -- never parses
//         the other columns' schema.
//
// Both return the same ordinals (cross-checked). The input format is auto-detected
// from its trailer (MFT1/MFP1 = modular; otherwise a jump-table footer). Build & run:
//   cmake --build build -j            # target name_resolve_bench
//   ./build/name_resolve_bench wide.{jt,si}.parquet [num_queried|--sweep]

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "thrift_codec.h"

using namespace fdb;

namespace {

// FileMetaData / SchemaElement / jump-table field ids.
enum { FMD_SCHEMA = 2, FMD_FOOTER_INDEX_POINTER = 10, SE_NAME = 4, SE_NUM_CHILDREN = 5,
       IDX_SCHEMA_LAYOUT = 5, SL_NUM_ELEMENTS = 1, SL_OFFSETS = 2, SL_NAME_HASH = 4,
       NHT_NUM_SLOTS = 1, NHT_DISC_BITS = 2, NHT_TABLE = 3 };
// Modular root-directory / SCHEMA_INDEX field ids.
enum { K_SCHEMA = 0, K_SCHEMA_INDEX = 6, MF_MODULES = 6, MDE_KIND = 1, MDE_LOCATION = 2,
       ML_OFFSET = 1, ML_LENGTH = 2, SI_HASH = 1, SI_DISC = 2, SI_ORD = 3, SI_ELEM_OFF = 4,
       SI_LEAF_ELEM = 5, SI_PARENT = 6 };

uint64_t Fnv1a64(const std::string& s) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (unsigned char c : s) { h ^= c; h *= 0x100000001b3ULL; }
  return h;
}
std::string Lower(std::string o) {
  for (char& c : o) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
  return o;
}

template <typename Fn>
double TimeUs(Fn fn) {
  using clock = std::chrono::steady_clock;
  long iters = 1;
  for (;;) {
    auto t0 = clock::now();
    for (long i = 0; i < iters; ++i) fn();
    double ns = std::chrono::duration<double, std::nano>(clock::now() - t0).count();
    if (ns > 200e6 || iters > (1L << 30)) return ns / iters / 1000.0;
    iters *= 2;
  }
}

// One decoded ArrayPage (dense BITSET only, which is all these fields use).
struct Dense { const uint8_t* data = nullptr; int32_t n = 0; int width = 0;
  uint64_t At(size_t i) const { return ExtractBits(data, i, width); } };

Dense ReadDense(Reader& r) {
  Dense a;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == 1 && f.type == T_I32) a.n = r.I32();                  // num_values
    else if (f.id == 2 && f.type == T_BINARY) {                       // values = [u8 width][packed]
      Span sp = r.BinarySpan();
      a.width = sp.size > 0 ? static_cast<uint8_t>(sp.data[0]) : 0;
      a.data = reinterpret_cast<const uint8_t*>(sp.data) + 1;
    } else r.Skip(f.type);                                            // encoding(3)/presence(4) ignored
  }
  r.StructEnd(s);
  return a;
}

// Everything decoded once, format-independent.
struct Ctx {
  std::string footer;
  const char* schema_base = nullptr;      // element offsets are relative to this
  int64_t schema_list_pos = 0;            // Reader().List() at footer.data()+this walks the schema
  // hash table
  const uint8_t* table = nullptr;
  int num_slots = 0, disc_bits = 0, ord_bits = 0;
  bool table_bitpacked = false;           // modular: ExtractBits(width); jt: LE bytes
  int slot_bits = 0, slot_bytes = 0;
  bool hash_is_leaf_ordinal = false;      // modular stores leaf ordinal; jt stores element index
  // structure
  std::vector<int64_t> elem_off;          // per element index
  std::vector<int> leaf_elem;             // leaf ordinal -> element index (empty = flat)
  std::vector<int> parent;                // element -> parent element (empty = flat)
  // queries / fidelity
  std::vector<std::string> leaf_paths;    // canonical: lowercase, NUL-joined, root excluded
  std::string aux;                        // owns a copied hash table (jump-table format)
};

uint64_t SlotAt(const Ctx& c, uint64_t i) {
  if (c.table_bitpacked) return ExtractBits(c.table, i, c.slot_bits);
  uint64_t v = 0;
  const uint8_t* p = c.table + i * c.slot_bytes;
  for (int b = 0; b < c.slot_bytes; ++b) v |= static_cast<uint64_t>(p[b]) << (8 * b);
  return v;
}

// Read a SchemaElement's name (cursor at struct start).
std::string ElemName(Reader& r) {
  std::string name;
  int16_t s = r.StructBegin();
  for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
    if (f.id == SE_NAME && f.type == T_BINARY) name = r.Binary(); else r.Skip(f.type);
  }
  r.StructEnd(s);
  return name;
}

// Parse the schema list (Reader positioned so List() reads the elements), returning
// each element's canonical path (empty for the root) and num_children.
void ParseSchema(const char* list_ptr, size_t avail,
                 std::vector<std::string>* path, std::vector<int>* nchild) {
  Reader r(list_ptr, avail);
  Reader::ListHdr h = r.List();
  std::vector<std::string> names(h.size);
  nchild->assign(h.size, 0);
  for (int32_t i = 0; i < h.size; ++i) {
    int16_t s = r.StructBegin();
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == SE_NAME && f.type == T_BINARY) names[i] = r.Binary();
      else if (f.id == SE_NUM_CHILDREN && f.type == T_I32) (*nchild)[i] = r.I32();
      else r.Skip(f.type);
    }
    r.StructEnd(s);
  }
  const int N = h.size;
  std::vector<int> parent(N, -1);
  { std::vector<std::pair<int, int>> st;
    for (int i = 0; i < N; ++i) {
      if (!st.empty()) { parent[i] = st.back().first; st.back().second--; }
      if ((*nchild)[i] > 0) st.push_back({i, (*nchild)[i]});
      while (!st.empty() && st.back().second == 0) st.pop_back();
    } }
  path->assign(N, std::string());
  for (int i = 1; i < N; ++i) {
    std::string seg = Lower(names[i]); int p = parent[i];
    (*path)[i] = (p <= 0) ? seg : (*path)[p] + std::string(1, '\0') + seg;
  }
}

int64_t Le64(const char* p) {
  uint64_t v = 0; for (int b = 0; b < 8; ++b) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[b])) << (8 * b);
  return static_cast<int64_t>(v);
}

Ctx LoadModular(std::string all) {
  Ctx c; c.footer = std::move(all);
  const std::string& b = c.footer;
  int64_t modular_start = 0, root_off = 0;
  if (!std::memcmp(b.data() + b.size() - 4, "MFT1", 4)) root_off = Le64(b.data() + b.size() - 12);
  else { modular_start = Le64(b.data() + b.size() - 20); root_off = Le64(b.data() + b.size() - 12); }
  const char* base = b.data() + modular_start;
  // root directory -> SCHEMA + SCHEMA_INDEX locations
  int64_t schema_off = -1, si_off = -1, si_len = 0;
  { Reader r(base + root_off, b.size() - modular_start - root_off);
    int16_t s = r.StructBegin();
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == MF_MODULES && f.type == T_LIST) {
        Reader::ListHdr h = r.List();
        for (int32_t i = 0; i < h.size; ++i) {
          int32_t kind = -1; int64_t off = 0, len = 0;
          int16_t s2 = r.StructBegin();
          for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
            if (g.id == MDE_KIND && g.type == T_I32) kind = r.I32();
            else if (g.id == MDE_LOCATION && g.type == T_STRUCT) {
              int16_t s3 = r.StructBegin();
              for (Reader::Field e = r.NextField(); e.type != T_STOP; e = r.NextField()) {
                if (e.id == ML_OFFSET) off = r.I64(); else if (e.id == ML_LENGTH) len = r.I64(); else r.Skip(e.type);
              }
              r.StructEnd(s3);
            } else r.Skip(g.type);
          }
          r.StructEnd(s2);
          if (kind == K_SCHEMA) schema_off = off;
          else if (kind == K_SCHEMA_INDEX) { si_off = off; si_len = len; }
        }
      } else r.Skip(f.type);
    }
    r.StructEnd(s); }
  if (si_off < 0) throw std::runtime_error("modular footer has no SCHEMA_INDEX module");
  c.schema_base = base + schema_off;
  c.schema_list_pos = (base - b.data()) + schema_off + 1;   // +1 skips the field-1 list header byte
  // SCHEMA_INDEX module
  Dense table, eoff, lei, par; bool has_lei = false, has_par = false;
  { Reader r(base + si_off, static_cast<size_t>(si_len));
    int16_t s = r.StructBegin();
    for (Reader::Field f = r.NextField(); f.type != T_STOP; f = r.NextField()) {
      if (f.id == SI_HASH && f.type == T_STRUCT) table = ReadDense(r);
      else if (f.id == SI_DISC && f.type == T_I8) c.disc_bits = r.U8();
      else if (f.id == SI_ORD && f.type == T_I8) c.ord_bits = r.U8();
      else if (f.id == SI_ELEM_OFF && f.type == T_STRUCT) eoff = ReadDense(r);
      else if (f.id == SI_LEAF_ELEM && f.type == T_STRUCT) { lei = ReadDense(r); has_lei = true; }
      else if (f.id == SI_PARENT && f.type == T_STRUCT) { par = ReadDense(r); has_par = true; }
      else r.Skip(f.type);
    }
    r.StructEnd(s); }
  c.table = table.data; c.num_slots = table.n; c.slot_bits = table.width;
  c.table_bitpacked = true; c.hash_is_leaf_ordinal = true;
  c.elem_off.resize(eoff.n);
  for (int i = 0; i < eoff.n; ++i) c.elem_off[i] = static_cast<int64_t>(eoff.At(i));
  if (has_lei) { c.leaf_elem.resize(lei.n); for (int i = 0; i < lei.n; ++i) c.leaf_elem[i] = static_cast<int>(lei.At(i)); }
  if (has_par) { c.parent.resize(par.n); for (int i = 0; i < par.n; ++i) c.parent[i] = static_cast<int>(par.At(i)); }
  return c;
}

Ctx LoadJumpTable(std::string all) {
  Ctx c;
  uint32_t flen; std::memcpy(&flen, all.data() + all.size() - 8, 4);
  c.footer = all.substr(all.size() - 8 - flen, flen);
  const std::string& f = c.footer;
  c.schema_base = f.data();          // jump-table offsets are relative to FileMetaData start
  int64_t fmd_length = 0;
  { Reader r(f.data(), f.size());
    for (Reader::Field fld = r.NextField(); fld.type != T_STOP; fld = r.NextField()) {
      if (fld.id == FMD_SCHEMA && fld.type == T_LIST) { c.schema_list_pos = r.offset(); r.Skip(T_LIST); }
      else if (fld.id == FMD_FOOTER_INDEX_POINTER && fld.type == T_BINARY) {
        std::string p = r.Binary(); std::memcpy(&fmd_length, p.data() + 17, 8);
      } else r.Skip(fld.type);
    } }
  // SchemaLayout in the index: offsets (whole-byte) + NameHashTable.
  Reader r(f.data() + fmd_length, f.size() - static_cast<size_t>(fmd_length));
  for (Reader::Field fld = r.NextField(); fld.type != T_STOP; fld = r.NextField()) {
    if (fld.id != IDX_SCHEMA_LAYOUT || fld.type != T_STRUCT) { r.Skip(fld.type); continue; }
    int nse = 0; std::string offs, table;
    int16_t s = r.StructBegin();
    for (Reader::Field g = r.NextField(); g.type != T_STOP; g = r.NextField()) {
      if (g.id == SL_NUM_ELEMENTS && g.type == T_I32) nse = r.I32();
      else if (g.id == SL_OFFSETS && g.type == T_BINARY) offs = r.Binary();
      else if (g.id == SL_NAME_HASH && g.type == T_STRUCT) {
        int16_t s2 = r.StructBegin();
        for (Reader::Field h = r.NextField(); h.type != T_STOP; h = r.NextField()) {
          if (h.id == NHT_NUM_SLOTS && h.type == T_I32) c.num_slots = r.I32();
          else if (h.id == NHT_DISC_BITS && h.type == T_I32) c.disc_bits = r.I32();
          else if (h.id == NHT_TABLE && h.type == T_BINARY) table = r.Binary();
          else r.Skip(h.type);
        }
        r.StructEnd(s2);
      } else r.Skip(g.type);
    }
    r.StructEnd(s);
    // decode whole-byte element offsets
    if (nse > 0 && !offs.empty()) {
      int bw = static_cast<int>(offs.size() / nse);
      c.elem_off.assign(nse, 0);
      for (int i = 0; i < nse; ++i) { int64_t v = 0;
        for (int b = 0; b < bw; ++b) v |= static_cast<int64_t>(static_cast<uint8_t>(offs[i * bw + b])) << (8 * b);
        c.elem_off[i] = v; }
    }
    if (!table.empty() && c.num_slots > 0) {
      c.slot_bytes = static_cast<int>(table.size() / c.num_slots);
      c.ord_bits = c.slot_bytes * 8 - c.disc_bits;
      c.aux = std::move(table);   // stable buffer; footer is not mutated
      c.table = reinterpret_cast<const uint8_t*>(c.aux.data());
    }
    break;
  }
  c.table_bitpacked = false; c.hash_is_leaf_ordinal = false;  // jt stores element index; fixtures are flat
  return c;
}

// element index of a raw slot ordinal.
int ElementIndex(const Ctx& c, int raw) {
  if (!c.hash_is_leaf_ordinal) return raw;                   // jt: ordinal is the element index
  if (c.leaf_elem.empty()) return raw + 1;                   // modular flat: leaf c -> element c+1
  return c.leaf_elem[raw];
}

// Reconstruct one element's full dotted path (parent chain up to a direct child of
// the root; root excluded). Flat schemas have no parent array: the path is the name.
std::string BuildPath(const Ctx& c, int elem) {
  std::vector<std::string> segs;
  const char* buf_end = c.footer.data() + c.footer.size();
  for (int cur = elem;;) {
    const char* at = c.schema_base + c.elem_off[cur];
    Reader r(at, static_cast<size_t>(buf_end - at));
    segs.push_back(Lower(ElemName(r)));
    int p = c.parent.empty() ? 0 : c.parent[cur];
    if (p == 0) break;
    cur = p;
  }
  std::string out;
  for (int i = static_cast<int>(segs.size()) - 1; i >= 0; --i) { if (!out.empty()) out += '\0'; out += segs[i]; }
  return out;
}

// walk: parse the whole schema, build path -> element index, look up the queries.
std::vector<int> WalkResolve(const Ctx& c, const std::vector<std::string>& queries) {
  std::vector<std::string> path; std::vector<int> nchild;
  ParseSchema(c.footer.data() + c.schema_list_pos, c.footer.size() - c.schema_list_pos, &path, &nchild);
  std::unordered_map<std::string, int> byname;
  byname.reserve(path.size() * 2);
  for (int i = 1; i < static_cast<int>(path.size()); ++i) if (!path[i].empty()) byname.emplace(path[i], i);
  std::vector<int> out; out.reserve(queries.size());
  for (const std::string& q : queries) { auto it = byname.find(q); out.push_back(it == byname.end() ? -1 : it->second); }
  return out;
}

// hash: FNV probe, confirm by reconstructing the candidate's full path. O(projected).
std::vector<int> HashResolve(const Ctx& c, const std::vector<std::string>& queries) {
  const uint64_t ordmask = LowMask(c.ord_bits);
  std::vector<int> out; out.reserve(queries.size());
  for (const std::string& q : queries) {
    uint64_t h = Fnv1a64(q);
    uint64_t home = h & (c.num_slots - 1);
    uint64_t want_disc = h >> (64 - c.disc_bits);
    int found = -1;
    for (uint32_t probe = 0; probe < static_cast<uint32_t>(c.num_slots); ++probe) {
      uint64_t v = SlotAt(c, (home + probe) & (c.num_slots - 1));
      if (v == 0) break;
      if ((v >> c.ord_bits) != want_disc) continue;
      int elem = ElementIndex(c, static_cast<int>((v & ordmask) - 1));
      if (BuildPath(c, elem) == q) { found = elem; break; }
    }
    out.push_back(found);
  }
  return out;
}

Ctx Load(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open " + path);
  std::string all((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  if (all.size() < 12) throw std::runtime_error("file too small");
  const char* tail = all.data() + all.size() - 4;
  if (!std::memcmp(tail, "MFT1", 4) || !std::memcmp(tail, "MFP1", 4)) return LoadModular(std::move(all));
  if (!std::memcmp(tail, "PAR1", 4)) return LoadJumpTable(std::move(all));
  throw std::runtime_error("unrecognized trailer (want MFT1/MFP1/PAR1)");
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) { std::fprintf(stderr, "usage: %s footer.{jt,si}.parquet [num_queried|--sweep]\n", argv[0]); return 2; }
  const std::string path = argv[1];
  bool sweep = false; int nq = 1;
  if (argc >= 3) { std::string a = argv[2]; if (a == "--sweep") sweep = true; else nq = std::atoi(a.c_str()); }
  try {
    Ctx c = Load(path);
    // canonical leaf paths, for query selection and fidelity (from the schema walk).
    std::vector<std::string> epath; std::vector<int> enchild;
    ParseSchema(c.footer.data() + c.schema_list_pos, c.footer.size() - c.schema_list_pos, &epath, &enchild);
    std::vector<std::string> leaves;
    for (int i = 1; i < static_cast<int>(epath.size()); ++i)
      if (!epath[i].empty() && enchild[i] == 0) leaves.push_back(epath[i]);
    int C = static_cast<int>(leaves.size());
    if (C == 0) throw std::runtime_error("no leaf columns");
    auto pick = [&](int k) {
      std::mt19937 rng(1234);
      std::vector<int> idx(C); for (int i = 0; i < C; ++i) idx[i] = i;
      std::shuffle(idx.begin(), idx.end(), rng);
      std::vector<std::string> q; for (int i = 0; i < k && i < C; ++i) q.push_back(leaves[idx[i]]);
      return q;
    };

    if (sweep) {
      std::printf("queried,walk_us,hash_us\n");
      std::vector<int> ks; for (int k = 1; k < C; k *= 4) ks.push_back(k); ks.push_back(C);
      for (int k : ks) {
        auto q = pick(k);
        if (WalkResolve(c, q) != HashResolve(c, q)) throw std::runtime_error("walk vs hash disagree");
        double w = TimeUs([&] { auto v = WalkResolve(c, q); if (v.empty()) std::abort(); });
        double hh = TimeUs([&] { auto v = HashResolve(c, q); if (v.empty()) std::abort(); });
        std::printf("%d,%.3f,%.3f\n", k, w, hh);
      }
      return 0;
    }

    int k = std::max(1, std::min(nq, C));
    auto q = pick(k);
    if (WalkResolve(c, q) != HashResolve(c, q)) throw std::runtime_error("walk vs hash disagree");
    double w = TimeUs([&] { auto v = WalkResolve(c, q); if (v.empty()) std::abort(); });
    double hh = TimeUs([&] { auto v = HashResolve(c, q); if (v.empty()) std::abort(); });
    std::printf("file            %s\n", path.c_str());
    std::printf("format          %s\n", c.table_bitpacked ? "modular (SCHEMA_INDEX)" : "jump-table");
    std::printf("columns         %d\n", C);
    std::printf("queried names   %d\n\n", k);
    std::printf("%-8s %14s\n", "resolve", "us/op");
    std::printf("%-8s %14.3f\n", "walk", w);
    std::printf("%-8s %14.3f   <- %.1fx vs walk\n", "hash", hh, hh > 0 ? w / hh : 0);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
