// Shopping Sessions: single-binary C++17 pipeline. See rules.md.
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
using namespace std;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAY = 86400000, WEEK = 7 * DAY, GAP = 1800000, LATE = DAY;
static int NCPU = 8;
static double T0;
static double now() {
  timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}
static void logt(const char* m) { fprintf(stderr, "[%7.3fs] %s\n", now() - T0, m); }

template <class F> static void parallel_for(size_t n, F fn) {
  atomic<size_t> next{0};
  auto work = [&](int t) {
    for (;;) { size_t i = next.fetch_add(1); if (i >= n) break; fn(i, t); }
  };
  vector<thread> th;
  for (int t = 1; t < NCPU; t++) th.emplace_back(work, t);
  work(0);
  for (auto& x : th) x.join();
}

// ---------- hashing ----------
static inline u64 mum(u64 a, u64 b) { __uint128_t r = (__uint128_t)a * b; return (u64)r ^ (u64)(r >> 64); }
static inline u64 hbytes(const char* p, size_t n) {
  u64 h = 0x9E3779B97F4A7C15ULL ^ (n * 0xff51afd7ed558ccdULL);
  while (n >= 8) { u64 w; memcpy(&w, p, 8); h = mum(h ^ w, 0xa0761d6478bd642fULL); p += 8; n -= 8; }
  u64 w = 0; if (n) memcpy(&w, p, n);
  h = mum(h ^ w, 0xe7037ed1a0b428dbULL);
  return mum(h, 0x8ebc6af09c88c6e3ULL) | 1;
}

struct SV { const char* p; const char* e; size_t n() const { return e - p; } };
static inline u64 uid(SV s) { return hbytes(s.p, s.n()); }
static inline bool sveq(SV a, const char* lit) { size_t n = strlen(lit); return a.n() == n && memcmp(a.p, lit, n) == 0; }
static inline bool svless(const char* ap, size_t an, const char* bp, size_t bn) {
  int c = memcmp(ap, bp, an < bn ? an : bn);
  return c < 0 || (c == 0 && an < bn);
}

// ---------- files ----------
struct File { const char* b; size_t n; };
static File read_whole(int fd, size_t n) {
  char* buf = (char*)malloc(n);
  if (!buf) return {nullptr, 0};
  size_t got = 0;
  while (got < n) {
    ssize_t r = pread(fd, buf + got, n - got, got);
    if (r <= 0) break;
    got += (size_t)r;
  }
  return {buf, got};
}
static vector<File> load_dir(const string& dir) {
  vector<string> names;
  if (DIR* d = opendir(dir.c_str())) {
    while (dirent* e = readdir(d)) {
      string s = e->d_name;
      if (s.size() > 4 && s.compare(s.size() - 4, 4, ".csv") == 0) names.push_back(dir + "/" + s);
    }
    closedir(d);
  }
  sort(names.begin(), names.end());
  vector<File> fs(names.size());
  parallel_for(names.size(), [&](size_t i, int) {
    fs[i] = {nullptr, 0};
    int fd = open(names[i].c_str(), O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) == 0 && st.st_size > 0) {
      void* m = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
      if (m != MAP_FAILED) fs[i] = {(const char*)m, (size_t)st.st_size};
      else fs[i] = read_whole(fd, (size_t)st.st_size);
    }
    close(fd);
  });
  return fs;
}
static File load_file(const string& path) {
  File f{nullptr, 0};
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return f;
  struct stat st;
  if (fstat(fd, &st) == 0 && st.st_size > 0) {
    void* m = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (m != MAP_FAILED) f = {(const char*)m, (size_t)st.st_size};
    else f = read_whole(fd, (size_t)st.st_size);
  }
  close(fd);
  return f;
}

struct Chunk { const char* b; const char* e; };
static vector<Chunk> make_chunks(const vector<File>& fs, size_t target = 1 << 22) {
  vector<Chunk> out;
  for (auto& f : fs) {
    if (!f.b) continue;
    const char* end = f.b + f.n;
    const char* nl = (const char*)memchr(f.b, '\n', f.n);
    if (!nl) continue;  // header only
    const char* pos = nl + 1;
    while (pos < end) {
      const char* next = pos + target;
      if (next >= end) { out.push_back({pos, end}); break; }
      const char* q = (const char*)memchr(next - 1, '\n', end - (next - 1));
      if (!q) { out.push_back({pos, end}); break; }
      next = q + 1;
      out.push_back({pos, next});
      pos = next;
    }
  }
  return out;
}
template <class F> static inline void for_lines(const char* b, const char* e, F f) {
  const char* p = b;
  while (p < e) {
    const char* nl = (const char*)memchr(p, '\n', e - p);
    const char* le = nl ? nl : e;
    f(p, le);
    if (!nl) break;
    p = nl + 1;
  }
}

// CSV line with exactly n fields; each field is bare or wrapped in quotes as a whole.
static inline bool parse_fields(const char* s, const char* e, int n, SV* out) {
  int k = 0;
  const char* p = s;
  for (;;) {
    if (k >= n) return false;
    const char *fs, *fe;
    if (p < e && *p == '"') {
      fs = p + 1;
      const char* q = fs;
      while (q < e && *q != '"' && *q != ',') q++;
      if (q >= e || *q != '"') return false;
      fe = q;
      p = q + 1;
      if (p < e && *p != ',') return false;
    } else {
      fs = p;
      const char* q = p;
      while (q < e && *q != ',') { if (*q == '"') return false; q++; }
      fe = q;
      p = q;
    }
    out[k++] = {fs, fe};
    if (p >= e) break;
    p++;
  }
  return k == n;
}

// ---------- timestamps ----------
static inline bool dig(char c) { return (unsigned)(c - '0') < 10; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
static i64 days_from_civil(i64 y, int m, int d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}
static bool parse19(const char* s, i64& ms) {
  static const int dp[] = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
  for (int i : dp) if (!dig(s[i])) return false;
  if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
  int y = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
  int mo = d2(s + 5), d = d2(s + 8), h = d2(s + 11), mi = d2(s + 14), se = d2(s + 17);
  if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || se > 59) return false;
  static const int dim[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int md = dim[mo - 1];
  if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
  if (d > md) return false;
  ms = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000;
  return true;
}
static bool parse_ts(const char* s, const char* e, i64& out) {
  size_t n = e - s;
  if (n == 0) return false;
  if (dig(s[0]) && n != 20 && n != 25) {  // F3 candidates (any length of digits)
    u64 v = 0;
    for (const char* p = s; p < e; p++) {
      if (!dig(*p)) return false;
      u64 d = *p - '0';
      if (v > (UINT64_MAX - d) / 10) return false;
      v = v * 10 + d;
    }
    if (v > (u64)INT64_MAX) return false;
    out = (i64)v;
    return true;
  }
  if (n == 20 && s[19] == 'Z') return parse19(s, out);
  if (n == 25 && (s[19] == '+' || s[19] == '-') && dig(s[20]) && dig(s[21]) && s[22] == ':' && dig(s[23]) && dig(s[24])) {
    int oh = d2(s + 20), om = d2(s + 23);
    if (oh > 23 || om > 59) return false;
    i64 ms;
    if (!parse19(s, ms)) return false;
    i64 off = (i64)oh * 3600000 + (i64)om * 60000;
    out = s[19] == '-' ? ms + off : ms - off;
    return true;
  }
  // all-digit strings of length 20 or 25 (F3)
  u64 v = 0;
  for (const char* p = s; p < e; p++) {
    if (!dig(*p)) return false;
    u64 d = *p - '0';
    if (v > (UINT64_MAX - d) / 10) return false;
    v = v * 10 + d;
  }
  if (v > (u64)INT64_MAX) return false;
  out = (i64)v;
  return true;
}
static inline i64 floordiv(i64 a, i64 b) { i64 q = a / b; if ((a % b != 0) && ((a < 0) != (b < 0))) q--; return q; }

// ---------- hash tables ----------
static inline u64 pack_sv(SV s) { return (u64)(uintptr_t)s.p | ((u64)min<size_t>(s.n(), 65535) << 48); }
static inline const char* pk_ptr(u64 v) { return (const char*)(uintptr_t)(v & 0xFFFFFFFFFFFFULL); }
static inline size_t pk_len(u64 v) { return v >> 48; }

struct E16 { u64 key; u64 pk; };
struct Table {  // id hash -> person key (hash of the person's name), plus name
  vector<E16> t; vector<u64> names; u64 mask = 0;
  void init_cap(size_t c) { t.assign(c, E16{0, 0}); names.assign(c, 0); mask = c - 1; }
  void put(u64 key, u64 pk, u64 nm) { size_t i = (key >> 1) & mask; while (t[i].key) i = (i + 1) & mask; t[i] = {key, pk}; names[i] = nm; }
  inline void prefetch(u64 key) const { __builtin_prefetch(&t[(key >> 1) & mask]); }
  inline u64 resolve(u64 key) const {
    size_t i = (key >> 1) & mask;
    for (;;) { const E16& e = t[i]; if (e.key == key) return e.pk; if (!e.key) return key; i = (i + 1) & mask; }
  }
  inline bool name_of(u64 key, u64& nm) const {
    size_t i = (key >> 1) & mask;
    for (;;) { const E16& e = t[i]; if (e.key == key) { nm = names[i]; return true; } if (!e.key) return false; i = (i + 1) & mask; }
  }
};
struct StrMap {  // exact string -> u32
  struct E { u64 h; const char* p; u32 n; u32 val; };
  vector<E> t; u64 mask = 0;
  void init(size_t n) { size_t c = 16; while (c < 2 * n + 16) c <<= 1; t.assign(c, E{0, nullptr, 0, 0}); mask = c - 1; }
  void put(SV s, u32 val) {
    u64 h = hbytes(s.p, s.n());
    size_t i = (h >> 1) & mask;
    while (t[i].h) { if (t[i].h == h && t[i].n == s.n() && memcmp(t[i].p, s.p, s.n()) == 0) return; i = (i + 1) & mask; }
    t[i] = E{h, s.p, (u32)s.n(), val};
  }
  inline bool get(const char* p, size_t n, u32& val) const {
    u64 h = hbytes(p, n);
    size_t i = (h >> 1) & mask;
    for (;;) {
      const E& e = t[i];
      if (!e.h) return false;
      if (e.h == h && e.n == n && memcmp(e.p, p, n) == 0) { val = e.val; return true; }
      i = (i + 1) & mask;
    }
  }
};

// ---------- records ----------
struct Rec { u64 h; i64 ing; i64 ts; u64 ukey; u64 unm; u64 enm; u32 price; u16 cat; u8 type; u8 flags; };
struct Ev { u64 pk; i64 ts; u64 nm; u32 price; u16 cat; u8 type; u8 pad; };
struct Click { u64 pk; i64 ts; u64 cid; u32 camp; u32 pad; };
static const int P = 512, NB = 1024;
static inline size_t bucket_of(u64 pk) { return (pk >> 44) & (NB - 1); }

struct Row { const char* np; u32 nl; int32_t day; i64 sessions, events, rev; int32_t top; };

// ---------- parquet writer (flat schema, PLAIN, uncompressed, one row group) ----------
struct Buf {
  vector<u8> v;
  void b(u8 x) { v.push_back(x); }
  void varint(u64 x) { while (x >= 0x80) { b((x & 0x7f) | 0x80); x >>= 7; } b((u8)x); }
  void zz(i64 x) { varint(((u64)x << 1) ^ (u64)(x >> 63)); }
  void bytes(const void* p, size_t n) { auto c = (const u8*)p; v.insert(v.end(), c, c + n); }
  void le32(u32 x) { bytes(&x, 4); }
  void le64(u64 x) { bytes(&x, 8); }
};
struct TW {
  Buf& o; int last = 0;
  explicit TW(Buf& b) : o(b) {}
  void fh(int id, int type) { int d = id - last; if (d > 0 && d <= 15) o.b((u8)((d << 4) | type)); else { o.b((u8)type); o.zz(id); } last = id; }
  void i32(int id, i64 x) { fh(id, 5); o.zz(x); }
  void i64f(int id, i64 x) { fh(id, 6); o.zz(x); }
  void str(int id, const string& s) { fh(id, 8); o.varint(s.size()); o.bytes(s.data(), s.size()); }
  void list(int id, int et, size_t n) { fh(id, 9); if (n < 15) o.b((u8)((n << 4) | et)); else { o.b((u8)(0xF0 | et)); o.varint(n); } }
  void stop() { o.b(0); }
};
struct Col { const char* name; int ptype; int conv; bool opt; Buf body; };
static void write_parquet(const string& path, vector<Col>& cols, i64 nrows) {
  Buf f;
  f.bytes("PAR1", 4);
  vector<i64> offs(cols.size()), sizes(cols.size());
  if (nrows > 0) {
    for (size_t i = 0; i < cols.size(); i++) {
      Buf ph;
      TW w(ph);
      w.i32(1, 0);
      w.i32(2, (i64)cols[i].body.v.size());
      w.i32(3, (i64)cols[i].body.v.size());
      w.fh(5, 12);
      { TW d(ph); d.i32(1, nrows); d.i32(2, 0); d.i32(3, 3); d.i32(4, 3); d.stop(); }
      w.stop();
      offs[i] = (i64)f.v.size();
      f.bytes(ph.v.data(), ph.v.size());
      f.bytes(cols[i].body.v.data(), cols[i].body.v.size());
      sizes[i] = (i64)ph.v.size() + (i64)cols[i].body.v.size();
    }
  }
  Buf m;
  TW w(m);
  w.i32(1, 1);
  w.list(2, 12, cols.size() + 1);
  { TW r(m); r.str(4, "schema"); r.i32(5, (i64)cols.size()); r.stop(); }
  for (auto& c : cols) {
    TW r(m);
    r.i32(1, c.ptype);
    r.i32(3, c.opt ? 1 : 0);
    r.str(4, c.name);
    if (c.conv >= 0) r.i32(6, c.conv);
    r.stop();
  }
  w.i64f(3, nrows);
  if (nrows > 0) {
    w.list(4, 12, 1);
    TW rg(m);
    rg.list(1, 12, cols.size());
    i64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
      TW cc(m);
      cc.i64f(2, offs[i]);
      cc.fh(3, 12);
      {
        TW md(m);
        md.i32(1, cols[i].ptype);
        md.list(2, 5, 2); m.zz(0); m.zz(3);
        md.list(3, 8, 1); m.varint(strlen(cols[i].name)); m.bytes(cols[i].name, strlen(cols[i].name));
        md.i32(4, 0);
        md.i64f(5, nrows);
        md.i64f(6, sizes[i]);
        md.i64f(7, sizes[i]);
        md.i64f(9, offs[i]);
        md.stop();
      }
      cc.stop();
      total += sizes[i];
    }
    rg.i64f(2, total);
    rg.i64f(3, nrows);
    rg.stop();
  } else {
    w.list(4, 12, 0);
  }
  w.stop();
  f.bytes(m.v.data(), m.v.size());
  f.le32((u32)m.v.size());
  f.bytes("PAR1", 4);
  FILE* fp = fopen(path.c_str(), "wb");
  if (!fp) { fprintf(stderr, "cannot write %s\n", path.c_str()); exit(2); }
  fwrite(f.v.data(), 1, f.v.size(), fp);
  fclose(fp);
}
static void put_str(Buf& b, const char* p, size_t n) { b.le32((u32)n); b.bytes(p, n); }

// ---------- main pipeline ----------
struct TState {
  vector<Row> rows;
  vector<u64> cs, cp, asst;
  vector<i64> cr;
  vector<u32> cnt, apos, act;
  vector<i64> catsum;
  vector<u16> touched;
};

static string getenv_s(const char* k, const char* d) { const char* v = getenv(k); return v ? v : d; }

static bool extract_json_ts(const string& js, const char* key, i64& out) {
  size_t k = js.find(string("\"") + key + "\"");
  if (k == string::npos) return false;
  size_t c = js.find(':', k);
  size_t q1 = js.find('"', c);
  size_t q2 = js.find('"', q1 + 1);
  if (c == string::npos || q1 == string::npos || q2 == string::npos) return false;
  return parse_ts(js.data() + q1 + 1, js.data() + q2, out);
}

static void run() {
  string DATA = getenv_s("DATA_DIR", "/data"), OUT = getenv_s("OUTPUT_DIR", "/output");
  NCPU = atoi(getenv_s("NCPU", "0").c_str());
  if (NCPU <= 0) NCPU = (int)thread::hardware_concurrency();
  if (NCPU <= 0) NCPU = 4;
  int T = NCPU;

  i64 WS = 0, WE = 0;
  {
    File mf = load_file(DATA + "/manifest.json");
    string js(mf.b ? mf.b : "", mf.n);
    if (!extract_json_ts(js, "window_start", WS) || !extract_json_ts(js, "window_end", WE)) { fprintf(stderr, "bad manifest\n"); exit(2); }
  }
  const i64 day0 = floordiv(WS, DAY), nd = floordiv(WE - 1, DAY) - day0 + 1;

  // reference files
  vector<string> catnames;
  StrMap prodcat;
  {
    File pf = load_file(DATA + "/products.csv");
    vector<pair<SV, SV>> pr;
    for (auto& c : make_chunks({pf}))
      for_lines(c.b, c.e, [&](const char* s, const char* e) { SV f[2]; if (parse_fields(s, e, 2, f)) pr.push_back({f[0], f[1]}); });
    vector<string> names{"unknown"};
    for (auto& x : pr) names.push_back(string(x.second.p, x.second.n()));
    sort(names.begin(), names.end());
    names.erase(unique(names.begin(), names.end()), names.end());
    catnames = names;
    unordered_map<string, u16> id;
    for (size_t i = 0; i < names.size(); i++) id[names[i]] = (u16)i;
    prodcat.init(pr.size());
    for (auto& x : pr) prodcat.put(x.first, id[string(x.second.p, x.second.n())]);
  }
  const u16 UNKNOWN_CAT = (u16)(lower_bound(catnames.begin(), catnames.end(), string("unknown")) - catnames.begin());
  vector<SV> camp_id, camp_ch;
  StrMap campmap;
  {
    File cf = load_file(DATA + "/campaigns.csv");
    vector<pair<SV, SV>> cr;
    for (auto& c : make_chunks({cf}))
      for_lines(c.b, c.e, [&](const char* s, const char* e) { SV f[2]; if (parse_fields(s, e, 2, f)) cr.push_back({f[0], f[1]}); });
    campmap.init(cr.size());
    for (auto& x : cr) {
      u32 tmp;
      if (campmap.get(x.first.p, x.first.n(), tmp)) continue;
      campmap.put(x.first, (u32)camp_id.size());
      camp_id.push_back(x.first); camp_ch.push_back(x.second);
    }
  }
  const u32 nc = (u32)camp_id.size();
  logt("reference loaded");

  // links (parallel: CAS hash-insert, lock-free union-find, parallel min-name)
  Table tab;
  {
    vector<File> lf = load_dir(DATA + "/links");
    auto chunks = make_chunks(lf);
    struct LP { SV a, b; u64 ha, hb; u32 sa, sb; };
    vector<vector<LP>> lt(T);
    parallel_for(chunks.size(), [&](size_t ci, int t) {
      for_lines(chunks[ci].b, chunks[ci].e, [&](const char* s, const char* e) {
        SV f[2];
        if (!parse_fields(s, e, 2, f)) return;
        if (f[0].p == f[0].e || f[1].p == f[1].e) return;
        lt[t].push_back(LP{f[0], f[1], uid(f[0]), uid(f[1]), 0, 0});
      });
    });
    size_t L = 0;
    for (auto& v : lt) L += v.size();
    size_t cap = 16;
    while (cap < 4 * L + 16) cap <<= 1;
    tab.init_cap(cap);
    vector<u32> par(cap), best(cap, UINT32_MAX);
    const size_t BLK = 8192;
    vector<array<size_t, 3>> blocks;
    for (int t = 0; t < T; t++)
      for (size_t lo = 0; lo < lt[t].size(); lo += BLK) blocks.push_back({(size_t)t, lo, min(lt[t].size(), lo + BLK)});
    const size_t SB = 1 << 16;
    size_t nsb = (cap + SB - 1) / SB;
    parallel_for(nsb, [&](size_t bi, int) { for (size_t i = bi * SB; i < min(cap, (bi + 1) * SB); i++) par[i] = (u32)i; });
    auto ins = [&](u64 h, SV s) -> u32 {
      size_t i = (h >> 1) & tab.mask;
      for (;;) {
        u64 cur = __atomic_load_n(&tab.t[i].key, __ATOMIC_RELAXED);
        if (cur == h) return (u32)i;
        if (cur == 0) {
          u64 exp = 0;
          if (__atomic_compare_exchange_n(&tab.t[i].key, &exp, h, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) { tab.names[i] = pack_sv(s); return (u32)i; }
          if (exp == h) return (u32)i;
        }
        i = (i + 1) & tab.mask;
      }
    };
    auto fnd = [&](u32 x) -> u32 {
      for (;;) {
        u32 p = __atomic_load_n(&par[x], __ATOMIC_RELAXED);
        if (p == x) return x;
        u32 g = __atomic_load_n(&par[p], __ATOMIC_RELAXED);
        __atomic_compare_exchange_n(&par[x], &p, g, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        x = g;
      }
    };
    parallel_for(blocks.size(), [&](size_t bi, int) {
      auto& v = lt[blocks[bi][0]];
      for (size_t i = blocks[bi][1]; i < blocks[bi][2]; i++) { v[i].sa = ins(v[i].ha, v[i].a); v[i].sb = ins(v[i].hb, v[i].b); }
    });
    parallel_for(blocks.size(), [&](size_t bi, int) {
      auto& v = lt[blocks[bi][0]];
      for (size_t i = blocks[bi][1]; i < blocks[bi][2]; i++) {
        u32 a = v[i].sa, b = v[i].sb;
        for (;;) {
          u32 ra = fnd(a), rb = fnd(b);
          if (ra == rb) break;
          if (ra < rb) { u32 x = ra; ra = rb; rb = x; }
          u32 exp = ra;
          if (__atomic_compare_exchange_n(&par[ra], &exp, rb, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) break;
        }
      }
    });
    parallel_for(nsb, [&](size_t bi, int) {
      for (size_t i = bi * SB; i < min(cap, (bi + 1) * SB); i++) {
        if (!tab.t[i].key) continue;
        u32 r = fnd((u32)i);
        for (;;) {
          u32 cur = __atomic_load_n(&best[r], __ATOMIC_RELAXED);
          if (cur != UINT32_MAX && !svless(pk_ptr(tab.names[i]), pk_len(tab.names[i]), pk_ptr(tab.names[cur]), pk_len(tab.names[cur]))) break;
          if (__atomic_compare_exchange_n(&best[r], &cur, (u32)i, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) break;
        }
      }
    });
    parallel_for(nsb, [&](size_t bi, int) {
      for (size_t i = bi * SB; i < min(cap, (bi + 1) * SB); i++) {
        if (!tab.t[i].key) continue;
        u32 bb = best[fnd((u32)i)];
        tab.t[i].pk = tab.t[bb].key;
        tab.names[i] = tab.names[bb];
      }
    });
  }
  logt("links resolved");

  // events
  vector<vector<vector<Ev>>> evb(T, vector<vector<Ev>>(NB));
  {
    vector<File> ef = load_dir(DATA + "/events");
    auto chunks = make_chunks(ef);
    vector<vector<vector<Rec>>> parts(T, vector<vector<Rec>>(P));
    {
      size_t total = 0;
      for (auto& f : ef) total += f.n;
      size_t est = total / 80 / ((size_t)T * P);
      est = est + est / 4 + 16;
      if (est > 1 << 15) est = 1 << 15;
      parallel_for(T, [&](size_t t, int) { for (int p = 0; p < P; p++) parts[t][p].reserve(est); });
    }
    parallel_for(chunks.size(), [&](size_t ci, int t) {
      auto& pt = parts[t];
      for_lines(chunks[ci].b, chunks[ci].e, [&](const char* s, const char* e) {
        SV f[7];
        if (!parse_fields(s, e, 7, f)) return;
        if (f[0].p == f[0].e) return;
        Rec r;
        memset(&r, 0, sizeof r);
        r.h = uid(f[0]);
        r.enm = pack_sv(f[0]);
        i64 ing;
        if (!parse_ts(f[3].p, f[3].e, ing)) ing = INT64_MAX;
        r.ing = ing;
        do {
          if (f[1].p == f[1].e || f[5].p == f[5].e) break;
          i64 ts;
          if (!parse_ts(f[2].p, f[2].e, ts)) break;
          if (ts < WS || ts >= WE) break;
          if (ing == INT64_MAX || ing - ts > LATE) break;
          u8 type;
          if (sveq(f[4], "view")) type = 0; else if (sveq(f[4], "cart")) type = 1; else if (sveq(f[4], "purchase")) type = 2; else break;
          u32 price = 0; u16 cat = 0;
          if (type == 2) {
            if (f[6].p == f[6].e) break;
            u64 val = 0;
            bool bad = false;
            for (const char* q = f[6].p; q < f[6].e; q++) { if (!dig(*q)) { bad = true; break; } val = val * 10 + (*q - '0'); if (val > 10000000) { bad = true; break; } }
            if (bad || val < 1) break;
            price = (u32)val;
            u32 cv;
            cat = prodcat.get(f[5].p, f[5].n(), cv) ? (u16)cv : UNKNOWN_CAT;
          }
          r.ts = ts; r.ukey = uid(f[1]); r.unm = pack_sv(f[1]); r.price = price; r.cat = cat; r.type = type; r.flags = 1;
        } while (0);
        pt[(r.h >> 48) & (P - 1)].push_back(r);
      });
    });
    logt("events parsed");
    parallel_for(P, [&](size_t p, int t) {
      size_t tot = 0;
      for (int tt = 0; tt < T; tt++) tot += parts[tt][p].size();
      if (!tot) return;
      size_t cap = 16;
      while (cap < 2 * tot) cap <<= 1;
      struct Slot { u64 h; const Rec* r; };
      vector<Slot> tbl(cap, Slot{0, nullptr});
      u64 m = cap - 1;
      for (int tt = 0; tt < T; tt++)
        for (const Rec& r : parts[tt][p]) {
          size_t i = (r.h >> 1) & m;
          for (;;) {
            Slot& s = tbl[i];
            if (!s.h) { s = {r.h, &r}; break; }
            if (s.h == r.h && pk_len(s.r->enm) == pk_len(r.enm) && memcmp(pk_ptr(s.r->enm), pk_ptr(r.enm), pk_len(r.enm)) == 0) {
              if (r.ing < s.r->ing) s.r = &r;
              break;
            }
            i = (i + 1) & m;
          }
        }
      vector<const Rec*> kept;
      kept.reserve(tot);
      for (auto& s : tbl) if (s.r && (s.r->flags & 1)) kept.push_back(s.r);
      size_t nk = kept.size();
      for (size_t k = 0; k < min<size_t>(nk, 16); k++) tab.prefetch(kept[k]->ukey);
      for (size_t k = 0; k < nk; k++) {
        if (k + 16 < nk) tab.prefetch(kept[k + 16]->ukey);
        const Rec& r = *kept[k];
        u64 pk = tab.resolve(r.ukey);
        evb[t][bucket_of(pk)].push_back(Ev{pk, r.ts, r.unm, r.price, r.cat, r.type, 0});
      }
      for (int tt = 0; tt < T; tt++) vector<Rec>().swap(parts[tt][p]);
    });
  }
  logt("events validated");

  // clicks
  vector<vector<vector<Click>>> ckb(T, vector<vector<Click>>(NB));
  {
    vector<File> cf = load_dir(DATA + "/clicks");
    auto chunks = make_chunks(cf);
    parallel_for(chunks.size(), [&](size_t ci, int t) {
      struct PC { u64 key; i64 ts; u64 cid; u32 camp; };
      PC pend[16];
      int np = 0;
      auto flushp = [&]() {
        for (int k = 0; k < np; k++) tab.prefetch(pend[k].key);
        for (int k = 0; k < np; k++) {
          u64 pk = tab.resolve(pend[k].key);
          ckb[t][bucket_of(pk)].push_back(Click{pk, pend[k].ts, pend[k].cid, pend[k].camp, 0});
        }
        np = 0;
      };
      for_lines(chunks[ci].b, chunks[ci].e, [&](const char* s, const char* e) {
        SV f[4];
        if (!parse_fields(s, e, 4, f)) return;
        if (f[0].p == f[0].e || f[1].p == f[1].e) return;
        i64 ts;
        if (!parse_ts(f[3].p, f[3].e, ts)) return;
        if (ts < WS - WEEK || ts >= WE) return;
        u32 camp;
        if (!campmap.get(f[2].p, f[2].n(), camp)) return;
        pend[np++] = PC{uid(f[1]), ts, pack_sv(f[0]), camp};
        if (np == 16) flushp();
      });
      flushp();
    });
  }
  logt("clicks parsed");

  // per-bucket sessionization / attribution
  vector<TState> ts(T);
  for (auto& S : ts) {
    S.cs.assign((size_t)(nc + 1) * nd, 0); S.cp.assign((size_t)(nc + 1) * nd, 0); S.cr.assign((size_t)(nc + 1) * nd, 0);
    S.asst.assign((size_t)(nc + 1) * nd, 0);
    S.cnt.assign(nc + 1, 0); S.apos.assign(nc + 1, 0); S.catsum.assign(catnames.size(), 0);
  }
  parallel_for(NB, [&](size_t b, int t) {
    TState& S = ts[t];
    vector<Ev> E0; vector<Click> K0;
    size_t ne = 0, nk = 0;
    for (int tt = 0; tt < T; tt++) { ne += evb[tt][b].size(); nk += ckb[tt][b].size(); }
    if (ne == 0) { for (int tt = 0; tt < T; tt++) { vector<Ev>().swap(evb[tt][b]); vector<Click>().swap(ckb[tt][b]); } return; }
    E0.reserve(ne); K0.reserve(nk);
    for (int tt = 0; tt < T; tt++) {
      E0.insert(E0.end(), evb[tt][b].begin(), evb[tt][b].end()); vector<Ev>().swap(evb[tt][b]);
      K0.insert(K0.end(), ckb[tt][b].begin(), ckb[tt][b].end()); vector<Click>().swap(ckb[tt][b]);
    }
    // group by person (hash grouping instead of sorting)
    size_t cap = 16;
    while (cap < ne + ne / 2 + 16) cap <<= 1;
    struct GS { u64 pk; u32 gid; u32 pad; };
    vector<GS> gt(cap, GS{0, 0, 0});
    u64 gm = cap - 1;
    vector<u32> eg(ne), ecnt;
    for (size_t i = 0; i < ne; i++) {
      size_t sl = (E0[i].pk >> 1) & gm;
      for (;;) {
        GS& g = gt[sl];
        if (!g.pk) { g.pk = E0[i].pk; g.gid = (u32)ecnt.size(); ecnt.push_back(0); }
        if (g.pk == E0[i].pk) { eg[i] = g.gid; ecnt[g.gid]++; break; }
        sl = (sl + 1) & gm;
      }
    }
    size_t G = ecnt.size();
    vector<u32> est(G + 1, 0);
    for (size_t g = 0; g < G; g++) est[g + 1] = est[g] + ecnt[g];
    vector<Ev> E(ne);
    {
      vector<u32> pos(est.begin(), est.end() - 1);
      for (size_t i = 0; i < ne; i++) E[pos[eg[i]]++] = E0[i];
    }
    vector<Ev>().swap(E0);
    vector<u32>().swap(eg);
    vector<u32> kst(G + 1, 0), kg(nk, UINT32_MAX);
    for (size_t i = 0; i < nk; i++) {
      size_t sl = (K0[i].pk >> 1) & gm;
      for (;;) {
        const GS& g = gt[sl];
        if (!g.pk) break;
        if (g.pk == K0[i].pk) { kg[i] = g.gid; kst[g.gid + 1]++; break; }
        sl = (sl + 1) & gm;
      }
    }
    for (size_t g = 0; g < G; g++) kst[g + 1] += kst[g];
    vector<Click> K(kst[G]);
    {
      vector<u32> pos(kst.begin(), kst.end() - 1);
      for (size_t i = 0; i < nk; i++) if (kg[i] != UINT32_MAX) K[pos[kg[i]]++] = K0[i];
    }
    vector<Click>().swap(K0);
    vector<u32>().swap(kg);
    // sort each person's rows by ts
    for (size_t g = 0; g < G; g++) {
      Ev* a = E.data() + est[g]; size_t n = est[g + 1] - est[g];
      if (n <= 24) {
        for (size_t x = 1; x < n; x++) { Ev v = a[x]; size_t y = x; while (y > 0 && a[y - 1].ts > v.ts) { a[y] = a[y - 1]; y--; } a[y] = v; }
      } else sort(a, a + n, [](const Ev& p, const Ev& q) { return p.ts < q.ts; });
      Click* c = K.data() + kst[g]; size_t m = kst[g + 1] - kst[g];
      if (m <= 24) {
        for (size_t x = 1; x < m; x++) { Click v = c[x]; size_t y = x; while (y > 0 && c[y - 1].ts > v.ts) { c[y] = c[y - 1]; y--; } c[y] = v; }
      } else if (m > 1) sort(c, c + m, [](const Click& p, const Click& q) { return p.ts < q.ts; });
    }
    for (size_t g = 0; g < G; g++) {
      size_t i = est[g], j = est[g + 1], ks = kst[g], ke = kst[g + 1];
      u64 pk = E[i].pk;
      size_t addp = ks, remp = ks;
      bool have = false; i64 curday = 0, d_sess = 0, d_ev = 0, d_rev = 0;
      bool named = false; u64 pnm = 0;
      auto flush = [&]() {
        if (!have) return;
        int top = -1; i64 bestv = -1;
        for (u16 c : S.touched) { i64 v = S.catsum[c]; if (v > bestv || (v == bestv && (int)c < top)) { bestv = v; top = c; } }
        for (u16 c : S.touched) S.catsum[c] = 0;
        S.touched.clear();
        if (!named) { u64 x; pnm = tab.name_of(pk, x) ? x : E[i].nm; named = true; }
        S.rows.push_back(Row{pk_ptr(pnm), (u32)pk_len(pnm), (int32_t)curday, d_sess, d_ev, d_rev, top});
        have = false; d_sess = d_ev = d_rev = 0;
      };
      size_t a = i;
      while (a < j) {
        i64 start = E[a].ts, prev = start;
        i64 day = floordiv(start, DAY);
        if (have && day != curday) flush();
        have = true; curday = day;
        i64 evn = 0, rev = 0, npur = 0;
        size_t bb = a;
        while (bb < j && E[bb].ts - prev <= GAP) {
          prev = E[bb].ts; evn++;
          if (E[bb].type == 2) {
            rev += E[bb].price; npur++;
            u16 c = E[bb].cat;
            if (S.catsum[c] == 0) S.touched.push_back(c);
            S.catsum[c] += E[bb].price;
          }
          bb++;
        }
        while (addp < ke && K[addp].ts <= start) {
          u32 c = K[addp].camp;
          if (S.cnt[c]++ == 0) { S.apos[c] = (u32)S.act.size(); S.act.push_back(c); }
          addp++;
        }
        while (remp < addp && K[remp].ts < start - WEEK) {
          u32 c = K[remp].camp;
          if (--S.cnt[c] == 0) { u32 pos = S.apos[c]; u32 last = S.act.back(); S.act[pos] = last; S.apos[last] = pos; S.act.pop_back(); }
          remp++;
        }
        size_t di = (size_t)(day - day0);
        for (u32 c : S.act) S.asst[(size_t)c * nd + di]++;
        u32 camp = nc;
        if (addp > ks) {
          i64 lt = K[addp - 1].ts;
          if (start - lt <= WEEK) {
            size_t m = addp - 1, bi = m;
            while (m > ks && K[m - 1].ts == lt) {
              m--;
              if (svless(pk_ptr(K[m].cid), pk_len(K[m].cid), pk_ptr(K[bi].cid), pk_len(K[bi].cid))) bi = m;
            }
            camp = K[bi].camp;
          }
        }
        size_t idx = (size_t)camp * nd + di;
        S.cs[idx]++; if (npur > 0) S.cp[idx]++; S.cr[idx] += rev;
        d_sess++; d_ev += evn; d_rev += rev;
        a = bb;
      }
      flush();
      for (u32 c : S.act) S.cnt[c] = 0;
      S.act.clear();
    }
  });
  logt("sessions done");

  // outputs
  mkdir(OUT.c_str(), 0777);
  string ud = OUT + "/user_daily", cd = OUT + "/campaign_daily";
  mkdir(ud.c_str(), 0777); mkdir(cd.c_str(), 0777);
  parallel_for(T, [&](size_t t, int) {
    auto& rows = ts[t].rows;
    if (rows.empty() && t != 0) return;
    vector<Col> cols = {{"user_id", 6, 0, false, {}}, {"day", 1, 6, false, {}}, {"sessions", 2, -1, false, {}},
                        {"events", 2, -1, false, {}}, {"revenue_cents", 2, -1, false, {}}, {"top_category", 6, 0, true, {}}};
    if (!rows.empty()) {
      size_t n = rows.size();
      size_t ng = (n + 7) / 8;
      vector<u8> bits(ng, 0);
      Buf vals;
      for (size_t i = 0; i < n; i++) {
        const Row& r = rows[i];
        put_str(cols[0].body, r.np, r.nl);
        cols[1].body.le32((u32)r.day);
        cols[2].body.le64((u64)r.sessions);
        cols[3].body.le64((u64)r.events);
        cols[4].body.le64((u64)r.rev);
        if (r.top >= 0) { bits[i / 8] |= (u8)(1 << (i % 8)); const string& s = catnames[r.top]; put_str(vals, s.data(), s.size()); }
      }
      Buf hdr; hdr.varint(((u64)ng << 1) | 1);
      Buf& tb = cols[5].body;
      tb.le32((u32)(hdr.v.size() + ng));
      tb.bytes(hdr.v.data(), hdr.v.size());
      tb.bytes(bits.data(), ng);
      tb.bytes(vals.v.data(), vals.v.size());
    }
    write_parquet(ud + "/part-" + to_string(t) + ".parquet", cols, (i64)rows.size());
  });
  {
    size_t sz = (size_t)(nc + 1) * nd;
    vector<u64> cs(sz, 0), cp(sz, 0), asst(sz, 0);
    vector<i64> cr(sz, 0);
    for (auto& S : ts) for (size_t i = 0; i < sz; i++) { cs[i] += S.cs[i]; cp[i] += S.cp[i]; cr[i] += S.cr[i]; asst[i] += S.asst[i]; }
    vector<Col> cols = {{"campaign_id", 6, 0, false, {}}, {"channel", 6, 0, false, {}}, {"day", 1, 6, false, {}}, {"sessions", 2, -1, false, {}},
                        {"purchasing_sessions", 2, -1, false, {}}, {"revenue_cents", 2, -1, false, {}}, {"assisted_sessions", 2, -1, false, {}}};
    i64 n = 0;
    for (u32 c = 0; c <= nc; c++)
      for (i64 d = 0; d < nd; d++) {
        size_t i = (size_t)c * nd + d;
        if (cs[i] == 0 && asst[i] == 0) continue;
        n++;
        if (c == nc) { put_str(cols[0].body, "organic", 7); put_str(cols[1].body, "organic", 7); }
        else { put_str(cols[0].body, camp_id[c].p, camp_id[c].n()); put_str(cols[1].body, camp_ch[c].p, camp_ch[c].n()); }
        cols[2].body.le32((u32)(day0 + d));
        cols[3].body.le64(cs[i]); cols[4].body.le64(cp[i]); cols[5].body.le64((u64)cr[i]); cols[6].body.le64(asst[i]);
      }
    write_parquet(cd + "/part-0.parquet", cols, n);
  }
  logt("done");
}

int main() {
  T0 = now();
  try { run(); } catch (const exception& e) { fprintf(stderr, "error: %s\n", e.what()); return 1; }
  return 0;
}
