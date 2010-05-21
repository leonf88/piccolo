#ifndef UTIL_STRING_H_
#define UTIL_STRING_H_

#include <vector>
#include <string>
#include <memory.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>

namespace dsm {

using std::string;

class StringPiece {
public:
  StringPiece();
  StringPiece(const string& s);
  StringPiece(const string& s, int len);
  StringPiece(const char* c);
  StringPiece(const char* c, int len);
  uint32_t hash() const;
  string AsString() const;

  int size() const { return len; }

  const char* data;
  int len;

  static std::vector<StringPiece> split(StringPiece sp, StringPiece delim);
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
  return a.data == b.data && a.len == b.len;
}

static const char* strnstr(const char* haystack, const char* needle, int len) {
  int nlen = strlen(needle);
  for (int i = 0; i < len - nlen; ++i) {
    if (strncmp(haystack + i, needle, nlen) == 0) {
      return haystack + i;
    }
  }
  return NULL;
}

#ifndef SWIG
string StringPrintf(StringPiece fmt, ...);
string VStringPrintf(StringPiece fmt, va_list args);
#endif

string ToString(int32_t);
string ToString(int64_t);
string ToString(string);
string ToString(StringPiece);

}

#endif /* STRING_H_ */
