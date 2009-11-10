#ifndef ACCUMULATOR_H_
#define ACCUMULATOR_H_

#include "util/common.h"
#include <algorithm>

#define POD_CAST(type, d) ( *((type*)d.data) )


namespace asyncgraph {
class Accumulator {
public:
  Accumulator() {
    VLOG(4) << "Created accumulator " << this;
  }

  virtual ~Accumulator() {
    VLOG(4) << "Destroyed accumulator " << this;
  }

  class Iterator {
  public:
    Iterator(Accumulator* a) : owner(a) {}
    virtual StringPiece key() = 0;
    virtual StringPiece value() = 0;
    virtual void next() = 0;
    virtual bool done() = 0;

    Accumulator* owner;
  };

  virtual Iterator* get_iterator() = 0;
  virtual void clear() = 0;
  virtual void add(const StringPiece &k, const StringPiece &v) = 0;

  // An estimate of the amount of space this accumulator is using.
  virtual int64_t size() = 0;

  // The iteration this accumulator contains (partial) data for.
  int iteration;
  int peer;
};

template <class T>
class STLIterator : public Accumulator::Iterator {
public:
  typedef typename T::key_type K;
  typedef typename T::value_type V;

  STLIterator(Accumulator* a, const T &m) : Iterator(a), data(m), i(data.begin()) {}
    
  StringPiece key() { 
    return StringPiece((char*)&(i->first), sizeof(K)); 
  }
  StringPiece value() { 
    return StringPiece((char*)&(i->second), sizeof(V)); 
  }

  void next() { ++i; }
  bool done() { return i == data.end(); }
private:
  const T &data;
  typename T::const_iterator i;
};

template <class K, class V>
class SumAccumulator : public Accumulator {
public:
  typedef unordered_map<K, V> MType;
  typedef STLIterator<MType> Iterator;

  void add(const StringPiece &k, const StringPiece &v) {
    add(POD_CAST(K, k), POD_CAST(V, v));
  }

  void add(const K& k, const V& v) { data[k] += v; }
  void clear() { data.clear(); }
  Iterator* get_iterator() { return new Iterator(this, data); }
  int64_t size() { return (int64_t)(data.size() * sizeof(K) * sizeof(V) * 1.5); }

private:
  MType data;
};

template <class K, class V>
class MinAccumulator : public Accumulator {
public:
  typedef unordered_map<K, V> MType;
  typedef STLIterator<MType> Iterator;

  void add(const StringPiece &k, const StringPiece &v) {
    add(POD_CAST(K, k), POD_CAST(V, v));
  }

  void add(const K& k, const V& v) {
    typename MType::iterator i = data.find(k);
    if (i != data.end()) {
      i->second = std::min(i->second, v);
    } else {
      data.insert(std::make_pair(k, v));
    }
  }

  int64_t size() { return (int64_t)(data.size() * sizeof(K) * sizeof(V) * 1.5); }

  void clear() { data.clear(); }
  Iterator* get_iterator() { return new Iterator(this, data); }

private:
  MType data;
};

// Doesn't accumulate values together, just appends them to the
// outgoing set.
template <class K, class V>
class NullAccumulator {
public:
  typedef unordered_multimap<K, V> MType;
  typedef STLIterator<MType> Iterator;

  void add(const K& k, const V& v) { data.insert(std::make_pair(k, v)); }
  void clear() { data.clear(); }
  Iterator* get_iterator() { return new Iterator(data); }

  int64_t size() { return (int64_t)(data.size() * sizeof(K) * sizeof(V) * 1.5); }
private:
  MType data;
};
}

#endif /* ACCUMULATOR_H_ */
