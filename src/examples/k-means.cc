/*
 * K-means clustering of points selected from a set of Gaussian
 * distributions.
 */

#include "client.h"

DEFINE_int64(num_dists, 2, "");
DEFINE_int64(num_points, 100, "");
DEFINE_bool(dump_results, false, "");

using namespace dsm;

struct Point {
  float x, y;
  float min_dist;
  int source;
};

struct Distribution {
  float x, y;
};

static TypedGlobalTable<int32_t, Point> *points;
static TypedGlobalTable<int32_t, Distribution> *dists;
static TypedGlobalTable<int32_t, Distribution> *actual;

class KMeansKernel : public DSMKernel {
public:
  void initialize_world() {
    points->resize(FLAGS_num_points);

    srand(0);
    vector<Distribution> rdists;
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      double dx = 0.5 - rand_double();
      double dy = 0.5 - rand_double();

      Distribution d = { dx, dy };
      rdists.push_back(d);
    }

    const int num_shards = points->num_shards();
    for (int64_t i = current_shard(); i < FLAGS_num_points; i += num_shards) {
      Distribution d = rdists[i % FLAGS_num_dists];
      double dx = d.x;
      double dy = d.y;

      Point p = { dx + 0.1 * (rand_double() - 0.5), dy + 0.1 * (rand_double() - 0.5), -1, 0 };
      points->update(i, p);
    }

    if (current_shard() == 0) {
      for (int i = 0; i < FLAGS_num_dists; ++i) {
        // Initialize a guess for center point of the distributions
        Point p = points->get(random() % FLAGS_num_points);
        Distribution d = { p.x, p.y };
        dists->update(i, d);
        actual->update(i, rdists[i]);
      }
    }
  }

  void initialize_expectation() {
    TypedIterator<int, Point> *it = points->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      it->value().min_dist = 2;
    }
  }

  // Iterate over all distributions, and for each local point, compute the
  // distribution with maximum likelihood.
  void compute_expectation() {
    vector<Distribution> local;
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      local.push_back(dists->get(i));
    }

    TypedIterator<int, Point> *it = points->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      for (int i = 0; i < FLAGS_num_dists; ++i) {
        Distribution& d = local[i];
        Point &p = it->value();
        double dist = pow(p.x - d.x, 2) + pow(p.y - d.y, 2);
        if (dist < p.min_dist) {
          p.min_dist = dist;
          p.source = i;
        }
      }
    }
  }

  void initialize_maximization() {
    TypedIterator<int, Distribution> *it = dists->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      Distribution &d = it->value();
//      LOG(INFO) << "Distribution" << ":: " << it->key() << " :: "<< d.x << " : " << d.y;

      if (d.x == 0 && d.y == 0) {
        Point p = points->get(random() % FLAGS_num_points);
        d.x = p.x;
        d.y = p.y;
      } else {
        d.x = 0;
        d.y = 0;
      }

    }
  }

  // Iterate over all points, and average in their contributions to the
  // appropriate distribution.
  void compute_maximization() {
    Distribution d;
    TypedIterator<int, Point> *it = points->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      const Point &p = it->value();
      d.x = p.x * FLAGS_num_dists / FLAGS_num_points;
      d.y = p.y * FLAGS_num_dists / FLAGS_num_points;
      dists->update(p.source, d);
    }
  }

  void print_results() {
    vector<Distribution> local;
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      local.push_back(actual->get(i));
    }

    for (int i = 0; i < FLAGS_num_dists; ++i) {
      Distribution d = dists->get(i);
      double best_diff = 1000;
      Distribution best = d;
      for (int j = 0; j < FLAGS_num_dists; ++j) {
        Distribution a = local[j];
        double diff = fabs(d.x - a.x) + fabs(d.y - a.y);
        if (diff < best_diff) {
          best_diff = diff;
          best = a;
        }
      }

      printf("%d guess: (%.2f %.2f) actual: (%.2f %.2f) error(%.2f, %.2f)\n",
             i, d.x, d.y, best.x, best.y, fabs(d.x - best.x), fabs(d.y - best.y));
    }

    if (FLAGS_dump_results) {
      for (int i = 0; i < FLAGS_num_points; ++i) {
        Point p = points->get(i);
        printf("%.2f %.2f %d\n", p.x, p.y, p.source);
      }
    }
  }
};

REGISTER_KERNEL(KMeansKernel);
REGISTER_METHOD(KMeansKernel, initialize_world);
REGISTER_METHOD(KMeansKernel, initialize_expectation);
REGISTER_METHOD(KMeansKernel, initialize_maximization);
REGISTER_METHOD(KMeansKernel, compute_expectation);
REGISTER_METHOD(KMeansKernel, compute_maximization);
REGISTER_METHOD(KMeansKernel, print_results);

struct DistAccum {
  void operator()(Distribution* d1, const Distribution& d2) {
    Distribution o;
    o.x = d1->x + d2.x;
    o.y = d1->y + d2.y;
    *d1 = o;
  }
};

static int KMeans(ConfigData& conf) {
  const int num_shards = conf.num_workers() * 4;
  dists = TableRegistry::Get()->create_table<int, Distribution>(0, num_shards, new Sharding::Mod, new DistAccum);
  points = TableRegistry::Get()->create_table<int, Point>(1, num_shards, new Sharding::Mod, new Accumulators<Point>::Replace);
  actual = TableRegistry::Get()->create_table<int, Distribution>(2, num_shards, new Sharding::Mod, new DistAccum);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("KMeansKernel", " initialize_world",  points);
    for (int i = 0; i < FLAGS_iterations; i++) {
      m.run_all("KMeansKernel", " initialize_expectation",  points);
      m.run_all("KMeansKernel", " compute_expectation",  points);
      m.run_all("KMeansKernel", " initialize_maximization",  dists);
      m.run_all("KMeansKernel", " compute_maximization",  dists);
    }
  //    m.run_one("KMeansKernel", " print_results",  0);
  }
  return 0;
}
REGISTER_RUNNER(KMeans);
