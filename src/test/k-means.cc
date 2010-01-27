/*
 * K-means clustering of points selected from a set of Gaussian
 * distributions.
 */

#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

DEFINE_int32(num_dists, 2, "");
DEFINE_int32(num_points, 100, "");
DEFINE_int32(iterations, 50, "");
DEFINE_bool(dump_results, false, "");

using namespace dsm;

struct Point {
  double x, y;
  int source;
  double min_dist;
};

struct Distribution {
  double x, y;
};

static TypedGlobalTable<int, Point> *points;
static TypedGlobalTable<int, Distribution> *dists;
static TypedGlobalTable<int, Distribution> *actual;

double rand_double() {
  return double(random()) / RAND_MAX;
}

class KMeansKernel : public DSMKernel {
public:
  void initialize_world() {
    srand(time(NULL));
    int c = 0;
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      double dx = 0.5 - rand_double();
      double dy = 0.5 - rand_double();

      Distribution d = { dx, dy };
      actual->put(i, d);

      for (int j = 0; j < FLAGS_num_points / FLAGS_num_dists; ++j) {
        Point p = { dx + 0.1 * (rand_double() - 0.5), dy + 0.1 * (rand_double() - 0.5), -1, 0 };
        points->put(c++, p);
      }
    }

    for (int i = 0; i < FLAGS_num_dists; ++i) {
      // Initialize a guess for center point of the distributions
      Point p = points->get(random() % FLAGS_num_points);
      Distribution d = { p.x, p.y };
      dists->put(i, d);
      LOG(INFO) << "Initial guess from " << d.x << " : " << d.y;
    }
  }

  void initialize_expectation() {
    TypedTable<int, Point>::Iterator *it = points->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      it->value().min_dist = 2;
    }
  }

  // Iterate over all distributions, and for each local point, compute the
  // distribution with maximum likelihood.
  void compute_expectation() {
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      Distribution d = dists->get(i);
      TypedTable<int, Point>::Iterator *it = points->get_typed_iterator(current_shard());
      for (; !it->done(); it->Next()) {
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
    TypedTable<int, Distribution>::Iterator *it = dists->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      Distribution &d = it->value();
      LOG(INFO) << "Distribution" << ":: " << it->key() << " :: "<< d.x << " : " << d.y;

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
    TypedTable<int, Point>::Iterator *it = points->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      const Point &p = it->value();
      d.x = p.x * FLAGS_num_dists / FLAGS_num_points;
      d.y = p.y * FLAGS_num_dists / FLAGS_num_points;
      dists->put(p.source, d);
    }
  }

  void print_results() {
    for (int i = 0; i < FLAGS_num_dists; ++i) {
      Distribution d = dists->get(i);
      double best_diff = 1000;
      Distribution best = d;
      for (int j = 0; j < FLAGS_num_dists; ++j) {
        Distribution a = actual->get(j);
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

static Distribution dist_merge(const Distribution& d1, const Distribution& d2) {
  Distribution o;
  o.x = d1.x + d2.x;
  o.y = d1.y + d2.y;
  return o;
}

int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  dists = Registry::create_table<int, Distribution>(0, conf.num_workers(), &ModSharding, &dist_merge);
  points = Registry::create_table<int, Point>(1, conf.num_workers(), &ModSharding, &Accumulator<Point>::replace);
  actual = Registry::create_table<int, Distribution>(2, conf.num_workers(), &ModSharding, &dist_merge);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ONE(m, KMeansKernel, initialize_world, 0);
    for (int i = 0; i < FLAGS_iterations; i++) {
      RUN_ALL(m, KMeansKernel, initialize_expectation, 1);
      RUN_ALL(m, KMeansKernel, compute_expectation, 1);
      RUN_ALL(m, KMeansKernel, initialize_maximization, 0);
      RUN_ALL(m, KMeansKernel, compute_maximization, 0);
    }
    RUN_ONE(m, KMeansKernel, print_results, 0);
  } else {
    Worker w(conf);
    w.Run();
  }
}

