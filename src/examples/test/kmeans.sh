#!/bin/bash
set -x
source $(dirname $0)/run_util.sh
run_command 'KMeans' '--iterations=10 --num_dists=100 --num_points=50000000 '
