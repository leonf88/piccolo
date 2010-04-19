#!/bin/bash

set -x

mkdir -p /scratch/cp-union
fusermount -u /scratch/cp-union

FUSE_CMD=

for h in 2 3 5 6 7 8 9 10 11 12 13; do
  mkdir -p /scratch/cp-remote/beaker-$h
  umount -f /scratch/cp-remote/beaker-$h
  mount beaker-$h:/scratch/ /scratch/cp-remote/beaker-$h
  FUSE_CMD="/scratch/cp-remote/beaker-$h=RW:${FUSE_CMD}"
done

unionfs-fuse  -o allow_other -o uid=1043 -o gid=1043 $FUSE_CMD /scratch/cp-union
