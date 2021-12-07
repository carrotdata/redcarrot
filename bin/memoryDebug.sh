#!/usr/bin/env bash

dir1=$1
junitRegExp=$2
dir2=carrot-core/target/surefire-reports

rm -f $dir1/Orphaned_allocations.log
rm -f $dir2/Orphaned_allocations.log

# shellcheck disable=SC2044
for ix in $(find $dir1 -name "*output.txt"); do
  grep "Orphaned allocations" $ix >>$dir1/Orphaned_allocations.log
done

# shellcheck disable=SC2044
for ix in $(find $dir2 -name "*output.txt"); do
  grep "Orphaned allocations" $ix >>$dir2/Orphaned_allocations.log
done

diff $dir1/Orphaned_allocations.log $dir2/Orphaned_allocations.log >$dir1/Orphaned_allocations.diff
if [ -z $junitRegExp ]; then
  more $dir1/Orphaned_allocations.diff
else
  more $dir1/Orphaned_allocations.diff | grep -i $junitRegExp
fi
