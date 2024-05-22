#!/usr/bin/env bash

usage() {
  echo
  echo Usage:
  echo $(basename "$0") bn=\<backup name\> mn=\<module name\> cp=\<compare phrase\>
  echo
  exit 1
}

for arg in "$@"; do
  key=$(echo "$arg" | cut -f1 -d=)
  val=$(echo "$arg" | cut -f2 -d=)

  case "$key" in
      bn) bn=${val} ;;
      mn) mn=${val} ;;
      cp) cp=${val} ;;
      help) help=ture ;;
      *) ;;
  esac
done

if [ -n "$help" ]; then
  usage
fi

if [ -z "$bn" ]; then
  usage
fi

if [ -z "$mn" ]; then
  mn=carrot-core
fi

if [ -z "$cp" ]; then
  cp="Orphaned allocations"
fi

reports_home=$mn/target/surefire-reports
backup_home=carrot-core-surefire-reports/$mn/$bn

rm -f "$reports_home"/Orphaned_allocations.log
rm -f "$backup_home"/Orphaned_allocations.log

# shellcheck disable=SC2044
for ix in $(find $reports_home -name "*output.txt"); do
  grep "$cp" "$ix" | cut -c31-10000 >>$reports_home/Orphaned_allocations.log
done

# shellcheck disable=SC2044
for ix in $(find "$backup_home" -name "*output.txt"); do
  grep "$cp" "$ix" | cut -c31-10000 >> "$backup_home"/Orphaned_allocations.log
done

# shellcheck disable=SC2046
echo Compare at $(date) $reports_home/Orphaned_allocations.log "$backup_home"/Orphaned_allocations.log
echo
diff $reports_home/Orphaned_allocations.log "$backup_home"/Orphaned_allocations.log >$reports_home/Orphaned_allocations.diff
more $reports_home/Orphaned_allocations.diff
