#!/usr/bin/env bash

usage() {
  echo
  echo Usage:
  # shellcheck disable=SC2046
  echo $(basename "$0") bn=\<backup name\> mn=\<module name\>
  echo
  exit 1
}

for arg in "$@"; do
  key=$(echo "$arg" | cut -f1 -d=)
  val=$(echo "$arg" | cut -f2 -d=)
  echo key=$key val=$val

  case "$key" in
      bn) bn=${val} ;;
      mn) mn=${val} ;;
      *) ;;
  esac
done

if [ -z "$bn" ]; then
  bn=$(date +"%m-%d-%y_%H-%M")
fi
if [ -z "$mn" ]; then
  mn=carrot-core
fi

reports_home=$mn/target/surefire-reports
backup_home=carrot-core-surefire-reports/$mn

mkdir "$backup_home" 2>/dev/null
mkdir "$backup_home/$bn" 2>/dev/null

cp -p $reports_home/*output.txt "$backup_home/$bn"
# shellcheck disable=SC2046
# shellcheck disable=SC2012
echo backed up $(ls -l "$backup_home/$bn"/* | wc -l) reports to "$backup_home/$bn" directory...
