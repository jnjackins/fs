#!/bin/bash

# Setup new venti and fossil filesystems, and run fossil in console mode.
# Run with -D to run fossil in debug mode.

set -e

cd $(dirname $0)

echo "building"
go install -race sigint.ca/fs/fossil

export venti=127.0.0.1
export NAMESPACE=$(pwd)

./clean.sh
if mount |grep -q fuse
	then echo "waiting for stale mounts to be cleaned up…"
	while mount |grep -q fuse; do sleep 5; done
fi

trap "./clean.sh" SIGINT SIGTERM

echo "formatting venti partitions"
dd if=/dev/zero of=arenas.part bs=8192 count=20000 2>/dev/null
dd if=/dev/zero of=isect.part bs=8192 count=1000 2>/dev/null
$PLAN9/bin/venti/fmtarenas arenas arenas.part 2>/dev/null
$PLAN9/bin/venti/fmtisect isect isect.part 2>/dev/null
$PLAN9/bin/venti/fmtindex venti.conf 2>/dev/null

echo "starting venti"
$PLAN9/bin/venti/venti -w 2>/dev/null

echo "formatting fossil partition"
dd if=/dev/zero of=fossil.part bs=8192 count=20000 2>/dev/null
fossil format -b 8k -y fossil.part
mkdir active snap archive

(
	sleep 2; 
	pgrep fossil >/dev/null || exit
	9pfuse -a main/active fossil.srv active;
	9pfuse -a main/snapshot fossil.srv snap;
	9pfuse -a main/archive fossil.srv archive;

	echo "the quick brown fox jumps over the lazy dog" >active/test
) &

echo "starting fossil"
fossil="fossil $FOSSILFLAGS start"
if test "$1" == "9"; then
	fossil="$PLAN9/bin/fossil/fossil $FOSSILFLAGS -t"
	shift
fi

$fossil -c '. flproto'
