#!/bin/sh

set -e

cd $(dirname $0)

./clean.sh

echo "formatting venti partitions"
dd if=/dev/zero of=arenas.part bs=8192 count=200000 2>/dev/null
dd if=/dev/zero of=isect.part bs=8192 count=10000 2>/dev/null
$PLAN9/bin/venti/fmtarenas arenas arenas.part 2>/dev/null
$PLAN9/bin/venti/fmtisect isect isect.part 2>/dev/null
$PLAN9/bin/venti/fmtindex venti.conf 2>/dev/null

echo "starting venti"
$PLAN9/bin/venti/venti
