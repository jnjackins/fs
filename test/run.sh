#!/bin/bash
# Setup new venti and fossil filesystems, and run fossil in console mode.
# Run with -D to run fossil in debug mode.

export venti=127.0.0.1
export NAMESPACE=$(pwd)

if mount |grep -q fuse
	then echo "waiting for stale mounts to be cleaned up…"
	while mount |grep -q fuse; do sleep 5; done
fi

trap "./clean.sh" SIGINT SIGTERM

dd if=/dev/zero of=arenas.part bs=8192 count=2000
dd if=/dev/zero of=isect.part bs=8192 count=100
$PLAN9/bin/venti/fmtarenas arenas arenas.part
$PLAN9/bin/venti/fmtisect isect isect.part
$PLAN9/bin/venti/fmtindex venti.conf
$PLAN9/bin/venti/venti -w

dd if=/dev/zero of=fossil.part bs=8192 count=1000
fossil fmt -y fossil.part
mkdir active snap archive

(
	sleep 1;
	9pfuse -a main/active fossilsrv active;
	9pfuse -a main/snapshot fossilsrv snap;
	9pfuse -a main/archive fossilsrv archive;
) &

fossil $1 cons -c '. flproto'
