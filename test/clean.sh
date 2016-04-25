#!/bin/bash
rm -f fossilsrv

# umount likes to fail the first time if fossil has already died.
(for _ in 1 2; do umount -f active; done; rmdir active) 2>/dev/null &
(for _ in 1 2; do umount -f snap; done; rmdir snap) 2>/dev/null &
(for _ in 1 2; do umount -f archive; done; rmdir archive) 2>/dev/null &

pkill venti 2>/dev/null
pkill fossil 2>/dev/null
rm -f {arenas,isect,fossil}.part