#!/bin/bash
# Wrapper around the image's entrypoint: create the replica set keyfile on
# the first start, then hand over unchanged.
#
# Why a keyfile on a single node: a replica set with access control must
# authenticate its members to each other, and mongod refuses to start with
# --replSet and authorization unless it has a keyfile (or x.509).
# The file is a shared secret, so it is generated here, inside the volume,
# never committed. mongod rejects a keyfile readable by group or others, so
# it must be mode 400; the entrypoint then chowns /data/configdb to the
# mongodb user (see docker-entrypoint.sh).
set -euo pipefail

keyfile=/data/configdb/keyfile
if [ ! -s "$keyfile" ]; then
  # 756 random bytes = 1008 base64 characters, the maximum keyfile length.
  openssl rand -base64 756 > "$keyfile"
fi
chmod 400 "$keyfile"

exec docker-entrypoint.sh "$@"
