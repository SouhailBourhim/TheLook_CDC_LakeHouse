#!/bin/bash
# Writes the ACL file from the passwords in onprem/.env, then starts
# redis-server as the redis user (ADR 018).
#
# Not through the image's entrypoint: it loads every module shipped in the
# image (Query Engine, JSON, time series, probabilistic). The features use
# only core data structures, so the modules would add commands and memory
# for nothing.
#
# Users, least privilege as for PostgreSQL and MongoDB:
#   default   disabled: a client that does not log in can do nothing
#   admin     everything, for operations (runbook: rebuild, INFO)
#   features  the features stream: the write commands it sends, on user:* keys
#   api       the API: read commands only, keys user:* read-only (%R~)
# Rules apply left to right: -@all clears, +@connection allows AUTH, HELLO,
# PING, CLIENT SETINFO..., -@dangerous then removes CLIENT KILL and the like.
set -euo pipefail

hash() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }

acl=/run/redis/users.acl
install -d -o redis -g redis -m 700 /run/redis
# Only hashes are written (#<sha256>): no password in clear on disk.
(
  umask 077
  cat > "$acl" <<ACL
user default off resetpass resetkeys resetchannels -@all
user admin on #$(hash "$REDIS_ADMIN_PASSWORD") ~* &* +@all
user features on #$(hash "$REDIS_FEATURES_PASSWORD") resetchannels ~user:* -@all +@connection -@dangerous +zadd +zremrangebyrank +zremrangebyscore +hset +pexpireat
user api on #$(hash "$REDIS_API_PASSWORD") resetchannels %R~user:* -@all +@connection +@read -@dangerous
ACL
)
chown redis:redis "$acl"

# The server process does not need the passwords in its environment.
unset REDIS_ADMIN_PASSWORD REDIS_FEATURES_PASSWORD REDIS_API_PASSWORD
exec setpriv --reuid redis --regid redis --clear-groups \
  redis-server /etc/redis/redis.conf --aclfile "$acl"
