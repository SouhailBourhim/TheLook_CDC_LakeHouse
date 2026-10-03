// Idempotent MongoDB setup, run by the mongo-init service on every
// `docker compose up` (the MongoDB counterpart of postgres/cdc-setup.sql).
// Running it again changes nothing, except passwords and roles, which are
// brought back to what this file and onprem/.env say.
//
//   1. initiate the single-node replica set rs0 (Debezium reads change
//      streams, which only exist on replica sets);
//   2. create or update the least-privilege users;
//   3. create the collections and their indexes.

function env(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set (onprem/.env)`);
  return value;
}

// directConnection: talk to this one server even though it is (or will be)
// a replica set member, instead of discovering the set from its config.
const conn = new Mongo("mongodb://mongo:27017/?directConnection=true");
const admin = conn.getDB("admin");
admin.auth(env("MONGO_ROOT_USER"), env("MONGO_ROOT_PASSWORD"));

// --- 1. Replica set -------------------------------------------------------
// The member's host name is what every replica-set-aware client is told to
// connect to, like Kafka's advertised.listeners: "mongo:27017" works inside
// the Compose network; tools on the host must use localhost:27017 with
// directConnection=true, or they would try to reach "mongo".
// mongosh throws server errors (ok: 0) as exceptions instead of returning
// them, so "not initiated yet" arrives as a caught error with its code name.
try {
  const status = admin.runCommand({ replSetGetStatus: 1 });
  print(`replica set ${status.set} already initiated`);
} catch (e) {
  if (e.codeName !== "NotYetInitialized") throw e;
  admin.runCommand({
    replSetInitiate: { _id: "rs0", members: [{ _id: 0, host: "mongo:27017" }] },
  });
  print("replica set rs0 initiated");
}

// A new replica set holds an election before it accepts writes.
for (let i = 0; !admin.runCommand({ hello: 1 }).isWritablePrimary; i++) {
  if (i === 120) throw new Error("no primary after 60 s");
  sleep(500);
}

// --- 2. Users --------------------------------------------------------------
// Roles are scoped to single collections and actions, so a leaked password
// cannot drop a collection or read the other one.
const web = conn.getDB("web");

function upsertRole(db, role, privileges) {
  if (db.getRole(role)) db.updateRole(role, { privileges, roles: [] });
  else db.createRole({ role, privileges, roles: [] });
}

function upsertUser(db, user, pwd, roles) {
  if (db.getUser(user)) db.updateUser(user, { pwd, roles });
  else db.createUser({ user, pwd, roles });
  print(`user ${user}: ${JSON.stringify(roles)}`);
}

// The generator upserts events (bulk updates with upsert=true need find,
// insert and update).
upsertRole(web, "eventsWriter", [
  { resource: { db: "web", collection: "events" }, actions: ["find", "insert", "update"] },
]);
upsertUser(web, "generator", env("MONGO_GENERATOR_PASSWORD"), [{ role: "eventsWriter", db: "web" }]);

// Debezium (connector capture.scope=database, capture.target=web): the
// built-in read role on web covers the initial snapshot (find) and the
// database-level change stream (changeStream). Nothing cluster-wide, no
// write. Any user may run the hello command, which Debezium also needs.
upsertUser(web, "debezium", env("DEBEZIUM_MONGO_PASSWORD"), [{ role: "read", db: "web" }]);

// --- 3. Collections and indexes ---------------------------------------------
// Collections are created explicitly so they exist (and are captured) before
// the first write. events has only the default unique index on _id.
if (!web.getCollectionNames().includes("events")) web.createCollection("events");

print("mongo setup done");
