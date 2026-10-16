const admin = db.getSiblingDB("admin");
const auth = admin.auth(process.env.MONGO_INITDB_ROOT_USERNAME, process.env.MONGO_INITDB_ROOT_PASSWORD);
if (!auth || !admin.runCommand({ ping: 1 }).ok) quit(1);
