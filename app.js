const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");
const { mqbroker } = require("./services/rabbitmq.service");


const MONGO_URI = process.env.MONGODB_URL;
const BACKUP_DIR = "/app/backups";
const COMPRESS_BACKUP = true;


const client = new MongoClient(MONGO_URI);


function runCommand(cmd) {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) return reject(error);
      if (stderr) console.error(stderr);
      resolve(stdout);
    });
  });
}


async function startDbBackup(payload, msg, channel) {

    try {

        // 2️⃣ List all databases
        const adminDb = client.db().admin();
        const { databases } = await adminDb.listDatabases();
        console.log(`📚 Found ${databases.length} databases:`);
        databases.forEach((db) => console.log(`   • ${db.name}`));

        // 3️⃣ Ensure backup directory exists
        if (!fs.existsSync(BACKUP_DIR)) {
            fs.mkdirSync(BACKUP_DIR);
        }

        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
        const rootBackupPath = path.join(BACKUP_DIR, `backup-${timestamp}`);
        fs.mkdirSync(rootBackupPath);

        // 4️⃣ Backup each database
        for (const dbInfo of databases) {
        const dbName = dbInfo.name;

        // Skip internal DBs if desired
        if (["admin", "local", "config"].includes(dbName)) {
            console.log(`⚙️ Skipping system database: ${dbName}`);
            continue;
        }

        console.log(`🟢 Backing up database: ${dbName}`);
        const dbBackupPath = path.join(rootBackupPath, dbName);

        // Run mongodump
        const dumpCommand = `mongodump --uri="${MONGO_URI}/${dbName}" --out="${dbBackupPath}"`;
        await runCommand(dumpCommand);

        console.log(`✅ Backup completed for ${dbName}`);

        // Optional: compress the backup
        if (COMPRESS_BACKUP) {
            const zipPath = `${dbBackupPath}.zip`;
            await runCommand(`zip -r "${zipPath}" "${dbBackupPath}"`);
            fs.rmSync(dbBackupPath, { recursive: true, force: true });
            console.log(`📦 Compressed backup created: ${zipPath}`);
        }
        }

        console.log(`🎉 All database backups completed successfully at ${rootBackupPath}`);
    } catch (err) {
        console.error("❌ Error during backup:", err.message);
    } finally {
        channel.ack(msg);
    }
}

process.on("exit", async () => {
    await client.close();
})


async function main() {

    await client.connect();
    console.log("🔗 Connecting to MongoDB...");
    console.log("✅ Connected successfully.");

    await mqbroker.consume("dbbackup", "dbbackup.run", startDbBackup)
}


main();