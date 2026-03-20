"use strict";

const fs = require("fs");
const mysql = require("mysql2/promise");
const path = require("path");
const { URL } = require("url");

const MYSQL_URL = String(process.env.MYSQL_URL || "").trim();
const MYSQL_HOST = String(process.env.MYSQL_HOST || "127.0.0.1").trim();
const MYSQL_PORT = parseInt(process.env.MYSQL_PORT || "3306", 10);
const MYSQL_USER = String(process.env.MYSQL_USER || "root").trim();
const MYSQL_PASSWORD = String(process.env.MYSQL_PASSWORD || "").trim();
const MYSQL_DATABASE = String(process.env.MYSQL_DATABASE || process.env.MYSQL_DB || "").trim();
const MYSQL_POOL_SIZE = parseInt(process.env.MYSQL_POOL_SIZE || "10", 10);

function configFromUrl(raw) {
  if (!raw) return null;
  try {
    const url = new URL(raw);
    if (url.protocol !== "mysql:") return null;
    return {
      host: url.hostname,
      port: url.port ? Number(url.port) : 3306,
      user: decodeURIComponent(url.username || ""),
      password: decodeURIComponent(url.password || ""),
      database: url.pathname ? url.pathname.replace(/^\//, "") : "",
    };
  } catch {
    return null;
  }
}

const urlConfig = configFromUrl(MYSQL_URL);
const dbName = (urlConfig && urlConfig.database) || MYSQL_DATABASE;
if (!dbName) {
  console.warn("[db] MYSQL_DATABASE is not set; database selection may fail.");
}

const pool = mysql.createPool({
  host: (urlConfig && urlConfig.host) || MYSQL_HOST,
  port: (urlConfig && urlConfig.port) || MYSQL_PORT || 3306,
  user: (urlConfig && urlConfig.user) || MYSQL_USER,
  password: (urlConfig && urlConfig.password) || MYSQL_PASSWORD,
  ...(dbName ? { database: dbName } : {}),
  waitForConnections: true,
  connectionLimit: Number.isFinite(MYSQL_POOL_SIZE) ? MYSQL_POOL_SIZE : 50,
  queueLimit: 5000,
  acquireTimeout: 10000,
  decimalNumbers: true,
});

async function query(sql, params = []) {
  const [rows] = await pool.execute(sql, params);
  return rows;
}

function parseSchemaStatements(sql) {
  return String(sql)
    .split(";")
    .map((stmt) => stmt.trim())
    .filter(Boolean);
}

async function ensureSchema() {
  const schemaPath = path.join(__dirname, "mysql-schema.sql");
  if (!fs.existsSync(schemaPath)) {
    throw new Error("mysql-schema.sql not found");
  }
  const sql = fs.readFileSync(schemaPath, "utf8");
  const statements = parseSchemaStatements(sql);
  if (!statements.length) {
    throw new Error("mysql-schema.sql is empty");
  }
  const conn = await pool.getConnection();
  try {
    for (const stmt of statements) {
      await conn.query(stmt);
    }
  } finally {
    conn.release();
  }
}

module.exports = {
  pool,
  query,
  ensureSchema,
  async close() {
    await pool.end();
  },
};
