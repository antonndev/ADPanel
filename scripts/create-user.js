"use strict";

const path = require("path");

require("dotenv").config({
  path: path.join(__dirname, "..", ".env"),
  override: true,
});

const speakeasy = require("speakeasy");
const db = require("../db");

let bcrypt;
try {
  bcrypt = require("bcrypt");
} catch {
  bcrypt = require("bcryptjs");
}

function usage() {
  return [
    "Usage:",
    "  sudo node scripts/create-user.js --email user@example.com --password \"pass\" [--admin] [--secret BASE32]",
    "  sudo node scripts/create-user.js --email user@example.com --password-hash \"$2b$...\" [--admin] [--secret BASE32]",
    "  sudo node scripts/create-user.js --update-password --email user@example.com --password \"newpass\" [--current-password \"oldpass\"]",
    "  sudo node scripts/create-user.js --update-password --email user@example.com --password-hash \"$2b$...\" [--current-password \"oldpass\"]",
    "  sudo node scripts/create-user.js --delete --email user@example.com",
  ].join("\n");
}

function parseArgs(argv) {
  const out = { admin: false, delete: false, updatePassword: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") {
      out.help = true;
      continue;
    }
    if (arg === "--admin") {
      out.admin = true;
      continue;
    }
    if (arg === "--delete" || arg === "--remove") {
      out.delete = true;
      continue;
    }
    if (arg === "--update-password" || arg === "--set-password") {
      out.updatePassword = true;
      continue;
    }
    if (arg === "--email") {
      out.email = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--password") {
      out.password = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--password-hash") {
      out.passwordHash = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--current-password") {
      out.currentPassword = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--secret") {
      out.secret = argv[i + 1];
      i += 1;
      continue;
    }
  }
  return out;
}

function requireSudo() {
  if (typeof process.getuid === "function" && process.getuid() !== 0) {
    console.error("This script must be run with sudo.");
    return false;
  }
  return true;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!requireSudo()) {
    return 1;
  }

  if (args.help || !args.email || (!args.delete && !args.updatePassword && !args.password && !args.passwordHash)) {
    console.log(usage());
    return 1;
  }

  const email = String(args.email || "").trim().toLowerCase();
  if (!email) {
    console.error("Invalid email.");
    return 1;
  }

  await db.ensureSchema();

  try {
    await db.query(`
      ALTER TABLE users ADD COLUMN IF NOT EXISTS recovery_codes TEXT NULL
    `);
  } catch (err) {
    if (!err.message?.includes('Duplicate column')) {
      try {
        const cols = await db.query("SHOW COLUMNS FROM users LIKE 'recovery_codes'");
        if (!cols || cols.length === 0) {
          await db.query("ALTER TABLE users ADD COLUMN recovery_codes TEXT NULL");
        }
      } catch (e) {
      }
    }
  }

  if (args.delete) {
    let existing = null;
    if (args.currentPassword) {
      const found = await db.query(
        "SELECT id, password FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
        [email]
      );
      existing = Array.isArray(found) ? found[0] : null;
      if (!existing) {
        console.error("User not found.");
        return 2;
      }
      if (!bcrypt.compareSync(String(args.currentPassword || ""), String(existing.password || ""))) {
        console.error("Current password is incorrect.");
        return 3;
      }
    }

    const result = await db.query("DELETE FROM users WHERE LOWER(email) = LOWER(?)", [email]);
    const affected = result && typeof result.affectedRows === "number" ? result.affectedRows : 0;
    if (!affected) {
      console.error("User not found.");
      return 2;
    }
    console.log(`User deleted: ${email}`);
    return 0;
  }

  if (args.updatePassword) {
    const found = await db.query(
      "SELECT id, password FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
      [email]
    );
    const existing = Array.isArray(found) ? found[0] : null;
    if (!existing) {
      console.error("User not found.");
      return 2;
    }

    if (args.currentPassword) {
      if (!bcrypt.compareSync(String(args.currentPassword || ""), String(existing.password || ""))) {
        console.error("Current password is incorrect.");
        return 3;
      }
    }

    let newHash = args.passwordHash ? String(args.passwordHash).trim() : "";
    if (newHash) {
      if (!/^\$2[aby]\$/.test(newHash)) {
        console.error("Password hash must be a bcrypt hash.");
        return 1;
      }
    } else {
      const rawPassword = String(args.password || "");
      if (!rawPassword) {
        console.error("Password is required.");
        return 1;
      }
      newHash = bcrypt.hashSync(rawPassword, 10);
    }

    await db.query("UPDATE users SET password = ? WHERE id = ?", [newHash, existing.id]);
    console.log(`Password updated: ${email}`);
    return 0;
  }

  let passwordHash = args.passwordHash ? String(args.passwordHash).trim() : "";
  if (passwordHash) {
    if (!/^\$2[aby]\$/.test(passwordHash)) {
      console.error("Password hash must be a bcrypt hash.");
      return 1;
    }
  } else {
    const rawPassword = String(args.password || "");
    if (!rawPassword) {
      console.error("Password is required.");
      return 1;
    }
    passwordHash = bcrypt.hashSync(rawPassword, 10);
  }

  const secret = args.secret
    ? String(args.secret).trim()
    : speakeasy.generateSecret({ length: 20 }).base32;

  const RECOVERY_CODE_COUNT = 7;
  const RECOVERY_CODE_LENGTH = 8;
  const RECOVERY_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

  function generateRecoveryCode() {
    let code = '';
    for (let i = 0; i < RECOVERY_CODE_LENGTH; i++) {
      code += RECOVERY_CODE_CHARS.charAt(Math.floor(Math.random() * RECOVERY_CODE_CHARS.length));
    }
    return code;
  }

  const plainRecoveryCodes = [];
  const hashedRecoveryCodes = [];
  for (let i = 0; i < RECOVERY_CODE_COUNT; i++) {
    const code = generateRecoveryCode();
    plainRecoveryCodes.push(code);
    hashedRecoveryCodes.push(bcrypt.hashSync(code, 10));
  }

  const recoveryCodesJson = JSON.stringify(hashedRecoveryCodes);

  try {
    await db.query(
      `INSERT INTO users (email, password, secret, admin, recovery_codes) VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE password = VALUES(password), secret = VALUES(secret), admin = VALUES(admin), recovery_codes = VALUES(recovery_codes)`,
      [email, passwordHash, secret, args.admin ? 1 : 0, recoveryCodesJson]
    );
  } catch (err) {
    throw err;
  }

  console.log(`User created/updated: ${email}`);
  console.log(`2FA secret: ${secret}`);
  console.log('');
  console.log('\x1b[1m\x1b[33m==================== RECOVERY CODES ====================\x1b[0m');
  console.log('\x1b[1m\x1b[31mSAVE THESE CODES! They can only be shown ONCE!\x1b[0m');
  console.log('\x1b[1m\x1b[31mEach code can only be used ONE TIME.\x1b[0m');
  console.log('');
  plainRecoveryCodes.forEach((code, idx) => {
    console.log(`  \x1b[1m\x1b[36m${idx + 1}. ${code}\x1b[0m`);
  });
  console.log('');
  console.log('\x1b[1m\x1b[33m=========================================================\x1b[0m');
  return 0;
}

async function run() {
  let exitCode = 0;
  try {
    exitCode = await main();
  } catch (err) {
    console.error("[create-user] failed:", err?.message || err);
    exitCode = 1;
  } finally {
    try {
      await db.close();
    } catch (err) {
      console.error("[create-user] close failed:", err?.message || err);
      exitCode = exitCode || 1;
    }
  }
  process.exitCode = exitCode;
}

run();
