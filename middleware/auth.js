const fs = require("fs"); 
const fsp = require("fs/promises");
const path = require("path");
const bcrypt = require("bcrypt");
const speakeasy = require("speakeasy");

const USERS_FILE = path.join(__dirname, "..", "users.json");

let users = {};
try {
  users = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
} catch {
  users = {};
}

async function saveUsers() {
  try {
    const json = JSON.stringify(users, null, 2);
    await fsp.writeFile(USERS_FILE, json, "utf8"); 
  } catch (e) {
    console.error("Failed to save users data:", e);
  }
}

function authMiddleware(req, res, next) {
  if (req.path.startsWith("/register")) return next();

  const sessionUser = req.session?.user;
  if (!sessionUser) {
    return res.redirect("/login");
  }

  if (!Object.prototype.hasOwnProperty.call(users, sessionUser)) {
    req.session.destroy(() => {});
    return res.redirect("/login");
  }
  const user = users[sessionUser];
  if (!user) {
    req.session.destroy(() => {});
    return res.redirect("/login");
  }

  next();
}

module.exports = { authMiddleware, users, saveUsers };
