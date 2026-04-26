"use strict";

const express = require("express");
const dns = require("dns").promises;
const db = require("../db");
const nodes = require("../nodes");
const { getTechnicalServerName } = require("../utils/server-name");
const router = express.Router();


async function resolveServer(req, res, next) {
    const serverName = req.params.serverName;
    if (!serverName) return res.status(400).json({ error: "missing server name" });

    try {
        const lowered = String(serverName).trim().toLowerCase();
        const rows = await db.query(
            `SELECT *
             FROM servers
             WHERE LOWER(name) = ? OR LOWER(bot) = ?
             ORDER BY
               CASE
                 WHEN LOWER(name) = ? THEN 0
                 WHEN LOWER(bot) = ? THEN 1
                 ELSE 2
               END,
               id DESC
             LIMIT 1`,
            [lowered, lowered, lowered, lowered]
        );
        if (!rows.length) return res.status(404).json({ error: "server not found" });
        req.server = rows[0];
        next();
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "internal error" });
    }
}



async function checkPerms(req, perm) {
    if (!req.session.user) return false;

    
    const userEmail = req.session.user; 
    let user = null;

    try {
        const userRows = await db.query("SELECT * FROM users WHERE email = ? LIMIT 1", [userEmail]);
        if (userRows.length > 0) user = userRows[0];
    } catch (e) {
        console.error("Error resolving user:", e);
        return false;
    }

    if (!user) return false;
    if (user.admin) return true;

    
    const access = await db.query(
        "SELECT * FROM user_access WHERE user_id = ? AND server_name = ?",
        [user.id, req.server.name]
    );
    if (!access.length) return false; 

    let acl = {};
    try {
        acl = typeof req.server.acl === 'string' ? JSON.parse(req.server.acl) : (req.server.acl || {});
    } catch (e) { acl = {}; }

    const userPerms = acl[userEmail] || {};
    return !!userPerms[perm];
}

router.get("/:serverName", resolveServer, async (req, res) => {
    
    if (!await checkPerms(req, 'subdomain_show')) {
        return res.status(403).json({ error: "permission denied" });
    }

    try {
        const subs = await db.query("SELECT * FROM subdomains WHERE server_id = ?", [req.server.id]);
        res.json({ subdomains: subs });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "db error" });
    }
});


router.post("/:serverName", resolveServer, async (req, res) => {
    
    if (!await checkPerms(req, 'subdomain_add')) {
        return res.status(403).json({ error: "permission denied" });
    }

    const domain = String(req.body.domain || "").trim().toLowerCase();
    if (!domain || domain.length > 253) return res.status(400).json({ error: "invalid domain" });

    
    const hostnameRegex = /^(?!-)[a-z0-9-]{1,63}(?<!-)(\.[a-z0-9-]{1,63})*\.[a-z]{2,}$/;
    if (!hostnameRegex.test(domain)) {
        return res.status(400).json({ error: "invalid domain format" });
    }

    
    let expectedIp = req.server.ip;
    if (!expectedIp && req.server.node_id) {
        try {
            const nodeRows = await db.query("SELECT address FROM nodes WHERE id = ?", [req.server.node_id]);
            if (nodeRows.length) expectedIp = nodeRows[0].address;
        } catch (e) {
            console.error("Node IP lookup failed:", e);
            return res.status(500).json({ error: "internal error" });
        }
    }

    if (!expectedIp) {
        return res.status(500).json({ error: "cannot determine node ip" });
    }

    
    let targetIps = [];
    if (/^(\d{1,3}\.){3}\d{1,3}$/.test(expectedIp)) {
        targetIps = [expectedIp];
    } else {
        try {
            targetIps = await dns.resolve4(expectedIp);
        } catch (e) {
            console.error("Failed to resolve node address:", expectedIp, e);
            return res.status(500).json({ error: "failed to resolve node address" });
        }
    }

    
    try {
        const addresses = await dns.resolve4(domain);
        
        const matched = addresses.some(addr => targetIps.includes(addr));

        if (!matched) {
            return res.status(400).json({
                error: "DNS verification failed",
                details: "Domain does not point to the expected server address"
            });
        }
    } catch (err) {
        
        return res.status(400).json({ error: "DNS lookup failed: record not found" });
    }

    
    let subId;
    try {
        const result = await db.query("INSERT INTO subdomains (server_id, domain, status) VALUES (?, ?, 'approved')", [req.server.id, domain]);
        subId = result.insertId;
    } catch (err) {
        if (err.code === 'ER_DUP_ENTRY') {
            return res.status(400).json({ error: "domain already registered" });
        }
        console.error(err);
        res.status(500).json({ error: "db error" });
        return;
    }

    
    try {
        const { node } = await nodes.remoteContext(req.server.name);
        if (node) {
            const base = nodes.nodeUrl(node);
            const url = `${base}/v1/servers/${encodeURIComponent(getTechnicalServerName(req.server, req.server.name))}/subdomains`;
            await nodes.httpJson(url, {
                method: 'POST',
                headers: nodes.nodeHeaders(node),
                body: JSON.stringify({ domain: domain, action: 'create' })
            });
        }
    } catch (err) {
        console.error("Agent call failed:", err);
        
        await db.query("DELETE FROM subdomains WHERE id = ?", [subId]);
        return res.status(500).json({ error: "agent communication failed" });
    }

    res.json({ ok: true, id: subId });
});

router.post("/:serverName/:id/verify", resolveServer, async (req, res) => {
    
    if (!await checkPerms(req, 'subdomain_add')) {
        return res.status(403).json({ error: "permission denied" });
    }

    const subId = req.params.id;

    let sub, expectedIp;
    try {
        const rows = await db.query("SELECT * FROM subdomains WHERE id = ? AND server_id = ?", [subId, req.server.id]);
        if (!rows.length) return res.status(404).json({ error: "subdomain not found" });
        sub = rows[0];

        expectedIp = req.server.ip;
        if (!expectedIp && req.server.node_id) {
            const nodeRows = await db.query("SELECT address FROM nodes WHERE id = ?", [req.server.node_id]);
            if (nodeRows.length) expectedIp = nodeRows[0].address;
        }
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: "internal error" });
    }

    if (!expectedIp) {
        return res.status(500).json({ error: "cannot determine node ip" });
    }

    
    let targetIps = [];
    if (/^(\d{1,3}\.){3}\d{1,3}$/.test(expectedIp)) {
        targetIps = [expectedIp];
    } else {
        try {
            targetIps = await dns.resolve4(expectedIp);
        } catch (e) {
            console.error("Failed to resolve node address:", expectedIp, e);
            return res.status(500).json({ error: "failed to resolve node address" });
        }
    }

    
    try {
        const addresses = await dns.resolve4(sub.domain);
        
        const matched = addresses.some(addr => targetIps.includes(addr));

        const newStatus = matched ? 'approved' : 'canceled';
        await db.query("UPDATE subdomains SET status = ? WHERE id = ?", [newStatus, subId]);

        
        if (matched) {
            const { node } = await nodes.remoteContext(req.server.name);
            if (node) {
                const base = nodes.nodeUrl(node);
                const url = `${base}/v1/servers/${encodeURIComponent(getTechnicalServerName(req.server, req.server.name))}/subdomains`;
                await nodes.httpJson(url, {
                    method: 'POST',
                    headers: nodes.nodeHeaders(node),
                    body: JSON.stringify({ domain: sub.domain, action: 'create' })
                });
            }
        }

        res.json({ ok: true, status: newStatus, expected: expectedIp, found: addresses });
    } catch (err) {
        console.error("DNS error:", err.message);
        
        await db.query("UPDATE subdomains SET status = 'canceled' WHERE id = ?", [subId]);
        res.json({ ok: true, status: 'canceled', error: "dns lookup failed" });
    }
});

router.delete("/:serverName/:id", resolveServer, async (req, res) => {
    
    if (!await checkPerms(req, 'subdomain_add')) {
        return res.status(403).json({ error: "permission denied" });
    }

    let domain;
    try {
        const rows = await db.query("SELECT domain FROM subdomains WHERE id = ? AND server_id = ?", [req.params.id, req.server.id]);
        if (!rows.length) return res.status(404).json({ error: "subdomain not found" });
        domain = rows[0].domain;
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: "internal error" });
    }

    try {
        
        const { node } = await nodes.remoteContext(req.server.name);
        if (node) {
            const base = nodes.nodeUrl(node);
            const url = `${base}/v1/servers/${encodeURIComponent(getTechnicalServerName(req.server, req.server.name))}/subdomains`;
            
            await nodes.httpJson(url, {
                method: 'POST',
                headers: nodes.nodeHeaders(node),
                body: JSON.stringify({ domain: domain, action: 'delete' })
            });
        }

        await db.query("DELETE FROM subdomains WHERE id = ? AND server_id = ?", [req.params.id, req.server.id]);
        res.json({ ok: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "db/agent error" });
    }
});

module.exports = router;
