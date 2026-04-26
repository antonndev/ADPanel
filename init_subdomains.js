const db = require('./db');
async function run() {
    try {
        await db.query(`
            CREATE TABLE IF NOT EXISTS subdomains (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              server_id BIGINT UNSIGNED NOT NULL,
              domain VARCHAR(255) NOT NULL,
              status ENUM('pending', 'approved', 'canceled') NOT NULL DEFAULT 'pending',
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              UNIQUE KEY uniq_subdomains_domain (domain),
              KEY idx_subdomains_server (server_id),
              CONSTRAINT fk_subdomains_server FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        `);
        console.log("Subdomains table created.");
        process.exit(0);
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}
run();
