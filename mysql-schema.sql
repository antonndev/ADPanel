CREATE TABLE IF NOT EXISTS users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  password VARCHAR(255) NOT NULL,
  secret VARCHAR(255) NOT NULL,
  admin TINYINT(1) NOT NULL DEFAULT 0,
  avatar_url VARCHAR(2048) NULL,
  preferences JSON NULL,
  recovery_codes TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS nodes (
  id VARCHAR(64) NOT NULL,
  uuid VARCHAR(64) NULL,
  name VARCHAR(120) NOT NULL,
  address VARCHAR(255) NOT NULL,
  ram_mb INT NOT NULL DEFAULT 0,
  disk_gb INT NOT NULL DEFAULT 0,
  ports JSON NOT NULL,
  token_id VARCHAR(64) NULL,
  token VARCHAR(255) NULL,
  created_at BIGINT NULL,
  api_port INT NOT NULL DEFAULT 8080,
  sftp_port INT NOT NULL DEFAULT 2022,
  port_ok TINYINT(1) NULL,
  last_seen BIGINT NULL,
  last_check BIGINT NULL,
  online TINYINT(1) NULL,
  build_config JSON NULL,
  max_upload_mb INT NOT NULL DEFAULT 10240,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_nodes_uuid (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS servers (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(120) NOT NULL,
  display_name VARCHAR(120) NULL,
  legacy_id VARCHAR(120) NULL,
  bot VARCHAR(120) NULL,
  template VARCHAR(64) NULL,
  start VARCHAR(255) NULL,
  node_id VARCHAR(64) NULL,
  ip VARCHAR(255) NULL,
  port INT NULL,
  status VARCHAR(32) NULL,
  runtime JSON NULL,
  docker JSON NULL,
  acl JSON NULL,
  resources JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_servers_name (name),
  KEY idx_servers_node (node_id),
  KEY idx_servers_legacy (legacy_id),
  KEY idx_servers_bot (bot),
  KEY idx_servers_status (status),
  CONSTRAINT fk_servers_node FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_access (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  server_name VARCHAR(120) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_user_access (user_id, server_name),
  CONSTRAINT fk_user_access_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ai_chats (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  title VARCHAR(255) NOT NULL DEFAULT 'New Chat',
  created_by BIGINT UNSIGNED NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_ai_chats_created (created_at),
  CONSTRAINT fk_ai_chats_user FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ai_messages (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  chat_id BIGINT UNSIGNED NOT NULL,
  role ENUM('user', 'assistant', 'system') NOT NULL,
  content LONGTEXT NOT NULL,
  thinking_time_ms INT UNSIGNED NULL,
  model VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_ai_messages_chat (chat_id),
  KEY idx_ai_messages_created (created_at),
  CONSTRAINT fk_ai_messages_chat FOREIGN KEY (chat_id) REFERENCES ai_chats(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS server_planner_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  server_name VARCHAR(120) NOT NULL,
  title VARCHAR(255) NOT NULL,
  prompt LONGTEXT NOT NULL,
  is_done TINYINT(1) NOT NULL DEFAULT 0,
  sort_order INT NOT NULL DEFAULT 0,
  created_by_email VARCHAR(255) NOT NULL,
  updated_by_email VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_server_planner_server (server_name),
  KEY idx_server_planner_sort (server_name, is_done, sort_order, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_status (
  user_id BIGINT UNSIGNED NOT NULL,
  status_text VARCHAR(80) NOT NULL DEFAULT 'Available',
  expires_at BIGINT UNSIGNED NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id),
  KEY idx_user_status_expires (expires_at),
  CONSTRAINT fk_user_status_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sftp_credentials (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  server_name VARCHAR(120) NOT NULL,
  sftp_username VARCHAR(255) NOT NULL,
  sftp_password_hash VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_sftp_user_server (user_id, server_name),
  UNIQUE KEY uniq_sftp_username (sftp_username),
  KEY idx_sftp_server (server_name),
  CONSTRAINT fk_sftp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
