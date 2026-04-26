module.exports = {
  apps: [
    {
      name: "adpanel",
      script: "__SCRIPT_PATH__",
      cwd: "__PANEL_DIR__",
      exec_mode: "cluster",
      instances: __INSTANCES__,
      node_args: "--max-old-space-size=__MAX_OLD_SPACE_MB__",
      max_memory_restart: "__WORKER_RAM_MB__M",
      restart_delay: 5000,
      exp_backoff_restart_delay: 250,
      kill_timeout: 5000,
      listen_timeout: 10000,
      env: {
        NODE_ENV: "production"
      }
    }
  ]
};
