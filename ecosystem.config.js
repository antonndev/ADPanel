module.exports = {
  apps: [
    {
      name: "adpanel",
      script: "/ADPanel/ADPanel-Enterprise/index.js",
      cwd: "/ADPanel/ADPanel-Enterprise",
      exec_mode: "cluster",
      instances: 1,
      node_args: "--max-old-space-size=5399",
      max_memory_restart: "6352M",
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
