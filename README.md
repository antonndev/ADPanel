<div align="center">
  <a href="https://ad-panel.com">
    <img src="https://cdn.jsdelivr.net/gh/antonndev/ADCDn/ADPanel-Banner.svg" alt="ADPanel Banner" width="100%" />
  </a>

  <br />

  **"A change of thought and perfection found its way."**

  [![License](https://img.shields.io/badge/license-MIT-black?style=flat-square)](./LICENSE)
  [![Docs](https://img.shields.io/badge/docs-ADPanel-blue?style=flat-square)](https://ad-panel.com/adocs)
  [![Discord](https://img.shields.io/badge/Discord-Join%20Community-5865F2?style=flat-square&logo=discord&logoColor=white)](https://ad-panel.com/discord)
<br />
  <br />

  ### ⚡ Quick Deployment

</div>

### ADPanel

```bash
sudo mkdir -p /var/www && sudo git clone https://github.com/antonndev/ADPanel.git /var/www/adpanel && cd /var/www/adpanel && sudo bash ./initialize.sh --choice 1
```

### ADaemon

```bash
curl -fsSL https://raw.githubusercontent.com/antonndev/ADPanel-Daemon/main/install-daemon.sh | tr -d '\r' | sudo bash -s -- --yes
```

---

### Overview
**ADPanel™** | The open-source Docker Panel engineered for speed. Host containers with ease. While others charge for features, we deliver free.

### Architecture
* **Security Rule #1** We bring an Apple-like experience to the developer's workflow.
* **Needing a change** Developers shouldn't have to settle for outdated interfaces. We're ending the era of boring, complex UIs.

### Documentation & Resources
* **Official Website:** [ad-panel.com](https://ad-panel.com)
* **Documentation:** [ad-panel.com/adocs](https://ad-panel.com/adocs)
* **API Reference:** [ad-panel.com/adocs/api](https://ad-panel.com/adocs/api)

### Infrastructure Support
**ADPanel™** isn’t just a game hosting panel, it’s a modern UI capable of running any Docker image on the planet. Start your journey today; our developers are constantly improving the platform to bring an enterprise-grade experience directly on your servers.

---

### Manual Setup

If you prefer to set up ADPanel manually instead of using the one-liner installer:

```bash
# Clone the repository
git clone https://github.com/antonndev/ADPanel.git
cd ADPanel

# Copy the example environment file and fill in your values
cp .env.example .env

# Install dependencies
npm install

# Start the panel
npm start
```

> **Redis is optional.** ADPanel works out of the box with file-based sessions.
> For production, we recommend enabling Redis — just uncomment `SESSION_STORE=redis`
> and set `REDIS_URL` in your `.env` file. If Redis becomes unavailable at runtime,
> the client automatically reconnects with exponential backoff.

### License
Released under the MIT License. Copyright © 2026 - present ADPanel Workers.
