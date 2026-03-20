(() => {
    const bot = document.documentElement.dataset.bot;
    if (!bot) {
        console.error("Bot name not found in dataset");
        return;
    }

    const resMemoryLabel = document.getElementById('resMemoryLabel');
    const resMemoryBar = document.getElementById('resMemoryBar');
    const resDiskLabel = document.getElementById('resDiskLabel');
    const resDiskBar = document.getElementById('resDiskBar');
    const resCpuLabel = document.getElementById('resCpuLabel');
    const resCpuBar = document.getElementById('resCpuBar');
    const resUptimeLabel = document.getElementById('resUptimeLabel');

    let resourceStatsUptimeInterval = null;
    let lastFetchedUptime = null;
    let uptimeFetchedAt = null;

    const sio = window.io;
    let socket;
    if (typeof sio === 'function') {
        try { socket = sio(); } catch { socket = null; }
    }
    if (!socket) {
        console.error('Socket.IO not available for resource popup');
        return;
    }

    function formatBytes(bytes, decimals = 2) {
        if (!+bytes) return '0 B';
        const k = 1024;
        const dm = decimals < 0 ? 0 : decimals;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
    }

    function formatUptime(seconds) {
        if (!seconds || seconds < 0) return '—';
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const mins = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);
        const parts = [];
        if (days > 0) parts.push(`${days}d`);
        if (hours > 0) parts.push(`${hours}h`);
        if (mins > 0) parts.push(`${mins}m`);
        if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
        return parts.join(' ');
    }

    function onResourceData(data) {
        if (!data || data.server !== bot) return;
        const isRunning = data.status === 'running' || data.status === 'online';

        if (data.memory && isRunning) {
            let usedMb, limitMb, pct;
            if (typeof data.memory === 'object') {
                usedMb = data.memory.used || 0;
                limitMb = data.memory.total || 0;
                pct = data.memory.percent ?? (limitMb > 0 ? Math.min(100, Math.round((usedMb / limitMb) * 100)) : 0);
            } else {
                usedMb = data.memory; limitMb = 0; pct = 0;
            }
            resMemoryLabel.textContent = limitMb > 0
                ? `${formatBytes(usedMb * 1048576)} / ${formatBytes(limitMb * 1048576)} (${Math.round(pct)}%)`
                : `${formatBytes(usedMb * 1048576)} / Unlimited`;
            resMemoryBar.style.width = `${pct}%`;
        } else {
            resMemoryLabel.textContent = '—';
            resMemoryBar.style.width = '0%';
        }

        if (data.disk) {
            let usedGb, limitGb, pct;
            if (typeof data.disk === 'object') {
                usedGb = data.disk.used || 0;
                limitGb = data.disk.total || 0;
                pct = data.disk.percent ?? (limitGb > 0 ? Math.min(100, Math.round((usedGb / limitGb) * 100)) : 0);
            } else {
                usedGb = data.disk / 1024; limitGb = 0; pct = 0;
            }
            resDiskLabel.textContent = limitGb > 0
                ? `${formatBytes(usedGb * 1073741824)} / ${formatBytes(limitGb * 1073741824)} (${Math.round(pct)}%)`
                : `${formatBytes(usedGb * 1073741824)} / Unlimited`;
            resDiskBar.style.width = `${pct}%`;
        } else {
            resDiskLabel.textContent = '—';
            resDiskBar.style.width = '0%';
        }

        if (data.cpu !== undefined && data.cpu !== null && isRunning) {
            const cpuMax = data.cpuLimit || 100;
            const cpuPct = Math.min(cpuMax, Math.round(data.cpu));
            const cpuBarWidth = cpuMax > 0 ? Math.min(100, (cpuPct / cpuMax) * 100) : 0;
            resCpuLabel.textContent = `${cpuPct}% / ${Math.round(cpuMax)}%`;
            resCpuBar.style.width = `${cpuBarWidth}%`;
        } else {
            resCpuLabel.textContent = '—';
            resCpuBar.style.width = '0%';
        }

        if (data.uptime && data.uptime > 0 && isRunning) {
            lastFetchedUptime = data.uptime;
            uptimeFetchedAt = Date.now();
            resUptimeLabel.textContent = formatUptime(data.uptime);
            startUptimeCounter();
        } else {
            lastFetchedUptime = null;
            uptimeFetchedAt = null;
            stopUptimeCounter();
            resUptimeLabel.textContent = isRunning ? 'Starting...' : 'Offline';
        }
    }

    function startUptimeCounter() {
        stopUptimeCounter();
        if (lastFetchedUptime === null || uptimeFetchedAt === null) return;
        resourceStatsUptimeInterval = setInterval(() => {
            if (lastFetchedUptime === null || uptimeFetchedAt === null) {
                stopUptimeCounter();
                return;
            }
            const elapsed = Math.floor((Date.now() - uptimeFetchedAt) / 1000);
            const currentUptime = lastFetchedUptime + elapsed;
            resUptimeLabel.textContent = formatUptime(currentUptime);
        }, 1000);
    }

    function stopUptimeCounter() {
        if (resourceStatsUptimeInterval) {
            clearInterval(resourceStatsUptimeInterval);
            resourceStatsUptimeInterval = null;
        }
    }

    let subscribed = false;

    function subscribe() {
        if (subscribed) return;
        subscribed = true;
        socket.on('resources:data', onResourceData);
        socket.emit('resources:subscribe', { server: bot });
    }

    function unsubscribe() {
        if (!subscribed) return;
        subscribed = false;
        socket.off('resources:data', onResourceData);
        socket.emit('resources:unsubscribe', { server: bot });
        stopUptimeCounter();
    }

    socket.on('connect', () => {
        if (subscribed) {
            subscribed = false;
            subscribe();
        } else if (document.visibilityState !== 'hidden') {
            subscribe();
        }
    });

    if (document.visibilityState !== 'hidden') {
        if (socket.connected) subscribe();
    }

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'hidden') {
            unsubscribe();
            return;
        }
        subscribe();
    });

    window.addEventListener('beforeunload', () => {
        unsubscribe();
    });
})();
