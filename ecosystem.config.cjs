/**
 * PM2 部署（无 Nginx，直连 Flask 端口）
 *
 * 使用前：
 *   cd 本目录 && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
 * 若不用虚拟环境，把 interpreter 改成系统 Python，例如 '/usr/bin/python3'
 *
 * 启动：pm2 start ecosystem.config.cjs
 * 保存：pm2 save && pm2 startup
 */
const path = require('path');

const root = __dirname;
const venvPython = path.join(root, '.venv', 'bin', 'python3');

module.exports = {
  apps: [
    {
      name: 'ae',
      script: path.join(root, 'ae_server.py'),
      cwd: root,
      interpreter: venvPython,
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 20,
      min_uptime: '15s',
      restart_delay: 5000,
      kill_timeout: 15000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: '1',
      },
    },
  ],
};
