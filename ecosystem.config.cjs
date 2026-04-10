/**
 * PM2 部署
 *
 * 使用前：
 *   1. cd 本目录 && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
 *   2. 复制 .env.example 为 .env 并填写各项密钥（chmod 600 .env）
 *
 * 启动：pm2 start ecosystem.config.cjs
 * 保存：pm2 save && pm2 startup
 */
const path = require('path');
const fs = require('fs');

const root = __dirname;
const venvPython = path.join(root, '.venv', 'bin', 'python3');

// 从 .env 文件加载环境变量（仅补充，不覆盖系统已有变量）
function loadDotEnv() {
  const envFile = path.join(root, '.env');
  if (!fs.existsSync(envFile)) return {};
  const vars = {};
  fs.readFileSync(envFile, 'utf8').split('\n').forEach(line => {
    // 匹配 KEY=VALUE，并支持行尾注释
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (m) {
      // 去掉可能存在的行尾注释并 trim
      const key = m[1];
      let val = m[2].split('#')[0].trim();
      // 去掉包裹的引号
      val = val.replace(/^["'](.*)["']$/, '$1');
      vars[key] = val;
    }
  });
  return vars;
}

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
        // 默认只监听本机，如需公网访问改为 '0.0.0.0'（建议配 Nginx 反代）
        AE_HOST: '127.0.0.1',
        ...loadDotEnv(),
      },
    },
  ],
};
