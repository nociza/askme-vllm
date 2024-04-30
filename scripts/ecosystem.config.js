const numInstances = 1;
const apps = [];

for (let i = 0; i < numInstances; i++) {
  const socketPath = `/tmp/uvicorn_sockets/uvicorn_app_${i}.sock`;

  apps.push({
    name: `fleecekm-batch-${i}`,
    script: '/srv/home/yuehengzhang/.local/bin/poetry',
    args: `run uvicorn fleecekmbackend.main:app --uds ${socketPath}`,
    instances: 1,
    autorestart: true,
    max_restarts: 10,
    exec_mode: 'fork',
    log_date_format: 'YYYY-MM-DD HH:mm Z',
    error_file: `logs/err_${i}.log`,
    out_file: `logs/out_${i}.log`,
    merge_logs: false,
  });
}

module.exports = { apps };
