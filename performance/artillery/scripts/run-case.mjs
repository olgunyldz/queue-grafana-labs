import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const artilleryDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const root = resolve(artilleryDir, '../..');
const cases = {
  // Normal flow: fast_send followed by fast_send_success / fast_send_failed.
  'both-open': {
    file: 'cases/both-open.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Consumer stopped, only fast_send is sent: the fast_send backlog builds up.
  'fast-send-backlog': {
    file: 'cases/fast-send-backlog.yml',
    start: ['producer'],
    stop: ['consumer'],
  },
  // No fast_send is sent at all, only the outcome events.
  'no-fast-send': {
    file: 'cases/no-fast-send.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Alert test: ~25% of fast_send never get an outcome, for 8 minutes.
  'missing-outcomes': {
    file: 'cases/missing-outcomes.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Alert test: 50% failures for 7 minutes (failed count + failed/success ratio).
  'high-failure': {
    file: 'cases/high-failure.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Alert test: 30 transfers/s for 7 minutes (send + outcome volume high).
  'fast-send-spike': {
    file: 'cases/fast-send-spike.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Alert test: ~0.2 transfers/s for 7 minutes (volume low; business hours only).
  'low-volume': {
    file: 'cases/low-volume.yml',
    start: ['producer', 'consumer'],
    stop: [],
  },
  // Producer stopped: only the consumer's health/metrics are checked.
  'consumer-only': {
    file: 'cases/producer-closed-consumer-open.yml',
    start: ['consumer'],
    stop: ['producer'],
    target: 'consumer',
  },
};

// Services under test (C# / .NET). The producer replicas sit behind the producer-lb
// load balancer; the consumer has 2 replicas in one consumer group.
const target = {
  services: { producer: ['dotnet-producer', 'producer-lb'], consumer: ['dotnet-consumer'] },
  urls: { producer: 'http://localhost:18081', consumer: 'http://localhost:2114' },
};

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: root, stdio: 'inherit', shell: process.platform === 'win32', ...options });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

function compose(action, services) {
  if (services.length === 0) return;
  run('docker', ['compose', action, '-d', ...services]);
}

function stop(services) {
  if (services.length === 0) return;
  run('docker', ['compose', 'stop', ...services]);
}

async function waitHealthy(services, timeoutMs = 60_000) {
  for (const service of services) {
    const url = `${target.urls[service]}/healthz`;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      try {
        const res = await fetch(url);
        if (res.ok) break;
      } catch {}
      if (Date.now() > deadline) throw new Error(`${service} did not become healthy at ${url}`);
      await new Promise((r) => setTimeout(r, 1000));
    }
    console.log(`${service} is healthy`);
  }
}

async function test(name) {
  const selected = cases[name];
  if (!selected) throw new Error(`Unknown case: ${name}`);
  const services = (roles) => roles.flatMap((role) => target.services[role]);
  // The load balancer is not stopped: it keeps running and just returns 502 while no producer is up.
  stop(['dotnet-producer', 'dotnet-consumer']);
  compose('up', services(selected.start));
  await waitHealthy(selected.start);
  mkdirSync(resolve(artilleryDir, 'reports'), { recursive: true });
  const report = `reports/${name}-${new Date().toISOString().replaceAll(':', '-')}.json`;
  const url = target.urls[selected.target ?? 'producer'];
  console.log(`Running ${name} against ${url}`);
  run('npx', ['--no-install', 'artillery', 'run', '--target', url, '--output', report, selected.file], { cwd: artilleryDir });
  console.log(`Report written to ${report}`);
}

const requested = process.argv[2] ?? 'both-open';
if (requested === 'all') {
  for (const name of Object.keys(cases)) await test(name);
} else {
  await test(requested);
}
