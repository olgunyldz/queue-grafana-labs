import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const artilleryDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const cases = {
  // Normal FAST flow: request followed by success / failed.
  'both-open': {
    file: 'cases/both-open.yml',
  },
  // Mixed high-load test covering FAST, Havale and EFT endpoints.
  'mixed-high-load': {
    file: 'cases/mixed-high-load.yml',
  },
  // Only requests are sent; with the FAST reconciler stopped the request backlog builds up.
  'request-backlog': {
    file: 'cases/request-backlog.yml',
  },
  // No request is sent at all, only the outcome events.
  'no-request': {
    file: 'cases/no-request.yml',
  },
  // Alert test: ~25% of the requests never get an outcome, for 8 minutes.
  'missing-outcomes': {
    file: 'cases/missing-outcomes.yml',
  },
  // Alert test: 50% failures for 7 minutes (failed count + failed/success ratio).
  'high-failure': {
    file: 'cases/high-failure.yml',
  },
  // Alert test: 30 transfers/s for 7 minutes (request + outcome volume high).
  'request-spike': {
    file: 'cases/request-spike.yml',
  },
  // Alert test: ~0.2 transfers/s for 7 minutes (volume low; business hours only).
  'low-volume': {
    file: 'cases/low-volume.yml',
  },
  // Consumer metrics endpoint check.
  'consumer-only': {
    file: 'cases/producer-closed-consumer-open.yml',
    target: 'consumer',
  },
  // Contract smoke test for FAST/EFT directional and Havale endpoints.
  'directional-endpoints': {
    file: 'cases/directional-endpoints.yml',
  },
};

// Services under test (C# / .NET). The producer replicas sit behind the producer-lb
// load balancer; the consumer has 2 replicas in one consumer group.
const target = {
  urls: { producer: 'http://127.0.0.1:8080', consumer: 'http://127.0.0.1:2114' },
};

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: 'inherit', shell: process.platform === 'win32', ...options });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

async function test(name) {
  const selected = cases[name];
  if (!selected) throw new Error(`Unknown case: ${name}`);
  // The performance runner never changes service state or performs health checks.
  // Services must be prepared externally before the scenario starts.
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
