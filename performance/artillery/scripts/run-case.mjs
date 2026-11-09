import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// Runs one or more cases: node scripts/run-case.mjs <case> [<case> ...]
// Each case is cases/<name>.yml. Reports go to reports/<name>-<timestamp>.json.
const artilleryDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const known = [
  'smoke',
  'saga-load',
  'saga-spike',
  'saga-early-outcome',
  'saga-high-failure',
  'saga-missing-outcomes',
  'request-backlog',
  'low-volume',
  'demo-steady',
];

const requested = process.argv.slice(2);
if (requested.length === 0) {
  console.error(`usage: node scripts/run-case.mjs <case> [...]\ncases: ${known.join(', ')}`);
  process.exit(2);
}
const unknown = requested.filter((name) => !known.includes(name));
if (unknown.length > 0) {
  console.error(`unknown case(s): ${unknown.join(', ')}`);
  process.exit(2);
}

mkdirSync(resolve(artilleryDir, 'reports'), { recursive: true });

for (const name of requested) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const report = resolve(artilleryDir, 'reports', `${name}-${stamp}.json`);
  console.log(`\n=== ${name} ===`);
  const result = spawnSync(
    'npx',
    ['artillery', 'run', '--output', report, resolve(artilleryDir, 'cases', `${name}.yml`)],
    { cwd: artilleryDir, stdio: 'inherit', shell: process.platform === 'win32' },
  );
  if (result.status !== 0) {
    console.error(`case ${name} failed (exit ${result.status})`);
    process.exit(result.status ?? 1);
  }
  console.log(`report: ${report}`);
}
