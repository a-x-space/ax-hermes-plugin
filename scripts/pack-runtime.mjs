import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const pluginRoot = resolve(scriptDir, '..');
const packageJson = JSON.parse(readFileSync(join(pluginRoot, 'package.json'), 'utf8'));
const artifactDir = join(pluginRoot, 'artifacts');
const stageRoot = mkdtempSync(join(tmpdir(), 'ax-hermes-plugin-'));
const stagePackage = join(stageRoot, 'ax');
const artifactName = `ax-hermes-plugin-${packageJson.version}.tar.gz`;

mkdirSync(stagePackage, { recursive: true });
mkdirSync(artifactDir, { recursive: true });

for (const file of [
  'plugin.yaml',
  '__init__.py',
  'adapter.py',
  'cli.py',
  'storage.py',
  'README.md',
  'after-install.md',
]) {
  copyFileSync(join(pluginRoot, file), join(stagePackage, file));
}

writeFileSync(
  join(stagePackage, 'VERSION'),
  `${packageJson.version}\n`,
);

rmSync(join(artifactDir, artifactName), { force: true });

const result = spawnSync('tar', ['-czf', join(artifactDir, artifactName), '-C', stageRoot, 'ax'], {
  cwd: pluginRoot,
  stdio: 'inherit',
});

rmSync(stageRoot, { recursive: true, force: true });

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

console.log(`Packed Hermes plugin: ${join(artifactDir, artifactName)}`);
