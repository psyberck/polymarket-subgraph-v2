import { basename, dirname, resolve } from 'node:path';
import { parseConfig } from '#supervisor/config.ts';
import { restoreAt } from '#supervisor/restore.ts';
import { pool } from '#tools/postgres.ts';

const [arg] = process.argv.slice(2);
if (!arg) {
  console.error('usage: restore.ts <output dir>/<end block id>');
  process.exit(2);
}

const dir = resolve(arg);
const endBlockId = Number(basename(dir));
if (!Number.isSafeInteger(endBlockId) || endBlockId <= 0) {
  console.error(`${dir}: the folder must be named after its end block id`);
  process.exit(2);
}

try {
  await restoreAt(parseConfig(['--output-dir', dirname(dir)]), endBlockId);
} finally {
  await pool.end();
}
