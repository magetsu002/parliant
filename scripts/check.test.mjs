import test from 'node:test';
import assert from 'node:assert/strict';
import { validateRepository } from './check.mjs';

test('bootstrap repository contract is internally valid', async () => {
  const result = await validateRepository(process.cwd());
  assert.ok(result.requiredFileCount >= 9);
  assert.ok(result.eventTypeCount >= 8);
});
