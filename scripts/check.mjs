import { readFile, access } from 'node:fs/promises';
import process from 'node:process';

const requiredFiles = [
  'README.md',
  'AGENTS.md',
  'SECURITY.md',
  'docs/ARCHITECTURE.md',
  'docs/ENGINEER_HANDOFF.md',
  'docs/MILESTONES.md',
  'docs/PRIVACY.md',
  'docs/THREAT_MODEL.md',
  'spec/events.schema.json'
];

export async function validateRepository(root = process.cwd()) {
  for (const path of requiredFiles) {
    await access(new URL(path, `file://${root.replace(/\/$/, '')}/`));
  }

  const schemaText = await readFile(new URL('spec/events.schema.json', `file://${root.replace(/\/$/, '')}/`), 'utf8');
  const schema = JSON.parse(schemaText);

  if (schema.$schema !== 'https://json-schema.org/draft/2020-12/schema') {
    throw new Error('Event schema must use JSON Schema draft 2020-12');
  }

  const eventTypes = schema.properties?.type?.enum;
  if (!Array.isArray(eventTypes) || !eventTypes.includes('audio.frame.v1') || !eventTypes.includes('question.detected.v1')) {
    throw new Error('Core SIDECAR event types are missing');
  }

  return { requiredFileCount: requiredFiles.length, eventTypeCount: eventTypes.length };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const result = await validateRepository();
  console.log(`SIDECAR bootstrap valid: ${result.requiredFileCount} required files, ${result.eventTypeCount} event types.`);
}
