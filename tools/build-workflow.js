// Собирает n8n workflow JSON из исходника на Workflow SDK (workflows/src/*.js).
// Использует тот же разборщик, что и MCP n8n (parseWorkflowCode из @n8n/workflow-sdk локального n8n).
//
//   node tools/build-workflow.js workflows/src/TourVenues_10_venue_email_finder.js out.json --id P6dj7b5aOgyPXowF --tag SVSVXzhrtjDxrsSB:TourVenues
//
// Результат импортируется в локальный n8n:  n8n import:workflow --input=out.json
const fs = require('fs');
const path = require('path');

const SDK = path.join(process.env.APPDATA, 'npm', 'node_modules', 'n8n', 'node_modules', '@n8n', 'workflow-sdk');
const { parseWorkflowCode } = require(SDK);

const [src, out, ...rest] = process.argv.slice(2);
if (!src || !out) {
  console.error('usage: node build-workflow.js <src.js> <out.json> [--id <workflowId>] [--name <name>]');
  process.exit(1);
}
const opt = {};
for (let i = 0; i < rest.length; i += 2) opt[rest[i].replace(/^--/, '')] = rest[i + 1];

// Строку import разборщик не принимает (MCP убирает её сам) — убираем так же
const code = fs.readFileSync(src, 'utf8').replace(/^import\s[^;]+;\s*$/m, '');
const wf = parseWorkflowCode(code);
if (opt.id) wf.id = opt.id;
if (opt.name) wf.name = opt.name;
// Импорт перезаписывает настройки: сохраняем доступ для MCP и тег проекта
wf.settings = Object.assign({ executionOrder: 'v1' }, wf.settings, { availableInMCP: true });
if (opt.tag) wf.tags = [{ id: opt.tag.split(':')[0], name: opt.tag.split(':')[1] }];
fs.writeFileSync(out, JSON.stringify(wf, null, 2));
console.log(`OK: ${wf.name} — ${wf.nodes.length} nodes → ${out}`);
