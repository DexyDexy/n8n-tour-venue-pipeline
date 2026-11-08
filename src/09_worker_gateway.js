// TourVenues_09_worker_gateway — исходник для n8n Workflow SDK. Живёт на ПК (локальный n8n), включён (published).
// Шлюз между Telegram-ботом на VPS (TourVenues_08_bot_controller) и рабочими workflow на ПК.
// VPS обращается сюда через Tailscale: POST http://<WORKER_HOST>:5678/webhook/tourvenues-gateway,
// заголовок X-TourVenues-Key (credential «TourVenues Gateway Key», одинаковый на ПК и VPS).
// Тело запроса: { action: 'ping' | 'status' | 'run' | 'unlock', job: '11' | '12' | '10' | 'all', dryRun, chatId, requestedBy }.
//   ping   — «ПК жив», идёт ли прогон;
//   status — идёт ли прогон + сколько баннеров / строк ждут в 01, 02, 03, жива ли Ollama (остаток Tavily с v003 не спрашивается);
//   run    — ставит блокировку, сразу отвечает «запущено», затем по очереди вызывает workflow (all = 11 → 12 → 10)
//            и снимает блокировку. Ошибка шага — сообщение в Telegram (запустившему и владельцу), цепочка останавливается;
//   unlock — снять блокировку вручную (если прогон оборвался, например n8n перезапустили).
// Блокировка — таблица данных tourvenues_bot_lock (одна строка lockKey = lock); старше 12 часов считается зависшей
// (TourVenues_10 без лимита строк может идти несколько часов).
// Отчёты о результатах шлют сами рабочие workflow (Recipients → Send Telegram).
import { workflow, node, trigger, switchCase, splitInBatches, nextBatch, expr } from '@n8n/workflow-sdk';

const GOOGLE = { googleApi: { id: 'REPLACE_ME', name: 'Google Sheets (service-account)' } };
const TELEGRAM = { telegramApi: { id: 'REPLACE_ME', name: 'TourVenues_Robot' } };
const GATEWAY_KEY = { httpHeaderAuth: { id: 'REPLACE_ME', name: 'TourVenues Gateway Key' } };
const LOCK_TABLE = { __rl: true, mode: 'id', value: 'BVF5DTZzXZIK5LPY', cachedResultName: 'tourvenues_bot_lock' };
const SHEET1 = { __rl: true, mode: 'list', value: 'gid=0', cachedResultName: 'Sheet1' };
const EXTRACTED_ID = '<SHEET_EXTRACTED_ID>'; // 02_OUT_extractedData
const NEWDATA_ID = '<SHEET_NEW_VENUES_ID>'; // 03_OUT_newData
const IN_FOLDER = '<DRIVE_FOLDER_INBOX_ID>'; // 01_OUT_Banners
const OWNER_CHAT = '<OWNER_TELEGRAM_CHAT_ID>';

const LOCK_SCHEMA = [
  { id: 'lockKey', displayName: 'lockKey', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
  { id: 'job', displayName: 'job', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
  { id: 'jobName', displayName: 'jobName', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
  { id: 'byName', displayName: 'byName', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
  { id: 'chatId', displayName: 'chatId', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
  { id: 'startedAt', displayName: 'startedAt', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true }
];

// ── Узлы ────────────────────────────────────────────────────────────────

const webhook = trigger({
  type: 'n8n-nodes-base.webhook',
  version: 2.2,
  config: {
    name: 'Webhook',
    parameters: {
      httpMethod: 'POST',
      path: 'tourvenues-gateway',
      authentication: 'headerAuth',
      responseMode: 'responseNode',
      options: {}
    },
    credentials: GATEWAY_KEY
  }
});

const readLock = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Read Lock',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      resource: 'row',
      operation: 'get',
      dataTableId: LOCK_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'lockKey', condition: 'eq', keyValue: 'lock' }] },
      returnAll: true
    }
  }
});

const decide = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Decide',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const body = $('Webhook').first().json.body || {};
const L = $('Read Lock').all().map(i => i.json).find(r => r.lockKey === 'lock') || {};
const TTL = 12 * 3600 * 1000; // блокировка старше 12 часов — зависшая, не мешает
const started = L.startedAt ? Date.parse(L.startedAt) : 0;
const busy = !!L.job && Date.now() - started < TTL;
const lock = L.job ? { job: L.job, jobName: L.jobName, by: L.byName, since: L.startedAt, stale: !busy } : null;
const W = {
  '11': { id: 'REPLACE_ME', name: 'TourVenues_11: баннеры → 02' },
  '12': { id: 'REPLACE_ME', name: 'TourVenues_12: сверка с базой → 03' },
  '10': { id: 'REPLACE_ME', name: 'TourVenues_10: поиск контактов → база' }
};
const JOBS = { '11': ['11'], '12': ['12'], '10': ['10'], all: ['11', '12', '10'] };
const NAMES = { '11': 'Баннеры', '12': 'Сверка с базой', '10': 'Поиск контактов', all: 'Весь конвейер (баннеры → сверка → контакты)' };
const action = String(body.action || '');
const respond = response => [{ json: { route: 'respond', response } }];
if (action === 'ping') return respond({ ok: true, busy, lock });
if (action === 'status') return [{ json: { route: 'status', busy, lock } }];
if (action === 'unlock') return [{ json: { route: 'unlock', response: { ok: true, unlocked: !!L.job, lock } } }];
if (action !== 'run') return respond({ ok: false, error: 'unknown action: ' + action });
const job = String(body.job || '');
if (!JOBS[job]) return respond({ ok: false, error: 'unknown job: ' + job });
if (busy) return respond({ ok: true, started: false, busy: true, lock });
const params = { dryRun: body.dryRun === true, chatId: String(body.chatId || ''), requestedBy: String(body.requestedBy || '') };
return [{ json: {
  route: 'run', job, jobName: NAMES[job], startedAt: new Date().toISOString(), ...params,
  steps: JOBS[job].map(k => ({ workflowId: W[k].id, stepName: W[k].name, ...params })),
  response: { ok: true, started: true, job, jobName: NAMES[job], dryRun: params.dryRun }
} }];`
    }
  }
});

const route = switchCase({
  version: 3.2,
  config: {
    name: 'Route',
    parameters: {
      rules: {
        values: [
          { outputKey: 'respond', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'respond', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'status', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'status', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'run', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'run', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'unlock', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'unlock', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } }
        ]
      },
      options: {}
    }
  }
});

const respond = node({
  type: 'n8n-nodes-base.respondToWebhook',
  version: 1.5,
  config: {
    name: 'Respond',
    parameters: { respondWith: 'json', responseBody: expr('{{ JSON.stringify($json.response) }}'), options: {} }
  }
});

// ── status ──

const ollamaPing = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Ollama Ping',
    executeOnce: true,
    alwaysOutputData: true,
    onError: 'continueRegularOutput',
    parameters: { method: 'GET', url: 'http://localhost:11434/api/version', options: { timeout: 5000 } }
  }
});

const listBanners = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'List Banners',
    executeOnce: true,
    alwaysOutputData: true,
    onError: 'continueRegularOutput',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'fileFolder',
      searchMethod: 'query',
      queryString: `'${IN_FOLDER}' in parents and mimeType contains 'image/' and trashed = false`,
      returnAll: true,
      filter: {},
      options: { fields: ['id', 'name'] }
    },
    credentials: GOOGLE
  }
});

const readExtracted = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read 02',
    executeOnce: true,
    alwaysOutputData: true,
    onError: 'continueRegularOutput',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: EXTRACTED_ID },
      sheetName: SHEET1,
      options: {}
    },
    credentials: GOOGLE
  }
});

const readNewData = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read 03',
    executeOnce: true,
    alwaysOutputData: true,
    onError: 'continueRegularOutput',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: NEWDATA_ID },
      sheetName: SHEET1,
      options: {}
    },
    credentials: GOOGLE
  }
});

const buildStatus = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Build Status',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `// Те же правила, что в TourVenues_12 (итоговые пометки в 02) и TourVenues_10 (отметка processed в 03)
const d = $('Decide').first().json;
const S = v => String(v == null ? '' : v).trim();
const rows = name => $(name).all().map(i => i.json).filter(r => r.row_number && S(r.NAME));
const failed = name => $(name).all().some(i => i.json.error);
const FINAL = /^(added to 03_OUT_newData|skipped as )/i;
const r02 = rows('Read 02');
const r03 = rows('Read 03');
const banners = $('List Banners').all().filter(i => i.json.id).length;
return [{ json: { response: {
  ok: true, busy: d.busy, lock: d.lock,
  banners: failed('List Banners') ? null : banners,
  new02: failed('Read 02') ? null : r02.filter(r => !S(r['Automation Notes'])).length,
  review02: failed('Read 02') ? null : r02.filter(r => S(r['Automation Notes']) && !FINAL.test(S(r['Automation Notes']))).length,
  todo03: failed('Read 03') ? null : r03.filter(r => !/^processed\\b/i.test(S(r['Automation Status']))).length,
  ollama: !!$('Ollama Ping').first().json.version
} } }];`
    }
  }
});

const respondStatus = node({
  type: 'n8n-nodes-base.respondToWebhook',
  version: 1.5,
  config: {
    name: 'Respond Status',
    parameters: { respondWith: 'json', responseBody: expr('{{ JSON.stringify($json.response) }}'), options: {} }
  }
});

// ── run ──

const writeLock = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Write Lock',
    parameters: {
      resource: 'row',
      operation: 'upsert',
      dataTableId: LOCK_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'lockKey', condition: 'eq', keyValue: 'lock' }] },
      columns: {
        mappingMode: 'defineBelow',
        value: {
          lockKey: 'lock',
          job: expr("{{ $('Decide').first().json.job }}"),
          jobName: expr("{{ $('Decide').first().json.jobName }}"),
          byName: expr("{{ $('Decide').first().json.requestedBy }}"),
          chatId: expr("{{ $('Decide').first().json.chatId }}"),
          startedAt: expr("{{ $('Decide').first().json.startedAt }}")
        },
        matchingColumns: [],
        schema: LOCK_SCHEMA,
        attemptToConvertTypes: false,
        convertFieldsToString: false
      },
      options: {}
    }
  }
});

const respondStarted = node({
  type: 'n8n-nodes-base.respondToWebhook',
  version: 1.5,
  config: {
    name: 'Respond Started',
    parameters: { respondWith: 'json', responseBody: expr("{{ JSON.stringify($('Decide').first().json.response) }}"), options: {} }
  }
});

const steps = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Steps',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `return $('Decide').first().json.steps.map(s => ({ json: s }));`
    }
  }
});

const loopSteps = splitInBatches({ version: 3, config: { name: 'Loop Steps', parameters: { batchSize: 1, options: {} } } });

// Шаг получает весь элемент (workflowId, stepName, dryRun, chatId, requestedBy) — From Bot в рабочем workflow
// принимает его как есть
const runStep = node({
  type: 'n8n-nodes-base.executeWorkflow',
  version: 1.4,
  config: {
    name: 'Run Step',
    onError: 'continueErrorOutput',
    parameters: {
      mode: 'once',
      source: 'database',
      workflowId: { __rl: true, mode: 'id', value: expr('{{ $json.workflowId }}') },
      options: { waitForSubWorkflow: true }
    }
  }
});

// После шага в цикл идёт снова элемент шага (выход Run Step — это ответ Telegram из рабочего workflow)
const stepDone = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Step Done',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `return [{ json: $('Loop Steps').first().json }];`
    }
  }
});

const stepFailed = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Step Failed',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const step = $('Loop Steps').first().json;
const err = $input.first().json.error;
const msg = String((err && (err.message || err.description)) || err || 'неизвестная ошибка').slice(0, 500);
const esc = t => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const text = '❌ <b>Ошибка: ' + esc(step.stepName) + '</b>' + (step.dryRun ? ' (тестовый режим)' : '') +
  '\\n' + esc(msg) + '\\nСледующие шаги не запускались. Подробности — в журнале запусков n8n на ПК.';
const ids = [...new Set([step.chatId, '${OWNER_CHAT}'].filter(Boolean))];
return ids.map(chatId => ({ json: { chatId, text: (chatId !== step.chatId && step.requestedBy ? '👤 Запустил: ' + esc(step.requestedBy) + '\\n' : '') + text } }));`
    }
  }
});

const sendError = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Error',
    onError: 'continueRegularOutput',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      additionalFields: { appendAttribution: false, parse_mode: 'HTML' }
    },
    credentials: TELEGRAM
  }
});

// пустая строка блокировки = «ничего не идёт»
const EMPTY_LOCK = {
  mappingMode: 'defineBelow',
  value: { lockKey: 'lock', job: '', jobName: '', byName: '', chatId: '', startedAt: '' },
  matchingColumns: [],
  schema: LOCK_SCHEMA,
  attemptToConvertTypes: false,
  convertFieldsToString: false
};

const clearLock = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Clear Lock',
    executeOnce: true,
    parameters: {
      resource: 'row',
      operation: 'upsert',
      dataTableId: LOCK_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'lockKey', condition: 'eq', keyValue: 'lock' }] },
      columns: EMPTY_LOCK,
      options: {}
    }
  }
});

// ── unlock ──

const unlock = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Unlock',
    executeOnce: true,
    parameters: {
      resource: 'row',
      operation: 'upsert',
      dataTableId: LOCK_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'lockKey', condition: 'eq', keyValue: 'lock' }] },
      columns: EMPTY_LOCK,
      options: {}
    }
  }
});

const respondUnlock = node({
  type: 'n8n-nodes-base.respondToWebhook',
  version: 1.5,
  config: {
    name: 'Respond Unlock',
    parameters: { respondWith: 'json', responseBody: expr("{{ JSON.stringify($('Decide').first().json.response) }}"), options: {} }
  }
});

// ── Связи ───────────────────────────────────────────────────────────────

export default workflow('tourvenues-09', 'TourVenues_09_worker_gateway_v003')
  .add(webhook)
  .to(readLock)
  .to(decide)
  .to(route
    .onCase(0, respond)
    .onCase(1, ollamaPing.to(listBanners).to(readExtracted).to(readNewData).to(buildStatus).to(respondStatus))
    .onCase(2, writeLock.to(respondStarted).to(steps).to(loopSteps
      .onDone(clearLock)
      .onEachBatch(runStep.to(stepDone).to(nextBatch(loopSteps)))))
    .onCase(3, unlock.to(respondUnlock)))
  .connect(runStep, 1, stepFailed, 0)
  .add(stepFailed)
  .to(sendError)
  .to(clearLock);
