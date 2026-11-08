// TourVenues_11_banner_venue_extractor — исходник для n8n Workflow SDK.
// Схема: папка Drive со скриншотами баннеров туров → по одному: Ollama (vision) достаёт концерты →
//        новые площадки (COUNTRY | CITY | NAME | Link to banner) дописываются в конец таблицы → баннер переносится
//        в 02_OUT_ProcessedBanners/<год>/<месяц>/<число>/ → после всех баннеров отчёт в Telegram.
import { workflow, node, trigger, ifElse, splitInBatches, nextBatch, expr } from '@n8n/workflow-sdk';

const SHEET_ID = '<SHEET_EXTRACTED_ID>'; // 02_OUT_extractedData
const IN_FOLDER = '<DRIVE_FOLDER_INBOX_ID>'; // 01_OUT_Banners
const OUT_FOLDER = '<DRIVE_FOLDER_PROCESSED_ID>'; // 02_OUT_ProcessedBanners
const GOOGLE = { googleApi: { id: 'REPLACE_ME', name: 'Google Sheets (service-account)' } };
const TELEGRAM = { telegramApi: { id: 'REPLACE_ME', name: 'TourVenues_Robot' } };

// Кому отправить отчёт (узел Recipients): владельцу всегда; если запуск из бота сделал другой человек — ещё и ему,
// а в копии владельцу — кто запустил.
const RECIPIENTS_CODE = `const s = $('Settings').first().json;
const esc = t => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const owner = String(s.telegramChatId);
const asker = String(s.requestChatId || '').trim();
const text = $json.text;
const out = [];
if (asker && asker !== owner) out.push({ json: { chatId: asker, text } });
out.push({ json: { chatId: owner, text: (asker && asker !== owner ? '👤 Запустил: ' + esc(s.requestedBy || asker) + '\\n' : '') + text } });
return out;`;

// Ключ для поиска дублей: страна + площадка, без регистра, диакритики, артикля и знаков.
// Город не учитываем: он бывает в разных написаниях (Warsaw / Warszawa, Lisboa / Lisbon).
const KEY_FN = `
function norm(s) {
  return String(s || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase()
    .replace(/ß/g, 'ss').replace(/&/g, ' and ').replace(/^the\\s+/, '').replace(/[^a-z0-9]+/g, '');
}
// «PSYKA FESTIVAL» → «Psyka Festival», «GENT» → «Gent»; у площадок короткие аббревиатуры (RCA, UBU, ZDB) не трогаем
function fixCaps(s, keepAcronym) {
  s = String(s || '').trim().replace(/\\s+/g, ' ');
  if (!/[A-ZÀ-Þ]/.test(s) || s !== s.toUpperCase() || (keepAcronym && /^[A-Z0-9]{1,4}$/.test(s))) return s;
  return s.toLowerCase().replace(/(^|[\\s\\-\\/(.])(\\p{L})/gu, (m, a, b) => a + b.toUpperCase());
}
function venueKey(country, place, venue) {
  return norm(country) + '|' + (norm(venue) || 'city:' + norm(place));
}
`;

// Модель отвечает компактным JSON в тексте. Строгий режим Ollama (format: schema) с qwen3-vl обрывает ответ
// на середине, поэтому JSON достаём из текста сами.
const PROMPT = `The attached image is a screenshot of a concert tour banner (often an Instagram post, sometimes a news article).
Extract EVERY concert/show listed on the banner.
Answer with JSON only, no markdown, one show per line, exactly in this format:
{"band":"<headlining band>","shows":[
["<date as written>","<country code>","<city>","<venue>"],
...
]}
- country code: two letters, uppercase (ISO 3166-1 alpha-2, e.g. DE, PL, NL; for the United Kingdom use UK). If the banner does not show the country, infer it from the city.
- city and venue: as written on the banner but in normal capitalization (not ALL CAPS). Venue "" if not shown.
Rows are often written as "date. city. venue" or "date venue......city, CC" - read each row carefully and do not mix up city and venue.
Ignore the phone/app interface (status bar, likes, comments, usernames, stories, ads). Do not invent shows. If the image is not a tour banner, return {"band":"","shows":[]}.`;

// ── Узлы ────────────────────────────────────────────────────────────────

const start = trigger({ type: 'n8n-nodes-base.manualTrigger', version: 1, config: { name: 'Start' } });

// Запуск из Telegram-бота: TourVenues_09_worker_gateway передаёт { dryRun, chatId, requestedBy } (можно и maxFiles).
// При ручном запуске этих полей нет — действуют значения по умолчанию из Settings.
const fromBot = trigger({ type: 'n8n-nodes-base.executeWorkflowTrigger', version: 1.3, config: { name: 'From Bot', parameters: { inputSource: 'passthrough' } } });

const settings = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Settings',
    parameters: {
      mode: 'manual',
      includeOtherFields: false,
      assignments: {
        assignments: [
          // true — ничего не пишет в таблицу и не переносит файлы (только распознаёт и шлёт отчёт); по умолчанию false
          { id: 's-dry', name: 'dryRun', value: expr('{{ $json.dryRun === true }}'), type: 'boolean' },
          // сколько баннеров обработать за запуск; по умолчанию 100
          { id: 's-max', name: 'maxFiles', value: expr('{{ Number($json.maxFiles) > 0 ? Number($json.maxFiles) : 100 }}'), type: 'number' },
          // модель Ollama с поддержкой картинок
          { id: 's-model', name: 'model', value: 'qwen3-vl:32b-instruct', type: 'string' },
          // отчёт всегда владельцу; запустил кто-то другой из бота — ещё и ему (requestChatId)
          { id: 's-chat', name: 'telegramChatId', value: '<OWNER_TELEGRAM_CHAT_ID>', type: 'string' },
          { id: 's-req', name: 'requestChatId', value: expr("{{ $json.chatId ? String($json.chatId) : '' }}"), type: 'string' },
          { id: 's-by', name: 'requestedBy', value: expr("{{ $json.requestedBy || '' }}"), type: 'string' },
          // папка для обработанных баннеров создаётся по дате запуска (часовой пояс Праги)
          { id: 's-year', name: 'year', value: expr("{{ $now.setZone('Europe/Prague').toFormat('yyyy') }}"), type: 'string' },
          { id: 's-month', name: 'month', value: expr("{{ $now.setZone('Europe/Prague').toFormat('MM') }}"), type: 'string' },
          { id: 's-day', name: 'day', value: expr("{{ $now.setZone('Europe/Prague').toFormat('dd') }}"), type: 'string' }
        ]
      }
    }
  }
});

// Папка года в 02_OUT_ProcessedBanners: найти или создать
const findYear = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Find Year Folder',
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'fileFolder',
      searchMethod: 'query',
      queryString: expr(`{{ "'${OUT_FOLDER}' in parents and name = '" + $('Settings').first().json.year + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false" }}`),
      returnAll: false,
      limit: 1,
      filter: {},
      options: { fields: ['id', 'name'] }
    },
    credentials: GOOGLE
  }
});

const hasYear = ifElse({
  version: 2.2,
  config: {
    name: 'Year Exists?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.id }}'), operator: { type: 'string', operation: 'notEmpty', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const createYear = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Create Year Folder',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'folder',
      operation: 'create',
      name: expr("{{ $('Settings').first().json.year }}"),
      driveId: { __rl: true, mode: 'list', value: 'My Drive' },
      folderId: { __rl: true, mode: 'id', value: OUT_FOLDER },
      options: {}
    },
    credentials: GOOGLE
  }
});

// Папка месяца внутри папки года
const findMonth = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Find Month Folder',
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'fileFolder',
      searchMethod: 'query',
      queryString: expr(`{{ "'" + $json.id + "' in parents and name = '" + $('Settings').first().json.month + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false" }}`),
      returnAll: false,
      limit: 1,
      filter: {},
      options: { fields: ['id', 'name'] }
    },
    credentials: GOOGLE
  }
});

const hasMonth = ifElse({
  version: 2.2,
  config: {
    name: 'Month Exists?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.id }}'), operator: { type: 'string', operation: 'notEmpty', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const createMonth = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Create Month Folder',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'folder',
      operation: 'create',
      name: expr("{{ $('Settings').first().json.month }}"),
      driveId: { __rl: true, mode: 'list', value: 'My Drive' },
      folderId: { __rl: true, mode: 'id', value: expr("{{ $('Find Year Folder').first().json.id || $('Create Year Folder').first().json.id }}") },
      options: {}
    },
    credentials: GOOGLE
  }
});

// Папка числа внутри папки месяца
const findDay = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Find Day Folder',
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'fileFolder',
      searchMethod: 'query',
      queryString: expr(`{{ "'" + $json.id + "' in parents and name = '" + $('Settings').first().json.day + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false" }}`),
      returnAll: false,
      limit: 1,
      filter: {},
      options: { fields: ['id', 'name'] }
    },
    credentials: GOOGLE
  }
});

const hasDay = ifElse({
  version: 2.2,
  config: {
    name: 'Day Exists?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.id }}'), operator: { type: 'string', operation: 'notEmpty', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const createDay = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Create Day Folder',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'folder',
      operation: 'create',
      name: expr("{{ $('Settings').first().json.day }}"),
      driveId: { __rl: true, mode: 'list', value: 'My Drive' },
      folderId: { __rl: true, mode: 'id', value: expr("{{ $('Find Month Folder').first().json.id || $('Create Month Folder').first().json.id }}") },
      options: {}
    },
    credentials: GOOGLE
  }
});

const targetFolder = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Target Folder',
    parameters: {
      mode: 'manual',
      includeOtherFields: false,
      assignments: {
        assignments: [
          { id: 't-id', name: 'targetFolderId', value: expr('{{ $json.id }}'), type: 'string' },
          { id: 't-path', name: 'targetPath', value: expr("{{ '02_OUT_ProcessedBanners/' + $('Settings').first().json.year + '/' + $('Settings').first().json.month + '/' + $('Settings').first().json.day + '/' }}"), type: 'string' }
        ]
      }
    }
  }
});

// Уже записанные площадки — для пропуска дублей (сверяем только с этой таблицей)
const readSheet = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read Sheet',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: SHEET_ID },
      sheetName: { __rl: true, mode: 'list', value: 'gid=0', cachedResultName: 'Sheet1' },
      options: {}
    },
    credentials: GOOGLE
  }
});

const listBanners = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'List Banners',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'fileFolder',
      searchMethod: 'query',
      queryString: `'${IN_FOLDER}' in parents and mimeType contains 'image/' and trashed = false`,
      returnAll: true,
      filter: {},
      options: { fields: ['id', 'name', 'mimeType'] }
    },
    credentials: GOOGLE
  }
});

const selectFiles = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Select Files',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `// Баннеры по имени (скриншоты называются по времени — получается по порядку съёмки)
const s = $('Settings').first().json;
const files = $input.all().map(i => i.json).filter(f => f.id && /^image\\//.test(f.mimeType || ''))
  .sort((a, b) => String(a.name).localeCompare(String(b.name), 'en', { numeric: true }))
  .slice(0, s.maxFiles);
return [{ json: { count: files.length, files } }];`
    }
  }
});

const hasFiles = ifElse({
  version: 2.2,
  config: {
    name: 'Has Banners?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.count }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
        combinator: 'and'
      }
    }
  }
});

const splitFiles = node({
  type: 'n8n-nodes-base.splitOut',
  version: 1,
  config: { name: 'Split Files', parameters: { fieldToSplitOut: 'files', include: 'noOtherFields', options: {} } }
});

const loopBanners = splitInBatches({ version: 3, config: { name: 'Loop Banners', parameters: { batchSize: 1, options: {} } } });

const downloadBanner = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Download Banner',
    onError: 'continueRegularOutput',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'file',
      operation: 'download',
      fileId: { __rl: true, mode: 'id', value: expr('{{ $json.id }}') },
      options: { binaryPropertyName: 'data' }
    },
    credentials: GOOGLE
  }
});

const buildRequest = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Build Request',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const s = $('Settings').first().json;
const file = $('Loop Banners').first().json;
let images = [];
try {
  const buf = await this.helpers.getBinaryDataBuffer(0, 'data');
  images = [buf.toString('base64')];
} catch (e) {}
const body = {
  model: s.model,
  stream: false,
  think: false,
  options: { temperature: 0, num_ctx: 8192 },
  messages: [{ role: 'user', content: ${JSON.stringify(PROMPT)}, images }]
};
return [{ json: { fileId: file.id, fileName: file.name, hasImage: images.length > 0, body } }];`
    }
  }
});

const ollamaExtract = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Ollama Extract',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'POST',
      url: 'http://localhost:11434/api/chat',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify($json.body) }}'),
      options: { timeout: 600000 }
    }
  }
});

const parseShows = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Parse Shows',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: KEY_FN + `
const req = $('Build Request').first().json;
const res = $input.first().json;
const out = { fileId: req.fileId, fileName: req.fileName, band: '', found: 0, rows: [], duplicates: 0, noVenue: 0, status: 'ok', error: '' };
let data = null;
if (!req.hasImage) { out.status = 'error'; out.error = 'download failed'; }
else if (res.error || !res.message) { out.status = 'error'; out.error = 'ollama: ' + JSON.stringify(res.error || res).slice(0, 200); }
else {
  const m = String(res.message.content || '').match(/\\{[\\s\\S]*\\}/);
  try { data = JSON.parse(m[0]); } catch (e) { out.status = 'error'; out.error = 'bad JSON from model: ' + String(res.message.content || '').slice(0, 100); }
}
if (data) {
  out.band = String(data.band || '').trim();
  // Уже известные площадки: таблица + добавленные раньше в этом же запуске
  const seen = new Set();
  for (const r of $('Read Sheet').all().map(i => i.json)) if (r.COUNTRY || r.NAME) seen.add(venueKey(r.COUNTRY, r.CITY, r.NAME));
  for (let k = 0; k < $runIndex; k++) {
    try { for (const it of $('Parse Shows').all(0, k)) for (const r of it.json.rows || []) seen.add(venueKey(r.COUNTRY, r.CITY, r.NAME)); } catch (e) {}
  }
  for (const sh of Array.isArray(data.shows) ? data.shows : []) {
    const v = Array.isArray(sh) ? sh : [sh.date, sh.country, sh.city, sh.venue];
    let country = String(v[1] || '').trim().toUpperCase();
    // Части Великобритании (EN, SCO…) → UK; SL — частая ошибка модели для Словении
    country = { GB: 'UK', EN: 'UK', ENG: 'UK', SCO: 'UK', SCT: 'UK', WAL: 'UK', WLS: 'UK', NIR: 'UK', SL: 'SI' }[country] || country;
    const place = fixCaps(v[2], false);
    const venue = fixCaps(v[3], true);
    if (!place && !venue) continue;
    out.found++;
    // без площадки (или с заглушкой TBA / TBC / «-») строку не вносим
    if (!venue || /^(tba|tbc|tbd|n\\/?a|[-–—?.]+)$/i.test(venue)) { out.noVenue++; continue; }
    const key = venueKey(country, place, venue);
    if (seen.has(key)) { out.duplicates++; continue; }
    seen.add(key);
    out.rows.push({ COUNTRY: country, CITY: place, NAME: venue, 'Link to banner': 'https://drive.google.com/file/d/' + req.fileId + '/view' });
  }
}
return [{ json: out }];`
    }
  }
});

const parsedOk = ifElse({
  version: 2.2,
  config: {
    name: 'Parsed OK?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [
          { leftValue: expr('{{ $json.status }}'), rightValue: 'ok', operator: { type: 'string', operation: 'equals' } },
          { leftValue: expr("{{ $('Settings').first().json.dryRun }}"), operator: { type: 'boolean', operation: 'false', singleValue: true } }
        ],
        combinator: 'and'
      }
    }
  }
});

const hasRows = ifElse({
  version: 2.2,
  config: {
    name: 'Has New Rows?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.rows.length }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
        combinator: 'and'
      }
    }
  }
});

const splitRows = node({
  type: 'n8n-nodes-base.splitOut',
  version: 1,
  config: { name: 'Split Rows', parameters: { fieldToSplitOut: 'rows', include: 'noOtherFields', options: {} } }
});

const appendRows = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Append Rows',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'append',
      documentId: { __rl: true, mode: 'id', value: SHEET_ID },
      sheetName: { __rl: true, mode: 'list', value: 'gid=0', cachedResultName: 'Sheet1' },
      columns: {
        mappingMode: 'defineBelow',
        value: {
          COUNTRY: expr('{{ $json.COUNTRY }}'),
          CITY: expr('{{ $json.CITY }}'),
          NAME: expr('{{ $json.NAME }}'),
          'Link to banner': expr('{{ $json["Link to banner"] }}')
        },
        schema: [
          { id: 'COUNTRY', displayName: 'COUNTRY', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'CITY', displayName: 'CITY', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'NAME', displayName: 'NAME', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'Link to banner', displayName: 'Link to banner', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true }
        ]
      },
      options: { cellFormat: 'RAW', useAppend: true }
    },
    credentials: GOOGLE
  }
});

// Перенос баннера в 02_OUT_ProcessedBanners/<год>/<месяц>/<число>/ (один раз на баннер, после записи строк)
const moveBanner = node({
  type: 'n8n-nodes-base.googleDrive',
  version: 3,
  config: {
    name: 'Move Banner',
    executeOnce: true,
    onError: 'continueRegularOutput',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'file',
      operation: 'move',
      fileId: { __rl: true, mode: 'id', value: expr("{{ $('Parse Shows').first().json.fileId }}") },
      driveId: { __rl: true, mode: 'list', value: 'My Drive' },
      folderId: { __rl: true, mode: 'id', value: expr("{{ $('Target Folder').first().json.targetFolderId }}") }
    },
    credentials: GOOGLE
  }
});

// Итог по баннеру — уходит обратно в цикл, на выходе цикла собираются все
const bannerResult = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Banner Result',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const p = $('Parse Shows').first().json;
const s = $('Settings').first().json;
let moved = false, moveError = '';
if (p.status === 'ok' && !s.dryRun) {
  const m = $input.first().json;
  if (m && m.error) moveError = 'move failed: ' + JSON.stringify(m.error).slice(0, 150);
  else moved = true;
}
return [{ json: {
  fileName: p.fileName, band: p.band, found: p.found, added: s.dryRun ? 0 : p.rows.length,
  wouldAdd: p.rows.length, duplicates: p.duplicates, noVenue: p.noVenue, moved,
  status: moveError ? 'error' : p.status, error: p.error || moveError,
  rows: p.rows
} }];`
    }
  }
});

const report = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Report',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const s = $('Settings').first().json;
const target = $('Target Folder').first().json.targetPath;
const res = $input.all().map(i => i.json).filter(r => r.fileName);
const sum = k => res.reduce((a, r) => a + (r[k] || 0), 0);
const errors = res.filter(r => r.status !== 'ok');
const esc = t => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const lines = [];
lines.push('<b>Tour Venues: баннеры обработаны</b>' + (s.dryRun ? ' (тестовый режим, без записи)' : ''));
if (!res.length) lines.push('Новых баннеров в папке нет.');
else {
  lines.push('Баннеров: ' + res.length + (errors.length ? ', с ошибкой: ' + errors.length : ''));
  lines.push('Концертов найдено: ' + sum('found'));
  lines.push((s.dryRun ? 'Было бы добавлено' : 'Добавлено в таблицу') + ': ' + (s.dryRun ? sum('wouldAdd') : sum('added')));
  lines.push('Дубли пропущены: ' + sum('duplicates'));
  if (sum('noVenue')) lines.push('Без NAME (не внесены): ' + sum('noVenue'));
  if (!s.dryRun) lines.push('Перенесено в ' + esc(target) + ': ' + res.filter(r => r.moved).length);
  for (const e of errors.slice(0, 10)) lines.push('⚠️ ' + esc(e.fileName) + ': ' + esc(e.error));
  if (errors.length) lines.push('Баннеры с ошибкой остались во входящей папке.');
}
return [{ json: { text: lines.join('\\n'), banners: res.length, found: sum('found'), added: sum('added'), duplicates: sum('duplicates'), noVenue: sum('noVenue'), errors: errors.length, details: res } }];`
    }
  }
});

const recipients = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Recipients',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: RECIPIENTS_CODE
    }
  }
});

const telegram = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Telegram',
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

// ── Связи ───────────────────────────────────────────────────────────────

export default workflow('tourvenues-11', 'TourVenues_11_banner_venue_extractor_v004')
  .add(start)
  .to(settings)
  .add(fromBot)
  .to(settings)
  .to(findYear)
  .to(hasYear
    .onTrue(findMonth)
    .onFalse(createYear.to(findMonth)))
  .add(findMonth)
  .to(hasMonth
    .onTrue(findDay)
    .onFalse(createMonth.to(findDay)))
  .add(findDay)
  .to(hasDay
    .onTrue(targetFolder)
    .onFalse(createDay.to(targetFolder)))
  .add(targetFolder)
  .to(readSheet)
  .to(listBanners)
  .to(selectFiles)
  .to(hasFiles
    .onTrue(splitFiles.to(loopBanners
      .onDone(report)
      .onEachBatch(downloadBanner.to(buildRequest).to(ollamaExtract).to(parseShows)
        .to(parsedOk
          .onTrue(hasRows
            .onTrue(splitRows.to(appendRows).to(moveBanner))
            .onFalse(moveBanner))
          .onFalse(bannerResult)))))
    .onFalse(report))
  .add(moveBanner)
  .to(bannerResult)
  .to(nextBatch(loopBanners))
  .add(report)
  .to(recipients)
  .to(telegram);
