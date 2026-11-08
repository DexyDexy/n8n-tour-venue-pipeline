// TourVenues_12_new_venue_filter — исходник для n8n Workflow SDK.
// Схема: таблица 02_OUT_extractedData (баннеры TourVenues_11 + то, что клиент вписал руками) — строки НЕ удаляются (с v004),
//        итог пишется в столбец «Automation Notes»:
//        «skipped as invalid <дата>: …» — брак (нет NAME / COUNTRY, TBA / TBC);
//        «skipped as duplicate <дата>: …» — дубль строки выше в 02, уже есть в 03 или точно есть в MASTER BASE;
//        «added to 03_OUT_newData <дата>» — новая площадка (или «ok» человека в «Human Notes») дописана в 03;
//        сомнительные — причина по-английски, такие строки проверяются заново каждым запуском.
//        Строки с итоговой пометкой («skipped as …», «added to 03_OUT_newData …») следующие запуски пропускают;
//        «ok» в «Human Notes» снимает пометку «skipped …» — строка уходит в 03. → отчёт в Telegram.
import { workflow, node, trigger, ifElse, expr } from '@n8n/workflow-sdk';

const EXTRACTED_ID = '<SHEET_EXTRACTED_ID>'; // 02_OUT_extractedData
const BASE_ID = '<SHEET_MASTER_DB_ID>'; // MASTER BASE
const NEWDATA_ID = '<SHEET_NEW_VENUES_ID>'; // 03_OUT_newData
const SHEET1 = { __rl: true, mode: 'list', value: 'gid=0', cachedResultName: 'Sheet1' };
const EXT_SHEET = SHEET1; // вкладка 02 (для тестов сборка подменяет на временную)
const NEW_SHEET = SHEET1; // вкладка 03
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

// Сравнение площадок. NAME — без регистра, диакритики, артикля и знаков (A_Way = A. Way, MKNŽ = MKNZ).
// COUNTRY — в базе коды записаны по-разному (SL — Словения, FIN, ROM…), приводим к одному.
// CITY — «точно» (cityLevel = 2): пустой с любым, одинаковые, известные варианты (Warsaw = Warszawa),
//        один содержит другой (Frankfurt = Frankfurt am Main); «похоже» (1): опечатка в 1–2 буквы
//        (Hossegor / Soorts-Hossego) — такие совпадения только помечаются, а не удаляются.
const MATCH_FN = `
function norm(s) {
  return String(s || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase()
    .replace(/ß/g, 'ss').replace(/&/g, ' and ').replace(/^the\\s+/, '').replace(/[^a-z0-9]+/g, '');
}
const COUNTRY_ALIAS = { GB: 'UK', EN: 'UK', ENG: 'UK', SCO: 'UK', SCT: 'UK', WAL: 'UK', NIR: 'UK', SL: 'SI', SLO: 'SI',
  FIN: 'FI', ROM: 'RO', TUR: 'TR', ISL: 'IS', ICE: 'IS', ARM: 'AM', BIH: 'BA', ALB: 'AL', KOS: 'XK', ISR: 'IL',
  SPAIN: 'ES', GERMANY: 'DE', FRANCE: 'FR', ITALY: 'IT', GREENLAND: 'GL', KYRGYZSTAN: 'KG', SOUTHAFRICA: 'ZA' };
function cc(s) { const c = norm(s).toUpperCase(); return COUNTRY_ALIAS[c] || c; }
const ISO = new Set(('AD AE AF AG AL AM AO AR AT AU AZ BA BB BD BE BF BG BH BI BJ BN BO BR BS BT BW BY BZ CA CD CF CG CH CI CL CM CN CO CR CU CV CY CZ ' +
  'DE DJ DK DM DO DZ EC EE EG ER ES ET FI FJ FM FO FR GA GD GE GH GL GM GN GQ GR GT GW GY HK HN HR HT HU ID IE IL IN IQ IR IS IT JM JO JP ' +
  'KE KG KH KI KM KN KP KR KW KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MG MH MK ML MM MN MO MR MT MU MV MW MX MY MZ NA NE NG NI NL NO NP NR NZ ' +
  'OM PA PE PG PH PK PL PR PS PT PW PY QA RO RS RU RW SA SB SC SD SE SG SI SK SL SM SN SO SR SS ST SV SY SZ TD TG TH TJ TL TM TN TO TR TT TV TW TZ ' +
  'UA UG UK US UY UZ VA VC VE VN VU WS XK YE ZA ZM ZW').split(' '));
const CITY_ALIAS = [
  ['warsaw', 'warszawa'], ['krakow', 'cracow'], ['gent', 'ghent'], ['brussels', 'brussel', 'bruxelles'],
  ['antwerp', 'antwerpen', 'anvers'], ['bruges', 'brugge'], ['leuven', 'louvain'], ['liege', 'luik'],
  ['hague', 'denhaag'], ['shertogenbosch', 'denbosch'], ['lisbon', 'lisboa'], ['porto', 'oporto'], ['seville', 'sevilla'],
  ['prague', 'praha', 'prag'], ['vienna', 'wien'], ['munich', 'munchen'], ['cologne', 'koln'], ['nuremberg', 'nurnberg'],
  ['hanover', 'hannover'], ['milan', 'milano'], ['rome', 'roma'], ['florence', 'firenze'], ['turin', 'torino'],
  ['naples', 'napoli'], ['venice', 'venezia'], ['genoa', 'genova'], ['copenhagen', 'kobenhavn'],
  ['gothenburg', 'goteborg'], ['geneva', 'geneve', 'genf'], ['basel', 'basle', 'bale'], ['zurich', 'zuerich'],
  ['belgrade', 'beograd'], ['bucharest', 'bucuresti'], ['athens', 'athina'], ['kyiv', 'kiev'],
  ['luxembourg', 'luxemburg'], ['dunkirk', 'dunkerque'], ['marseille', 'marseilles'], ['lyon', 'lyons']
];
const CITY_CANON = {};
for (const g of CITY_ALIAS) for (const c of g) CITY_CANON[c] = g[0];
function cityKey(s) { const x = norm(s); return CITY_CANON[x] || x; }
function lev(a, b) {
  if (Math.abs(a.length - b.length) > 2) return 9;
  let prev = Array.from({ length: b.length + 1 }, (x, j) => j);
  for (let i = 1; i <= a.length; i++) {
    const cur = [i];
    for (let j = 1; j <= b.length; j++) cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    prev = cur;
  }
  return prev[b.length];
}
function words(s) {
  return String(s || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().split(/[^a-z0-9]+/).filter(w => w.length >= 4);
}
// 2 — точно тот же город, 1 — похоже (опечатка), 0 — другой
function cityLevel(a, b) {
  const x = cityKey(a), y = cityKey(b);
  if (!x || !y || x === y) return 2;
  if (Math.min(x.length, y.length) >= 4 && (x.includes(y) || y.includes(x))) return 2;
  if (Math.min(x.length, y.length) >= 6 && lev(x, y) <= 2) return 1;
  for (const p of words(a)) for (const q of words(b)) {
    const pc = CITY_CANON[p] || p, qc = CITY_CANON[q] || q;
    if (pc === qc || (Math.min(pc.length, qc.length) >= 6 && lev(pc, qc) <= 1)) return 1;
  }
  return 0;
}
// Похожие (но не одинаковые) названия: одно содержит другое или опечатка
function nameSimilar(a, b) {
  if (!a || !b || a === b) return false;
  const m = Math.min(a.length, b.length);
  return (m >= 4 && (a.includes(b) || b.includes(a))) || (m >= 6 && lev(a, b) <= 2);
}
// В базе в одной ячейке бывает несколько вариантов: «Kult41 - Kult 41», «Gowiththeflow > KulturA.»
function variants(name) {
  return String(name || '').split(/\\s+[-–—|>\\/]\\s+|\\s+aka\\s+/i).map(v => v.trim()).filter(Boolean);
}
// «Ядро» названия: без артикля в начале, без слов-типов (bar, club, salle…) и без названия своего города.
// Le Truskel = Truskel, Molotow Club = Molotow, Fuzz Cartaxo (Cartaxo) = Fuzz, Les Vivres de l'Art = Vivres de L'Art
const ARTICLES = new Set(['the', 'le', 'la', 'les', 'l', 'el', 'los', 'las', 'il', 'lo', 'de', 'het', 'die', 'der', 'das', 'den']);
const TYPES = new Set(['bar', 'club', 'klub', 'cafe', 'pub', 'sala', 'salle', 'festival', 'official', 'officiel']);
function core(name, city) {
  const t = String(name || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().replace(/ß/g, 'ss')
    .replace(/&/g, ' and ').split(/[^a-z0-9]+/).filter(Boolean);
  const cw = new Set(words(city).concat(words(CITY_CANON[norm(city)] || '')));
  while (t.length > 1 && ARTICLES.has(t[0])) t.shift();
  const k = t.filter(w => !TYPES.has(w) && !cw.has(w)).join('');
  return k.length >= 3 ? k : norm(name);
}
function nameKeys(name, city) { return [...new Set(variants(name).map(v => core(v, city)).filter(Boolean))]; }
// Индекс «страна|ядро названия» → список городов
function makeIndex() {
  const m = new Map(), rows = new Map();
  return {
    add(r) {
      for (const n of nameKeys(r.NAME, r.CITY)) {
        const k = cc(r.COUNTRY) + '|' + n;
        if (!m.has(k)) { m.set(k, []); rows.set(k, []); }
        m.get(k).push(String(r.CITY || '').trim());
        rows.get(k).push(r);
      }
    },
    cities(r) { return [].concat(...nameKeys(r.NAME, r.CITY).map(n => m.get(cc(r.COUNTRY) + '|' + n) || [])); },
    has(r) { return this.cities(r).some(c => cityLevel(c, r.CITY) === 2); },
    // строка, с которой совпало (для пометки «… (CH | Bern | Spinnerei)»)
    find(r) {
      for (const n of nameKeys(r.NAME, r.CITY)) {
        const hit = (rows.get(cc(r.COUNTRY) + '|' + n) || []).find(e => cityLevel(e.CITY, r.CITY) === 2);
        if (hit) return hit;
      }
      return null;
    }
  };
}
`;

// ── Узлы ────────────────────────────────────────────────────────────────

const start = trigger({ type: 'n8n-nodes-base.manualTrigger', version: 1, config: { name: 'Start' } });

// Запуск из Telegram-бота: TourVenues_09_worker_gateway передаёт { dryRun, chatId, requestedBy }.
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
          // true — ничего не пишет (ни в 03, ни пометок в 02), только присылает отчёт «что было бы»; по умолчанию false
          { id: 's-dry', name: 'dryRun', value: expr('{{ $json.dryRun === true }}'), type: 'boolean' },
          // отчёт всегда владельцу; запустил кто-то другой из бота — ещё и ему (requestChatId)
          { id: 's-chat', name: 'telegramChatId', value: '<OWNER_TELEGRAM_CHAT_ID>', type: 'string' },
          { id: 's-req', name: 'requestChatId', value: expr("{{ $json.chatId ? String($json.chatId) : '' }}"), type: 'string' },
          { id: 's-by', name: 'requestedBy', value: expr("{{ $json.requestedBy || '' }}"), type: 'string' },
          // дата в пометках «added to 03_OUT_newData <дата>» (часовой пояс Праги)
          { id: 's-date', name: 'today', value: expr("{{ $now.setZone('Europe/Prague').toFormat('yyyy-MM-dd') }}"), type: 'string' }
        ]
      }
    }
  }
});

const readExtracted = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read Extracted',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: EXTRACTED_ID },
      sheetName: EXT_SHEET,
      options: {}
    },
    credentials: GOOGLE
  }
});

const readBase = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read Base',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: BASE_ID },
      sheetName: SHEET1,
      options: {}
    },
    credentials: GOOGLE
  }
});

// То, что уже перенесено раньше и ещё лежит в 03 — второй раз не добавляем
const readNewData = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read New Data',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: NEWDATA_ID },
      sheetName: NEW_SHEET,
      options: {}
    },
    credentials: GOOGLE
  }
});

// Разбор строк 02 без итоговой пометки: брак / дубль / есть в базе / уже в 03 / на проверку / новая.
// Строки с «ok» в Human Notes разбираются первыми — если у них есть копия, дублем помечается копия, а не они.
const classify = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Classify',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: MATCH_FN + `
const s = $('Settings').first().json;
const S = v => String(v == null ? '' : v);
const clean = r => ({ COUNTRY: S(r.COUNTRY).trim(), CITY: S(r.CITY).trim().replace(/\\s+/g, ' '), NAME: S(r.NAME).trim().replace(/\\s+/g, ' ') });
const fmt = r => [r.COUNTRY, r.CITY || '-', r.NAME].join(' | ');
const ext = $('Read Extracted').all().map(i => i.json).filter(r => r.row_number);

// База: точные совпадения, площадки по стране (для похожих названий), в каких странах встречается город
const base = makeIndex();
const baseByName = new Map(), baseByCountry = new Map(), cityCountries = new Map();
for (const raw of $('Read Base').all().map(i => i.json)) {
  if (!raw.NAME) continue;
  const b = clean(raw), c = cc(b.COUNTRY), k = cityKey(b.CITY);
  base.add(b);
  for (const n of nameKeys(b.NAME, b.CITY)) {
    const e = { COUNTRY: c, CITY: b.CITY, NAME: b.NAME, n };
    if (!baseByName.has(n)) baseByName.set(n, []);
    baseByName.get(n).push(e);
    if (!baseByCountry.has(c)) baseByCountry.set(c, []);
    baseByCountry.get(c).push(e);
  }
  if (k) {
    if (!cityCountries.has(k)) cityCountries.set(k, new Map());
    const m = cityCountries.get(k);
    m.set(c, (m.get(c) || 0) + 1);
  }
}
const already = makeIndex();
for (const r of $('Read New Data').all().map(i => i.json)) if (r.NAME) already.add(clean(r));

// заглушка вместо значения: TBA / TBC / TBD / n/a / «-» / «?»
const PLACEHOLDER = /^(n\\/?a|none|unknown|[-–—?.]+)$|\\b(tba|tbc|tbd|tbn)\\b/i;
const isOk = r => /^\\s*ok\\b/i.test(S(r['Human Notes']));
// Итоговые пометки прошлых запусков: такие строки больше не разбираются (кроме «ok» человека у «skipped …»)
const FINAL = /^\\s*(added to 03_OUT_newData|skipped as )/i;
const ADDED = /^\\s*added to 03_OUT_newData/i;
const todo = ext.filter(r => !FINAL.test(S(r['Automation Notes'])) || (isOk(r) && !ADDED.test(S(r['Automation Notes']))));
const order = todo.filter(isOk).concat(todo.filter(r => !isOk(r)));

// Почему строка сомнительная (пустой список — не сомнительная)
function doubts(r, seen) {
  const out = [], c = cc(r.COUNTRY), keys = nameKeys(r.NAME, r.CITY);
  // без города в 03 не пускаем (если площадка точно есть в базе — строка уже удалена выше)
  if (!r.CITY) out.push('CITY is empty');
  if (!ISO.has(c)) out.push('Unknown country code "' + r.COUNTRY + '"');
  // та же площадка в той же стране, но город другой или похожий
  const other = base.cities(r);
  if (other.length) {
    const near = other.filter(x => cityLevel(x, r.CITY) === 1);
    const list = [...new Set(near.length ? near : other)].slice(0, 3).join(', ');
    out.push(near.length ? 'Same NAME in base with similar CITY: ' + list : 'Same NAME in base in another CITY: ' + list);
  } else {
    // та же площадка в том же городе, но страна другая — обычно ошибка распознавания
    const cross = [].concat(...keys.map(n => baseByName.get(n) || [])).filter(b => b.COUNTRY !== c && r.CITY && b.CITY && cityLevel(b.CITY, r.CITY) === 2);
    if (cross.length) out.push('In base with another COUNTRY: ' + fmt(cross[0]));
  }
  // похожее название в том же городе
  if (r.CITY) {
    const sim = (baseByCountry.get(c) || []).filter(b => b.CITY && keys.some(n => nameSimilar(b.n, n)) && cityLevel(b.CITY, r.CITY) === 2);
    const uniq = [...new Map(sim.map(b => [b.NAME, b])).values()].slice(0, 2);
    if (uniq.length) out.push('Similar NAME in base: ' + uniq.map(fmt).join('; '));
  }
  // город в базе встречается только в других странах
  const cityC = r.CITY ? cityCountries.get(cityKey(r.CITY)) : null;
  if (cityC && !cityC.has(c)) {
    const total = [...cityC.values()].reduce((a, x) => a + x, 0);
    if (total >= 2) out.push('CITY "' + r.CITY + '" is listed under ' + [...cityC.keys()].slice(0, 3).join('/') + ' in base');
  }
  // в NAME стоит город (перепутаны столбцы)
  const asCity = cityCountries.get(cityKey(r.NAME));
  if (asCity && (asCity.get(c) || 0) >= 2) out.push('NAME looks like a city - columns mixed up?');
  // похожая строка выше в этой же таблице
  const twin = seen.find(e => {
    if (cc(e.COUNTRY) !== c) return false;
    const ek = nameKeys(e.NAME, e.CITY);
    return (ek.some(x => keys.includes(x)) && cityLevel(e.CITY, r.CITY) === 1) ||
      (r.CITY && e.CITY && ek.some(x => keys.some(n => nameSimilar(x, n))) && cityLevel(e.CITY, r.CITY) === 2);
  });
  if (twin) out.push('Possible duplicate of: ' + fmt(twin));
  return out;
}

const res = { bad: [], dup: [], inBase: [], inNew: [], review: [], moved: [], movedOk: 0 };
const D = s.today;
const notes = [];
const setNote = (raw, note) => { if (note !== S(raw['Automation Notes']).trim()) notes.push({ row_number: raw.row_number, note }); };
const kept = makeIndex(), seen = [];
for (const raw of order) {
  const r = clean(raw), ok = isOk(raw);
  const row = Object.assign({ row: raw.row_number }, r);
  const bad = !r.NAME ? 'no NAME' : !r.COUNTRY ? 'no COUNTRY'
    : [['NAME', r.NAME], ['COUNTRY', r.COUNTRY], ['CITY', r.CITY]].filter(x => PLACEHOLDER.test(x[1])).map(x => x[0] + ' is "' + x[1] + '"').join(', ');
  if (bad) { res.bad.push(Object.assign({ note: bad }, row)); setNote(raw, 'skipped as invalid ' + D + ': ' + bad); continue; }
  const twin = kept.find(r);
  if (twin) { res.dup.push(row); setNote(raw, 'skipped as duplicate ' + D + ': same as row ' + twin.row + ' (' + fmt(twin) + ')'); continue; }
  kept.add(row);
  if (already.has(r)) { res.inNew.push(row); seen.push(r); setNote(raw, 'skipped as duplicate ' + D + ': already in 03_OUT_newData'); continue; }
  const b = ok ? null : base.find(r);
  if (b) { res.inBase.push(row); seen.push(r); setNote(raw, 'skipped as duplicate ' + D + ': in MASTER BASE (' + fmt(b) + ')'); continue; }
  const why = ok ? [] : doubts(r, seen);
  seen.push(r);
  if (why.length) {
    const note = why.join('; ').slice(0, 500);
    res.review.push(Object.assign({ note }, row));
    setNote(raw, note);
    continue;
  }
  if (ok) res.movedOk++;
  res.moved.push(Object.assign(row, { COUNTRY: r.COUNTRY.toUpperCase() }));
  setNote(raw, 'added to 03_OUT_newData ' + D + (ok ? ' (ok by human)' : ''));
}
// Проверка перед записью: строки, которым пишем пометку, должны остаться на своих местах
const byRow = new Map(ext.map(r => [r.row_number, r]));
const snapshot = notes.map(n => { const r = byRow.get(n.row_number);
  return { row: n.row_number, COUNTRY: S(r.COUNTRY), CITY: S(r.CITY), NAME: S(r.NAME), HN: S(r['Human Notes']) }; });
res.moved.sort((a, b) => a.row - b.row);
res.review.sort((a, b) => a.row - b.row);
return [{ json: {
  dryRun: s.dryRun, total: ext.length, checked: todo.length, finalBefore: ext.length - todo.length,
  appendCount: res.moved.length, noteCount: notes.length, writeCount: res.moved.length + notes.length,
  toAppend: res.moved.map(r => ({ COUNTRY: r.COUNTRY, CITY: r.CITY, NAME: r.NAME })),
  notes, snapshot, res
} }];`
    }
  }
});

const canWrite = ifElse({
  version: 2.2,
  config: {
    name: 'Write?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [
          { leftValue: expr('{{ $json.dryRun }}'), operator: { type: 'boolean', operation: 'false', singleValue: true } },
          { leftValue: expr('{{ $json.writeCount }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }
        ],
        combinator: 'and'
      }
    }
  }
});

const hasNew = ifElse({
  version: 2.2,
  config: {
    name: 'Has New?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.appendCount }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
        combinator: 'and'
      }
    }
  }
});

const splitNew = node({
  type: 'n8n-nodes-base.splitOut',
  version: 1,
  config: { name: 'Split New', parameters: { fieldToSplitOut: 'toAppend', include: 'noOtherFields', options: {} } }
});

const appendNew = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Append to New Data',
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'append',
      documentId: { __rl: true, mode: 'id', value: NEWDATA_ID },
      sheetName: NEW_SHEET,
      columns: {
        mappingMode: 'defineBelow',
        value: {
          COUNTRY: expr('{{ $json.COUNTRY }}'),
          CITY: expr('{{ $json.CITY }}'),
          NAME: expr('{{ $json.NAME }}')
        },
        schema: [
          { id: 'COUNTRY', displayName: 'COUNTRY', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'CITY', displayName: 'CITY', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'NAME', displayName: 'NAME', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true }
        ]
      },
      options: { cellFormat: 'RAW', useAppend: true }
    },
    credentials: GOOGLE
  }
});

// Перед записью читаем 02 ещё раз: если строки сдвинулись или поменялись (кто-то сортировал, удалял, правил
// во время работы), ничего не пишем — следующий запуск разберёт заново
const rereadExtracted = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Re-read Extracted',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: EXTRACTED_ID },
      sheetName: EXT_SHEET,
      options: {}
    },
    credentials: GOOGLE
  }
});

const check02 = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Check 02',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const c = $('Classify').first().json;
const S = v => String(v == null ? '' : v);
const fresh = new Map();
for (const r of $input.all().map(i => i.json)) if (r.row_number) fresh.set(r.row_number, r);
const changed = c.snapshot.filter(o => {
  const f = fresh.get(o.row);
  return !f || S(f.COUNTRY) !== o.COUNTRY || S(f.CITY) !== o.CITY || S(f.NAME) !== o.NAME || S(f['Human Notes']) !== o.HN;
});
return [{ json: Object.assign({}, c, { ok: !changed.length,
  error: changed.length ? 'таблица 02 изменилась во время работы (' + changed.length + ' стр.), ничего не записано' : '' }) }];`
    }
  }
});

const isUnchanged = ifElse({
  version: 2.2,
  config: {
    name: '02 Unchanged?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.ok }}'), operator: { type: 'boolean', operation: 'true', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const noteItems = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Note Items',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const n = $('Classify').first().json.notes;
return n.length ? n.map(x => ({ json: x })) : [{ json: { row_number: 0 } }];`
    }
  }
});

const hasNotes = ifElse({
  version: 2.2,
  config: {
    name: 'Has Notes?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.row_number }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
        combinator: 'and'
      }
    }
  }
});

// Пометки пишутся после записи в 03: «added to 03_OUT_newData» появляется, только когда строка уже там
const updateNotes = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Write Notes',
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'update',
      documentId: { __rl: true, mode: 'id', value: EXTRACTED_ID },
      sheetName: EXT_SHEET,
      columns: {
        mappingMode: 'defineBelow',
        value: {
          row_number: expr('{{ $json.row_number }}'),
          'Automation Notes': expr('{{ $json.note }}')
        },
        matchingColumns: ['row_number'],
        schema: [
          { id: 'Automation Notes', displayName: 'Automation Notes', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'row_number', displayName: 'row_number', required: false, defaultMatch: false, display: true, type: 'number', canBeUsedToMatch: true, readOnly: true, removed: false }
        ]
      },
      options: { cellFormat: 'RAW' }
    },
    credentials: GOOGLE
  }
});

const report = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Report',
    executeOnce: true,
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const c = $('Classify').first().json;
const r = c.res;
const chk = $('Check 02').isExecuted ? $('Check 02').first().json : null;
const esc = t => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const fmt = x => esc(x.COUNTRY + ' | ' + (x.CITY || '-') + ' | ' + x.NAME);
const dry = c.dryRun;
const done = !dry && chk && chk.ok;
const lines = ['<b>Tour Venues: проверка по MASTER BASE</b>' + (dry ? ' (тестовый режим, без записи)' : '')];
lines.push('Строк в 02_OUT_extractedData: ' + c.total + (c.finalBefore ? ' (уже с итоговой пометкой, пропущены: ' + c.finalBefore + ')' : ''));
if (!c.checked) lines.push('Новых строк для проверки нет.');
else {
  lines.push('Проверено строк: ' + c.checked);
  lines.push((dry ? 'Было бы добавлено' : 'Добавлено') + ' в 03_OUT_newData: ' + r.moved.length + (r.movedOk ? ' (из них по «ok»: ' + r.movedOk + ')' : ''));
  lines.push('skipped as duplicate: ' + (r.inBase.length + r.dup.length + r.inNew.length) + ' (есть в базе: ' + r.inBase.length + ', дубль в 02: ' + r.dup.length + ', уже в 03: ' + r.inNew.length + ')');
  lines.push('skipped as invalid (нет NAME / COUNTRY, TBA / TBC): ' + r.bad.length);
  lines.push('<b>На проверку: ' + r.review.length + '</b>' + (r.review.length ? ' — причина в Automation Notes, «ok» в Human Notes = добавить' : ''));
  if (chk && !chk.ok) lines.push('⚠️ ' + esc(chk.error) + '. Запустите ещё раз.');
  if (r.review.length) {
    lines.push('');
    for (const x of r.review.slice(0, 15)) lines.push('• ' + fmt(x) + ' — <i>' + esc(x.note) + '</i>');
    if (r.review.length > 15) lines.push('… и ещё ' + (r.review.length - 15));
  }
}
return [{ json: { text: lines.join('\\n'), total: c.total, checked: c.checked, finalBefore: c.finalBefore, added: r.moved.length,
  inBase: r.inBase.length, dup: r.dup.length, inNew: r.inNew.length, bad: r.bad.length, review: r.review.length,
  notesWritten: done ? c.noteCount : 0, error: chk && !chk.ok ? chk.error : '', res: r } }];`
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
      text: expr('{{ $json.text.length > 4000 ? $json.text.slice(0, 4000) + "\\n…" : $json.text }}'),
      additionalFields: { appendAttribution: false, parse_mode: 'HTML' }
    },
    credentials: TELEGRAM
  }
});

// ── Связи ───────────────────────────────────────────────────────────────

export default workflow('tourvenues-12', 'TourVenues_12_new_venue_filter_v005')
  .add(start)
  .to(settings)
  .add(fromBot)
  .to(settings)
  .to(readExtracted)
  .to(readBase)
  .to(readNewData)
  .to(classify)
  .to(canWrite
    .onTrue(rereadExtracted)
    .onFalse(report))
  .add(rereadExtracted)
  .to(check02)
  .to(isUnchanged
    .onTrue(hasNew
      .onTrue(splitNew.to(appendNew).to(noteItems))
      .onFalse(noteItems))
    .onFalse(report))
  .add(noteItems)
  .to(hasNotes
    .onTrue(updateNotes.to(report))
    .onFalse(report))
  .add(report)
  .to(recipients)
  .to(telegram);
