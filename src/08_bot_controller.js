// TourVenues_08_bot_controller — исходник для n8n Workflow SDK. Живёт на VPS (n8n в Docker, MCP n8n-vps), включён (published).
// Telegram-бот TourVenues_Robot как пульт управления: меню из кнопок → подтверждение → запуск рабочих workflow на ПК.
// Telegram присылает сообщения сюда сразу (Telegram Trigger, адрес <VPS_DOMAIN>/webhook/… открыт мимо Cloudflare Access).
// Кто может пользоваться — таблица данных tourvenues_bot_users на VPS (tg_id, name, role: owner | client).
//   Незнакомец пишет /start → получает свой ID, владельцу приходит запрос с кнопками «Дать доступ» / «Отклонить».
//   client: запуск 11 / 12 / 10 / всё по порядку, состояние. owner: то же + тестовый режим (без записи), /unlock.
// Под полем ввода — постоянные кнопки «📋 Меню» и «📊 Состояние» (приходят по /start и при выдаче доступа).
// Запуск и состояние — запрос на ПК через Tailscale: TourVenues_09_worker_gateway (POST /webhook/tourvenues-gateway,
// заголовок X-TourVenues-Key, credential «TourVenues Gateway Key»). ПК не ответил — «рабочий компьютер недоступен».
// Отчёт о результате присылает сам рабочий workflow на ПК (запустившему и копию владельцу).
import { workflow, node, trigger, switchCase, expr } from '@n8n/workflow-sdk';

const TELEGRAM = { telegramApi: { id: 'REPLACE_ME', name: 'TourVenues_Robot' } };
const GATEWAY_KEY = { httpHeaderAuth: { id: 'REPLACE_ME', name: 'TourVenues Gateway Key' } };
const USERS_TABLE = { __rl: true, mode: 'id', value: 'gFRRPANUu7SC5X8X', cachedResultName: 'tourvenues_bot_users' };
const GATEWAY_URL = 'http://<WORKER_HOST>:5678/webhook/tourvenues-gateway'; // ПК 7950white в Tailscale
const OWNER_CHAT = '<OWNER_TELEGRAM_CHAT_ID>';

// Общие для Decide и After PC: шаги конвейера и экранирование для HTML-разметки Telegram
const COMMON_FN = `const OWNER = '${OWNER_CHAT}';
const esc = t => String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const JOBS = {
  '11': { title: '🖼 Баннеры', about: 'Читает новые баннеры из папки 01_OUT_Banners и дописывает концерты в 02_OUT_extractedData. Обработанные файлы переносятся в 02_OUT_ProcessedBanners.' },
  '12': { title: '🔎 Сверка с базой', about: 'Сверяет новые строки 02_OUT_extractedData с MASTER BASE. Новые площадки уходят в 03_OUT_newData, сомнительные остаются в 02 с пометкой для человека.' },
  '10': { title: '📇 Поиск контактов', about: 'Ищет e-mail, сайт и соцсети для всех новых строк 03_OUT_newData и дописывает их в MASTER BASE. Тратит кредиты Tavily; кончатся — прогон остановится сам, следующий продолжит с той же строки. Около минуты на строку.' },
  all: { title: '▶️ Всё по порядку', about: 'Баннеры → сверка с базой → поиск контактов, шаг за шагом. Отчёт приходит после каждого шага.' }
};
`;

// ── Узлы ────────────────────────────────────────────────────────────────

const telegramTrigger = trigger({
  type: 'n8n-nodes-base.telegramTrigger',
  version: 1.5,
  config: {
    name: 'Telegram',
    parameters: { updates: ['message', 'callback_query'], additionalFields: {} },
    credentials: TELEGRAM
  }
});

const findUser = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Find User',
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      resource: 'row',
      operation: 'get',
      dataTableId: USERS_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'tg_id', condition: 'eq', keyValue: expr("{{ String((($json.callback_query || $json.message || {}).from || {}).id || '-') }}") }] },
      returnAll: true
    }
  }
});

// Каждый выходной элемент — одно действие: route = text | menu | confirm_owner | confirm_client | grant_ask | answer | close | gateway | grant | keyboard
const decide = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Decide',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: COMMON_FN + `
const u = $('Telegram').first().json;
const cb = u.callback_query;
const msg = cb ? cb.message : u.message;
if (!msg || !msg.chat) return [];
const from = (cb || u.message).from || {};
const chatId = String(msg.chat.id);
const userId = String(from.id);
const tgName = [from.first_name, from.last_name].filter(Boolean).join(' ') || from.username || userId;
const row = $('Find User').all().map(i => i.json).find(r => String(r.tg_id) === userId);
const role = row ? String(row.role || '') : '';
const name = row && row.name ? String(row.name) : tgName;
const out = [];
const add = (route, extra) => out.push({ json: Object.assign({ route, chatId }, extra) });
const text = t => add('text', { text: t });
const menu = () => add('menu', { text: '<b>Tour Venues — пульт управления</b>\\nВыберите действие. Отчёт о каждом прогоне придёт сюда.' +
  (role === 'owner' ? '\\n\\nКоманды: /status — состояние, /unlock — снять зависшую блокировку, /id — ваш ID.' : '') });
const gateway = (payload, extra) => add('gateway', Object.assign({ payload, role, name }, extra));

// Незнакомец: сообщаем ID, по /start — запрос владельцу
if (!role) {
  if (cb) { add('answer', { callbackId: cb.id, text: 'Нет доступа' }); return out; }
  const isStart = /^\\/start\\b/.test(String(msg.text || ''));
  text('Здравствуйте! Это бот Tour Venues.\\nДоступа у вас пока нет. Ваш Telegram ID: <code>' + userId + '</code>' +
    (isStart ? '\\nЗапрос отправлен владельцу — когда доступ откроют, придёт сообщение.' : '\\nЧтобы запросить доступ, отправьте /start'));
  if (isStart) add('grant_ask', { chatId: OWNER, newId: userId,
    text: '🔔 Запрос доступа к боту: <b>' + esc(tgName) + '</b>' + (from.username ? ' (@' + esc(from.username) + ')' : '') + ', ID <code>' + userId + '</code>' });
  return out;
}

// Нажатие кнопки
if (cb) {
  const [kind, arg] = String(cb.data || '').split(':');
  const ageMin = (Date.now() / 1000 - (msg.date || 0)) / 60;
  const ack = t => add('answer', { callbackId: cb.id, text: t || '' });
  const close = t => add('close', { messageId: msg.message_id, text: t });
  if ((kind === 'run' || kind === 'dry') && ageMin > 30) {
    ack('Кнопка устарела');
    close('⌛ Кнопка устарела — откройте меню заново.');
    menu();
    return out;
  }
  if (kind === 'job' && JOBS[arg]) {
    ack();
    add(role === 'owner' ? 'confirm_owner' : 'confirm_client', { job: arg, text: '<b>' + JOBS[arg].title + '</b>\\n' + JOBS[arg].about + '\\n\\nЗапустить?' });
    return out;
  }
  if ((kind === 'run' || kind === 'dry') && JOBS[arg]) {
    if (kind === 'dry' && role !== 'owner') { ack('Нет доступа'); return out; }
    ack('Отправляю на рабочий ПК…');
    close('⏳ <b>' + JOBS[arg].title + '</b>' + (kind === 'dry' ? ' (тестовый режим)' : '') + ': отправлено на рабочий ПК…');
    gateway({ action: 'run', job: arg, dryRun: kind === 'dry', chatId, requestedBy: name }, { jobTitle: JOBS[arg].title });
    return out;
  }
  if (kind === 'st') { ack('Спрашиваю ПК…'); gateway({ action: 'status' }); return out; }
  if (kind === 'cancel') { ack(); close('Отменено.'); return out; }
  if (kind === 'menu') { ack(); menu(); return out; }
  if ((kind === 'grant' || kind === 'deny') && role === 'owner' && /^\\d+$/.test(arg || '')) {
    // имя нового человека — из текста запроса («… к боту: Имя (@ник), ID 123»)
    const m = String(msg.text || '').match(/к боту: (.*?)(?: \\(@[^)]*\\))?, ID \\d+/);
    const newName = m ? m[1] : arg;
    ack();
    if (kind === 'grant') {
      close('✅ Доступ выдан: <b>' + esc(newName) + '</b> (ID <code>' + arg + '</code>, роль client).\\nУбрать доступ — удалить строку в таблице tourvenues_bot_users на VPS.');
      add('grant', { newId: arg, newName });
    } else close('✖️ Запрос отклонён: ' + esc(newName) + ' (ID <code>' + arg + '</code>).');
    return out;
  }
  ack('Неизвестная кнопка');
  return out;
}

// Текстовое сообщение (кнопки под полем ввода присылают свой текст: «📋 Меню», «📊 Состояние»)
const t = String(msg.text || '').trim();
if (/^\\/status\\b/.test(t) || t === '📊 Состояние') gateway({ action: 'status' });
else if (/^\\/unlock\\b/.test(t) && role === 'owner') gateway({ action: 'unlock' });
else if (/^\\/id\\b/.test(t)) text('Ваш Telegram ID: <code>' + userId + '</code>');
else {
  if (/^\\/start\\b/.test(t)) add('keyboard', { text: 'Кнопки «📋 Меню» и «📊 Состояние» — внизу экрана, под полем ввода.' });
  menu();
}
return out;`
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
          { outputKey: 'text', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'text', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'menu', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'menu', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'confirm_owner', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'confirm_owner', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'confirm_client', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'confirm_client', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'grant_ask', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'grant_ask', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'answer', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'answer', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'close', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'close', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'gateway', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'gateway', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'grant', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'grant', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'keyboard', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.route }}'), rightValue: 'keyboard', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } }
        ]
      },
      options: {}
    }
  }
});

const HTML = { appendAttribution: false, parse_mode: 'HTML', disable_web_page_preview: true };

const sendText = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Text',
    parameters: { resource: 'message', operation: 'sendMessage', chatId: expr('{{ $json.chatId }}'), text: expr('{{ $json.text }}'), additionalFields: HTML },
    credentials: TELEGRAM
  }
});

const sendMenu = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Menu',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'inlineKeyboard',
      inlineKeyboard: {
        rows: [
          { row: { buttons: [
            { text: '🖼 Баннеры', additionalFields: { callback_data: 'job:11' } },
            { text: '🔎 Сверка с базой', additionalFields: { callback_data: 'job:12' } }
          ] } },
          { row: { buttons: [
            { text: '📇 Поиск контактов', additionalFields: { callback_data: 'job:10' } },
            { text: '▶️ Всё по порядку', additionalFields: { callback_data: 'job:all' } }
          ] } },
          { row: { buttons: [
            { text: '📊 Состояние', additionalFields: { callback_data: 'st' } }
          ] } }
        ]
      },
      additionalFields: HTML
    },
    credentials: TELEGRAM
  }
});

const sendConfirmOwner = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Confirm (owner)',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'inlineKeyboard',
      inlineKeyboard: {
        rows: [
          { row: { buttons: [
            { text: '✅ Запустить', additionalFields: { callback_data: expr("{{ 'run:' + $json.job }}") } },
            { text: '🧪 Тест без записи', additionalFields: { callback_data: expr("{{ 'dry:' + $json.job }}") } }
          ] } },
          { row: { buttons: [
            { text: '✖️ Отмена', additionalFields: { callback_data: 'cancel' } }
          ] } }
        ]
      },
      additionalFields: HTML
    },
    credentials: TELEGRAM
  }
});

const sendConfirmClient = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Confirm (client)',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'inlineKeyboard',
      inlineKeyboard: {
        rows: [
          { row: { buttons: [
            { text: '✅ Запустить', additionalFields: { callback_data: expr("{{ 'run:' + $json.job }}") } },
            { text: '✖️ Отмена', additionalFields: { callback_data: 'cancel' } }
          ] } }
        ]
      },
      additionalFields: HTML
    },
    credentials: TELEGRAM
  }
});

const sendGrantAsk = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Grant Ask',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'inlineKeyboard',
      inlineKeyboard: {
        rows: [
          { row: { buttons: [
            { text: '✅ Дать доступ', additionalFields: { callback_data: expr("{{ 'grant:' + $json.newId }}") } },
            { text: '✖️ Отклонить', additionalFields: { callback_data: expr("{{ 'deny:' + $json.newId }}") } }
          ] } }
        ]
      },
      additionalFields: HTML
    },
    credentials: TELEGRAM
  }
});

const answerCallback = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Answer Callback',
    onError: 'continueRegularOutput',
    parameters: {
      resource: 'callback',
      operation: 'answerQuery',
      queryId: expr('{{ $json.callbackId }}'),
      additionalFields: { text: expr('{{ $json.text }}') }
    },
    credentials: TELEGRAM
  }
});

// Текст сообщения с кнопками меняется, кнопки исчезают (повторно нажать нельзя)
const closeMessage = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Close Message',
    onError: 'continueRegularOutput',
    parameters: {
      resource: 'message',
      operation: 'editMessageText',
      messageType: 'message',
      chatId: expr('{{ $json.chatId }}'),
      messageId: expr('{{ $json.messageId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'none',
      additionalFields: { parse_mode: 'HTML', disable_web_page_preview: true }
    },
    credentials: TELEGRAM
  }
});

// Запрос на ПК. Выключен ПК / не запущен n8n — ошибка соединения или таймаут, After PC отвечает «ПК недоступен».
const callPc = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Call PC',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'POST',
      url: GATEWAY_URL,
      authentication: 'genericCredentialType',
      genericAuthType: 'httpHeaderAuth',
      sendBody: true,
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify($json.payload) }}'),
      options: { timeout: expr("{{ $json.payload.action === 'status' ? 60000 : 10000 }}") }
    },
    credentials: GATEWAY_KEY
  }
});

const afterPc = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'After PC',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: COMMON_FN + `
const ctx = $('Decide').all().map(i => i.json).find(x => x.route === 'gateway');
const r = $input.first().json || {};
const p = ctx.payload;
const out = [];
const say = (chatId, text) => out.push({ json: { chatId, text } });
const when = iso => iso ? new Date(iso).toLocaleString('ru-RU', { timeZone: 'Europe/Prague', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : '?';
const lockText = l => l ? '«' + esc(l.jobName) + '», запустил ' + esc(l.by || '?') + ' ' + when(l.since) : '';
const num = v => v == null ? 'не удалось прочитать' : String(v);

if (r.error || typeof r.ok !== 'boolean') {
  say(ctx.chatId, '⏸ <b>Рабочий компьютер сейчас недоступен</b> — выключен или на нём не запущен n8n.\\nПопробуйте позже.');
  if (ctx.role !== 'owner' && p.action === 'run') say(OWNER, '⏸ ' + esc(ctx.name) + ' хотел запустить «' + esc(ctx.jobTitle) + '», но ПК недоступен.');
  return out;
}
if (p.action === 'run') {
  if (r.started) {
    say(ctx.chatId, '▶️ <b>Запущено: ' + ctx.jobTitle + '</b>' + (p.dryRun ? ' (тестовый режим, без записи)' : '') + '\\nОтчёт придёт сюда, когда прогон закончится.');
    if (ctx.role !== 'owner') say(OWNER, '▶️ ' + esc(ctx.name) + ' запустил «' + ctx.jobTitle + '».');
  } else if (r.busy) say(ctx.chatId, '⏳ Сейчас уже идёт прогон: ' + lockText(r.lock) + '.\\nДождитесь отчёта и запустите снова.');
  else say(ctx.chatId, '⚠️ Не удалось запустить: ' + esc(r.error || 'неизвестная ошибка'));
  return out;
}
if (p.action === 'status') {
  const lines = ['📊 <b>Состояние</b>'];
  if (r.busy) lines.push('⏳ Идёт прогон: ' + lockText(r.lock));
  else lines.push('✅ Сейчас ничего не запущено' + (r.lock && r.lock.stale && ctx.role === 'owner' ? ' (есть старая блокировка: ' + lockText(r.lock) + ' — /unlock)' : ''));
  lines.push('');
  lines.push('Баннеров в 01_OUT_Banners: ' + num(r.banners));
  lines.push('Новых строк в 02 (ещё не сверены с базой): ' + num(r.new02));
  lines.push('Сомнительных строк в 02: ' + num(r.review02) + (r.review02 ? ' — похожи на площадку из базы, но не совпадают точно. Причина — в столбце Automation Notes; «ok» в Human Notes = добавить в 03' : ''));
  lines.push('В 03 ждут поиска контактов: ' + num(r.todo03));
  lines.push('LLM Server: ' + (r.ollama ? 'работает' : '⚠️ не отвечает (баннеры и поиск контактов не пойдут)'));
  say(ctx.chatId, lines.join('\\n'));
  return out;
}
if (p.action === 'unlock') {
  say(ctx.chatId, r.unlocked ? '🔓 Блокировка снята: ' + lockText(r.lock) : 'Блокировки не было.');
  return out;
}
return out;`
    }
  }
});

const addUser = node({
  type: 'n8n-nodes-base.dataTable',
  version: 1.1,
  config: {
    name: 'Add User',
    parameters: {
      resource: 'row',
      operation: 'upsert',
      dataTableId: USERS_TABLE,
      matchType: 'allConditions',
      filters: { conditions: [{ keyName: 'tg_id', condition: 'eq', keyValue: expr('{{ $json.newId }}') }] },
      columns: {
        mappingMode: 'defineBelow',
        value: { tg_id: expr('{{ $json.newId }}'), name: expr('{{ $json.newName }}'), role: 'client' },
        matchingColumns: [],
        schema: [
          { id: 'tg_id', displayName: 'tg_id', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'name', displayName: 'name', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'role', displayName: 'role', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true }
        ],
        attemptToConvertTypes: false,
        convertFieldsToString: false
      },
      options: {}
    }
  }
});

const granted = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Granted',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `return $input.all().map(i => ({ json: { chatId: String(i.json.tg_id),
  text: '✅ Доступ к боту Tour Venues открыт.\\nКнопки «📋 Меню» и «📊 Состояние» — внизу экрана, под полем ввода. Отчёт о каждом прогоне придёт сюда.' } }));`
    }
  }
});

// Постоянные кнопки под полем ввода (reply keyboard): остаются, пока бот их не уберёт
const sendKeyboard = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Keyboard',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $json.chatId }}'),
      text: expr('{{ $json.text }}'),
      replyMarkup: 'replyKeyboard',
      replyKeyboard: {
        rows: [
          { row: { buttons: [{ text: '📋 Меню' }, { text: '📊 Состояние' }] } }
        ]
      },
      replyKeyboardOptions: { resize_keyboard: true },
      additionalFields: HTML
    },
    credentials: TELEGRAM
  }
});

const sendReply = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Send Reply',
    parameters: { resource: 'message', operation: 'sendMessage', chatId: expr('{{ $json.chatId }}'), text: expr('{{ $json.text }}'), additionalFields: HTML },
    credentials: TELEGRAM
  }
});

// ── Связи ───────────────────────────────────────────────────────────────

export default workflow('tourvenues-08', 'TourVenues_08_bot_controller_v004')
  .add(telegramTrigger)
  .to(findUser)
  .to(decide)
  .to(route
    .onCase(0, sendText)
    .onCase(1, sendMenu)
    .onCase(2, sendConfirmOwner)
    .onCase(3, sendConfirmClient)
    .onCase(4, sendGrantAsk)
    .onCase(5, answerCallback)
    .onCase(6, closeMessage)
    .onCase(7, callPc.to(afterPc).to(sendReply))
    .onCase(8, addUser.to(granted).to(sendKeyboard))
    .onCase(9, sendKeyboard));
