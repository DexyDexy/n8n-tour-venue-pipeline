// TourVenues_10_venue_email_finder — исходник для n8n Workflow SDK.
// Схема (с v009 без запроса остатка кредитов Tavily): все необработанные строки 03_OUT_newData (снизу вверх; с v008 без лимита
//        и без резерва кредитов — проход идёт, пока есть строки или пока Tavily не ответит «кредиты кончились») →
//        [поиск по строке циклом (v006, docs/search-algorithm-v006.md): Plan Step решает следующий шаг —
//         короткий запрос «NAME CITY» (с country) → модель проверяет выдачу, исправляет название/город, называет сайт →
//         сайты-кандидаты (бесплатно; не открылся — Tavily extract), принимаются по названию + городу →
//         нет адресов: «… email» → тот же запрос advanced → поиск по исправленному названию → Facebook/Instagram;
//         в конце модель пишет NOTES (описание + жанры), лог поиска — в SCRAPER NOTES → строка дописывается
//         в MASTER BASE, в 03 — отметка в Automation Status (ошибка — заметка в 03, строка остаётся)]
//        → отчёт в Telegram. Кредиты Tavily закончились — проход останавливается, строка остаётся в 03.
import { workflow, node, trigger, ifElse, switchCase, splitInBatches, nextBatch, languageModel, outputParser, expr } from '@n8n/workflow-sdk';

const NEWDATA_ID = '<SHEET_NEW_VENUES_ID>'; // 03_OUT_newData (вход)
const BASE_ID = '<SHEET_MASTER_DB_ID>'; // MASTER BASE (выход)
const SHEET1 = { __rl: true, mode: 'list', value: 'gid=0', cachedResultName: 'Sheet1' };
const NEW_SHEET = SHEET1; // вкладка 03 (для тестов сборка подменяет на временную)
const BASE_SHEET = SHEET1; // вкладка базы
const BASE_TAB = 'Sheet1'; // та же вкладка базы — по имени и номеру (gid) для прямых запросов к Google Sheets API
const BASE_GID = 0;
const GOOGLE = { googleApi: { id: 'REPLACE_ME', name: 'Google Sheets (service-account)' } };
const TAVILY = { httpHeaderAuth: { id: 'REPLACE_ME', name: 'Tavily API brutal robots' } }; // с 2026-10-21 (раньше AGibU4iSqfd9WIcm «Tavily API»)
const TAVILY_URL = 'https://api.tavily.com'; // для теста «кредиты кончились» сборка подменяет на локальную заглушку
const OLLAMA = { ollamaApi: { id: 'REPLACE_ME', name: 'Ollama account' } };
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

// Ошибка Tavily, после которой продолжать бессмысленно: 432/433 — кредиты (лимит тарифа / pay-as-you-go)
// закончились, 401 — неверный ключ, 429 — слишком частые запросы
const FATAL_FN = `
function tavilyFatal(err) {
  if (!err) return '';
  const t = typeof err === 'string' ? err : JSON.stringify(err);
  if (/\\b432\\b|\\b433\\b|usage limit|pay-as-you-go/i.test(t)) return 'credits exhausted';
  if (/\\b401\\b|unauthorized|invalid api key/i.test(t)) return 'API key rejected';
  if (/\\b429\\b|too many requests|excessive requests/i.test(t)) return 'rate limit';
  return '';
}
`;

const BROWSER_HEADERS = {
  parameters: [
    { name: 'User-Agent', value: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36' },
    { name: 'Accept-Language', value: 'en,pl;q=0.8,pt;q=0.7,de;q=0.6' }
  ]
};
const PAGE_OPTIONS = {
  allowUnauthorizedCerts: true,
  timeout: 15000,
  response: { response: { responseFormat: 'text', neverError: true } }
};

// Разбор адресов. В песочнице Code-узла n8n нет глобального URL, поэтому — регулярными выражениями.
const URL_FN = `
function parseUrl(u) {
  const m = String(u || '').trim().match(/^(https?):\\/\\/([^\\/?#:]+)(:\\d+)?([^?#]*)/i);
  if (!m) return null;
  const origin = m[1].toLowerCase() + '://' + m[2].toLowerCase() + (m[3] || '');
  return { origin, hostname: m[2].toLowerCase(), pathname: m[4] || '/' };
}
function host(u) { const p = parseUrl(u); return p ? p.hostname.replace(/^www\\./, '') : ''; }
function resolveUrl(href, base) {
  const h = String(href || '').trim();
  if (/^https?:\\/\\//i.test(h)) return h;
  const b = parseUrl(base);
  if (!b) return '';
  if (h.startsWith('//')) return b.origin.split(':')[0] + ':' + h;
  if (h.startsWith('/')) return b.origin + h;
  if (/^[a-z][a-z0-9+.-]*:/i.test(h)) return '';
  const dir = b.pathname.endsWith('/') ? b.pathname : b.pathname.replace(/[^\\/]*$/, '');
  return b.origin + dir + h.replace(/^\\.\\//, '');
}
`;

// Общая функция поиска e-mail в тексте/HTML (вставляется в несколько Code-узлов)
const EXTRACT_FN = `
// Раскодировать защиту от спама: HTML-коды символов (&#x62;&#105;…) и Cloudflare (data-cfemail / email-protection#)
function cfDecode(hex) {
  const k = parseInt(hex.slice(0, 2), 16);
  let s = '';
  for (let i = 2; i + 1 < hex.length; i += 2) s += String.fromCharCode(parseInt(hex.slice(i, i + 2), 16) ^ k);
  return s;
}
function decodeText(text) {
  return String(text || '')
    .replace(/&#x([0-9a-f]{1,6});/gi, (m, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&#(\\d{1,7});/g, (m, d) => String.fromCharCode(parseInt(d, 10)))
    .replace(/data-cfemail="([0-9a-f]+)"/gi, (m, h) => ' ' + cfDecode(h) + ' ')
    .replace(/email-protection#([0-9a-f]+)/gi, (m, h) => ' ' + cfDecode(h) + ' ');
}
// Адреса, рядом с которыми (до 150 символов перед адресом) написано про букинг/концерты/программу
function bookingHinted(text) {
  const t = decodeText(text).replace(/<[^>]+>/g, ' ');
  const out = [];
  const re = /[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}/gi;
  let m;
  while ((m = re.exec(t))) {
    const before = t.slice(Math.max(0, m.index - 150), m.index);
    if (/(booking|concerts?|koncert|gigs|programa(c|ç)|program(me|ming)?\\b|promot)/i.test(before)) {
      const e = m[0].toLowerCase();
      if (!out.includes(e)) out.push(e);
    }
  }
  return out;
}
function validEmail(e) {
  const [l, d] = String(e).split('@');
  return !!l && !!d && l.length <= 40 && !/[._-]$/.test(l) && !/(^|\\.)-|-(\\.|$)/.test(d);
}
function extractEmails(text) {
  if (!text) return [];
  // %20 / %3A — закодированные пробел и двоеточие в ссылках (mailto%20%3a%20remi@… → remi@…)
  const t = decodeText(text).replace(/%20/g, ' ').replace(/%3a/gi, ':')
    .replace(/&#64;|&#x40;|%40|&commat;|\\\\u0040/gi, '@')
    .replace(/\\s*[\\[\\(\\{]\\s*(at|@)\\s*[\\]\\)\\}]\\s*/gi, '@')
    .replace(/\\s*[\\[\\(\\{]\\s*dot\\s*[\\]\\)\\}]\\s*/gi, '.');
  const out = [];
  for (const m of t.match(/[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}/gi) || []) {
    let e = m.toLowerCase().replace(/^(u003[ce]|x40|mailto:)/, '').replace(/^[._%+-]+/, '').replace(/\\.+$/, '');
    // склеенный текст ссылки и адреса: info@361grad.de361grad.de → info@361grad.de
    e = e.replace(/@((?:[a-z0-9-]+\\.)+[a-z]{2,})\\1$/, '@$1');
    if (/\\.(png|jpe?g|gif|svg|webp|css|js|ico)$/.test(e)) continue;
    // не адрес, а кусок ссылки или заголовка: «…-no-beato-@casa.capitao»
    if (!validEmail(e)) continue;
    if (!out.includes(e)) out.push(e);
  }
  return out;
}
` + URL_FN;

const AGG_FN = `
const AGGREGATORS = ['tripadvisor.','booking.com','google.','yelp.','wikipedia.org','wikidata.org','songkick.com','bandsintown.com',
  'setlist.fm','ra.co','residentadvisor.net','last.fm','eventim.','ticketmaster.','biletyna.pl','ebilet.pl','goingapp.pl','going.pl',
  'kupbilecik.pl','bilety24.pl','timeout.com','foursquare.com','wanderlog.com','lonelyplanet.com','x.com','twitter.com','youtube.com',
  'tiktok.com','spotify.com','linkedin.com','discogs.com','allevents.in','concertful.com','jambase.com','eventbrite.','dice.fm',
  'bol.pt','blueticket.pt','ticketline.sapo.pt','viralagenda.com','agendalx.pt','visitportugal.com','visitlisboa.com','visitporto.travel',
  'warsawtour.pl','krakow.travel','gdansk4u.pl','yelp.pl','zomato.com','restaurantguru.com','mapcarta.com','waze.com','apple.com',
  'reddit.com','pinterest.','threads.net','soundcloud.com','bandcamp.com','mixcloud.com','songkick.','skiddle.com','clubbable.com',
  'livenation.','ticketsea.com','likeit-loveit.com','seated.com','festscanner.com','masqueticket.com','rockbarlegends.com','rock-alliance.eu'];
// v006: каталоги, туризм, билетные сервисы, найденные при исследовании (поддомены тоже: x.mapado.com)
const AGG_RE = /(tripadvisor|booking\\.com|google\\.|yelp|wikipedia|songkick|bandsintown|setlist|ra\\.co|last\\.fm|eventim|ticketmaster|livenation|dice\\.fm|jambase|concertful|allevents|eventbrite|skiddle|mapquest|mindtrip|foodpearl|restaurantguru|youtube|spotify|tiktok|linkedin|mapcarta|wanderlog|foursquare|apple\\.com|waze|bandcamp|soundcloud|discogs|petitfute|tourisme|visit|stayhappening|gigseekr|inyourpocket|evendo|zilesinopti|timeout|gigatools|festivalticker|festivalsindeutschland|festivalfans|eventfinda|kulturkalender|rausgegangen|meetup|deezer|qobuz|metal-archives|reverbnation|shazam|mapado|eventbook|helloasso|billetweb|weezevent|shotgun|linktr\\.ee|rocketreach|leadiq|zoominfo)/;
function isAggregator(h) { return AGG_RE.test(h) || AGGREGATORS.some(a => h === a || h.endsWith('.' + a) || (a.endsWith('.') && (h.startsWith(a) || h.includes('.' + a)))); }
function isFacebook(h) { return /(^|\\.)(facebook\\.com|fb\\.com)$/.test(h); }
function isInstagram(h) { return /(^|\\.)instagram\\.com$/.test(h); }

// Ссылку FB/IG приводим к странице профиля; посты, события, фото, локации — '' (кроме FB vanity)
function socialPage(url) {
  const raw = String(url || '');
  const p = parseUrl(raw);
  if (!p) return '';
  const h = host(raw);
  const seg = p.pathname.split('/').filter(Boolean);
  if (isFacebook(h)) {
    const v = raw.match(/[?&]vanity=([A-Za-z0-9.\\-_]+)/);
    if (v) return 'https://www.facebook.com/' + v[1] + '/';
    if (seg[0] === 'profile.php') { const id = raw.match(/[?&]id=(\\d+)/); return id ? 'https://www.facebook.com/profile.php?id=' + id[1] : ''; }
    if (seg[0] === 'pages' && seg[1]) return 'https://www.facebook.com/' + seg.slice(1, 3).join('/') + '/';
    if (seg[0] === 'p' && seg[1]) return 'https://www.facebook.com/p/' + seg[1] + '/';
    if (!seg[0] || /^(media|events|groups|photo|photo\\.php|photos|watch|story\\.php|permalink\\.php|sharer|share|hashtag|reel|people|search|login|marketplace)$/i.test(seg[0])) return '';
    return 'https://www.facebook.com/' + seg[0] + '/';
  }
  if (isInstagram(h)) {
    if (!seg[0] || /^(explore|p|reel|reels|stories|tv|accounts|direct|popular|topics|web|about|legal|developer|directory|challenge)$/i.test(seg[0])) return '';
    return 'https://www.instagram.com/' + seg[0] + '/';
  }
  return '';
}

// Характерные слова названия (без общих слов вроде club/bar/rock) — для сверки с доменами
const STOP_WORDS = ['the','and','club','klub','bar','pub','cafe','rock','music','live','hall','park','house','festival','fest',
  'post','punk','back','strikes','again','arena','venue','centre','center','sala','teatro','teatr','space','studio','stage',
  'dos','das','de','da','do','la','le','los','el','du','des','der','die','und','na','w','z',
  'arci','csa','circolo','centro','sociale'];
// minLen 4 — для сверки с доменами сайтов (короткие слова дают ложные совпадения: «old» в goldclub.com);
// для соцсетей — 3 (иначе «bus» из «Le Bus Magique» не учитывается и подходит чужая страница «Le Coin Magique»)
function venueWords(name, minLen) {
  minLen = minLen || 4;
  // пояснение в скобках — не часть названия: «Declansart (Live Radio Stream)» → «Declansart»
  const base = String(name || '').replace(/\\s*\\([^)]*\\)\\s*/g, ' ').trim() || String(name || '');
  const all = base.toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').split(/[^a-z0-9]+/).filter(Boolean);
  if (all.length === 1 && all[0].length >= 3) return all;
  const strong = all.filter(w => w.length >= minLen && !STOP_WORDS.includes(w));
  // Название целиком из общих слов («Post Punk Strikes Back Festival») — берём все длинные, кроме самых общих
  return strong.length ? strong : all.filter(w => w.length >= minLen && !['festival', 'fest', 'club', 'klub', 'music'].includes(w));
}
// Страница соцсети «своя», если в адресе есть характерные слова названия: для названий из нескольких слов —
// хотя бы два (иначе «punk» из «Post Punk Strikes Back» совпадает с чужим scarboroughpunkfestival)
function handleMatches(page, words) {
  // нет характерных слов («Guy Festival» при minLen 4) — не с чем сверять, чужую страницу не берём
  if (!words.length) return false;
  const h = socialHandle(page);
  return words.filter(w => h.includes(w)).length >= Math.min(2, words.length);
}
// адрес страницы без домена, раскодированный и без диакритики: «declan%C5%9FART» → «declansart»
function socialHandle(p) {
  let s = String(p || '').replace(/^https:\\/\\/www\\.[a-z]+\\.com\\//, '');
  try { s = decodeURIComponent(s); } catch (e) {}
  return s.toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/[^a-z0-9]/g, '');
}
function hostLabel(h) { return String(h || '').split('.').slice(0, -1).join('').replace(/[^a-z0-9]/g, ''); }
function hostMatchesVenue(h, words) { const l = hostLabel(h); return !!l && words.some(w => l.includes(w)); }
`;

// v006: проверка названия и города (сайт, страницы соцсетей), отказ от сайта
const NAME_FN = `
const nrm = v => String(v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '');
// страна в тексте результата — признак своей страницы соцсети (вместе с названием в адресе страницы)
const COUNTRY_WORDS = { DE: ['germany', 'deutschland'], FR: ['france'], PL: ['poland', 'polska'], UK: ['uk', 'united kingdom', 'england', 'scotland', 'wales'], GB: ['uk', 'united kingdom', 'england', 'scotland', 'wales'],
  CH: ['switzerland', 'schweiz', 'suisse'], BE: ['belgium', 'belgique', 'belgie'], ES: ['spain', 'espana'], PT: ['portugal'], IT: ['italy', 'italia'], NL: ['netherlands', 'nederland'],
  CZ: ['czech', 'cesko'], SK: ['slovakia', 'slovensko'], SE: ['sweden', 'sverige'], RO: ['romania'], AT: ['austria', 'osterreich'], LT: ['lithuania', 'lietuva'] };
const NAME_STOP = ['the','and','club','klub','bar','pub','cafe','rock','music','live','hall','park','house','festival','fest','arena','venue','centre','center','sala','teatro','space','studio','stage',
  'dos','das','de','da','do','la','le','los','el','du','des','der','die','und','na','w','z','arci','csa','circolo','centro','sociale','e','v','ev','open','air','openair'];
// характерные слова названия от 3 букв (без общих); название из одного слова — как есть
function nameWords(name) {
  const all = String(name || '').replace(/\\([^)]*\\)/g, ' ').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  if (all.length === 1) return all;
  const s = all.filter(w => w.length >= 3 && !NAME_STOP.includes(w));
  return s.length ? s : all.filter(w => w.length >= 3);
}
// название без хвоста-места: «3gHub in Cetate» → «3gHub»
const cutPlace = n => String(n || '').split(/\\s+(?:in|at|@)\\s+/i)[0];
// одна опечатка (OCR): «Weiner» ≈ Weiher
function lev1(x, y) {
  if (Math.abs(x.length - y.length) > 1) return false;
  let i = 0; while (i < x.length && x[i] === y[i]) i++;
  return x.slice(i + 1) === y.slice(i + 1) || x.slice(i) === y.slice(i + 1) || x.slice(i + 1) === y.slice(i);
}
// Сайт свой, если в тексте есть название (слитно или все характерные слова) и город — точно
// или с одной опечаткой в слове от 5 букв, в тексте или в домене; страна не годится
// похожесть названий (исправление модели годится, только если похоже на исходное: «Musikverein Vienna» ≠ «Live Music Hall»)
const big = x => { const a = []; for (let i = 0; i < x.length - 1; i++) a.push(x.slice(i, i + 2)); return a; };
const dice = (x, y) => { const a = big(x), b = big(y).slice(); if (!a.length || !b.length) return 0; let n = 0; for (const g of a) { const k = b.indexOf(g); if (k >= 0) { n++; b.splice(k, 1); } } return 2 * n / (a.length + big(y).length); };
const similar = (x, y) => { const a = nrm(x), b = nrm(y); return a === b || (a.length >= 4 && b.includes(a)) || (b.length >= 4 && a.includes(b)) || dice(a, b) >= 0.5; };
const similarCity = (x, y) => similar(x, y) || similar(String(x).slice(0, 5), String(y).slice(0, 5));
function siteMatches(st, text, h) {
  const all = nrm(text);
  const fx = st.fix || {};
  // название / город от модели — только если похожи на исходные (Live Music Hall «Weiner» → модель: «Köln» — нет)
  const names = [st.name, st.NAME, fx.name && similar(fx.name, st.NAME) ? fx.name : ''].filter(Boolean);
  const cities = [st.city, st.CITY, fx.city && similarCity(fx.city, st.CITY) ? fx.city : ''].filter(Boolean);
  // слово названия от 6 букв — можно с одной опечаткой (OCR: «Poutreasseau» — на сайте «Poutrasseau»)
  const words = new Set(String(text || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().split(/[^a-z0-9]+/).filter(t => t.length >= 5));
  const has = x => all.includes(x) || (x.length >= 6 && [...words].some(t => lev1(t, x)));
  const okName = names.some(n => { const c = nrm(cutPlace(n)); const w = nameWords(n); return (c.length >= 4 && all.includes(c)) || (w.length > 0 && w.every(has)); });
  // с опечаткой — только в домене или в почтовом адресе «индекс + город» («63796 Weiher»): иначе «Weiner» ≈ слово «weiter»
  const low = String(text || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase();
  const tokens = String(h).split(/[^a-z0-9]+/).filter(t => t.length >= 5);
  for (const m of low.matchAll(/\\b\\d{4,5}\\s+([a-z][a-z-]{4,})/g)) tokens.push(m[1].replace(/-/g, ''));
  const okCity = cities.some(c => nrm(c).length >= 3 && all.includes(nrm(c))) ||
    cities.some(c => { const cn = nrm(c); return cn.length >= 5 && tokens.some(t => lev1(t, cn)); });
  return { okName, okCity };
}
// Адрес годится для базы (не служебный адрес платформы / шаблона) — по нему решается, искать ли дальше
// (у 361grad.de на Wix — только адреса Wix: искать дальше)
function usableEmail(e) {
  const [l, d] = String(e || '').split('@');
  if (!l || !d) return false;
  if (/(^|\\.)(example\\.(com|org|net)|domain\\.com|email\\.com|yourdomain\\.com|yoursite\\.com|mysite\\.com|company\\.com|test\\.com|w3\\.org|schema\\.org|facebookmail\\.com|instagram\\.com|facebook\\.com)$/.test(d)) return false;
  if (/(^|\\.)(ovh|wix|wixpress|sentry|sentry-next|schedulista|mapado|seopress|wordpress|wpengine|elementor|godaddy|ionos|hostinger|squarespace|webflow|jimdo|strato|1and1)\\.[a-z.]+$/.test(d)) return false;
  return !/^(name|your(name|email)?|email|user|username|someone|john\\.?doe|example|test|no-?reply|do-?not-?reply|donotreply|mailer-daemon|postmaster|bounces?|notifications?)$/.test(l);
}
// Сайт отвергнут: в SCRAPER NOTES, его адреса из выдачи не берём
function rejectSite(st, h, why) {
  st.siteRejected.push(h);
  st.scraperNotes.push('site rejected: ' + h);
  st.emails = st.emails.filter(e => e.email.split('@')[1] !== h);
  st.log.push('   site ' + h + ' rejected (' + why + ')');
}
`;

// ── Узлы ────────────────────────────────────────────────────────────────

const start = trigger({ type: 'n8n-nodes-base.manualTrigger', version: 1, config: { name: 'Start' } });

// Запуск из Telegram-бота: TourVenues_09_worker_gateway передаёт { dryRun, maxRows, chatId, requestedBy }.
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
          // true — ищет, но ничего не пишет в базу и не трогает 03 (отчёт приходит); по умолчанию false
          { id: 's-dry', name: 'dryRun', value: expr('{{ $json.dryRun === true }}'), type: 'boolean' },
          // сколько строк 03 обработать за запуск; 0 — все необработанные, пока не кончатся кредиты Tavily.
          // Обычное число (с 2026-10-22): владелец меняет его в n8n сам (0 или 20); действует и на запуски из бота.
          // Перед загрузкой новой сборки — спросить владельца, какое число сейчас стоит в n8n
          { id: 's-max', name: 'maxRows', value: 5, type: 'number' },
          { id: 's-ext', name: 'useExtract', value: true, type: 'boolean' },
          // бюджет кредитов Tavily на строку: поиск идёт шагами (NAME CITY, … email, advanced, исправленное название,
          // Facebook/Instagram, extract), пока не найдены адреса или не кончился бюджет (в среднем уходит ≈2,5)
          { id: 's-cpr', name: 'creditsPerRow', value: 6, type: 'number' },
          // отчёт всегда владельцу; запустил кто-то другой из бота — ещё и ему (requestChatId)
          { id: 's-chat', name: 'telegramChatId', value: '<OWNER_TELEGRAM_CHAT_ID>', type: 'string' },
          { id: 's-req', name: 'requestChatId', value: expr("{{ $json.chatId ? String($json.chatId) : '' }}"), type: 'string' },
          { id: 's-by', name: 'requestedBy', value: expr("{{ $json.requestedBy || '' }}"), type: 'string' },
          // дата для столбца UPDATED AT (только день, часовой пояс Праги)
          { id: 's-date', name: 'today', value: expr("{{ $now.setZone('Europe/Prague').toFormat('yyyy-MM-dd') }}"), type: 'string' }
        ]
      }
    }
  }
});

const readNewData = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read New Data',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
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

// База — чтобы не искать заново строку, которая уже дописана туда (например, прошлый запуск оборвался
// между записью в базу и отметкой в 03)
const readBase = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Read Base',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: BASE_ID },
      sheetName: BASE_SHEET,
      options: {}
    },
    credentials: GOOGLE
  }
});

const selectRows = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Select Rows',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `// Строки 03 без отметки «processed …» в Automation Status, снизу вверх.
// Уже есть в базе (та же страна + город + название) — без поиска, только отметка в 03.
// Остальные — все (или не больше maxRows, если он задан); кредиты заранее не резервируются.
const s = $('Settings').first().json;
const S = v => String(v == null ? '' : v);
const norm = v => S(v).normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '');
const keyOf = r => norm(r.COUNTRY) + '|' + norm(r.CITY) + '|' + norm(r.NAME);
let dn = null;
try { dn = new Intl.DisplayNames(['en'], { type: 'region' }); } catch (e) {}

const inBase = new Set($('Read Base').all().map(i => i.json).filter(r => r.NAME).map(keyOf));
// строки с отметкой «processed …» уже обработаны (заметки об ошибках и кредитах — повторяются)
const all = $('Read New Data').all().map(i => i.json).filter(r => r.row_number && S(r.NAME).trim())
  .filter(r => !/^processed\\b/i.test(S(r['Automation Status']).trim()))
  .sort((a, b) => b.row_number - a.row_number);
// «redo» в Automation Status — искать заново и перезаписать строку базы (даже если она уже там есть)
const isRedo = r => /^redo\\b/i.test(S(r['Automation Status']).trim());
const free = all.filter(r => inBase.has(keyOf(r)) && !isRedo(r));
const paid = all.filter(r => !inBase.has(keyOf(r)) || isRedo(r));
const take = s.maxRows > 0 ? paid.slice(0, s.maxRows) : paid;
const rows = free.concat(take).sort((a, b) => b.row_number - a.row_number).map(r => {
  const code = S(r.COUNTRY).trim();
  let countryName = code;
  if (dn && /^[A-Za-z]{2}$/.test(code)) { try { countryName = dn.of(code.toUpperCase()) || code; } catch (e) {} }
  return {
    row_number: r.row_number, COUNTRY: S(r.COUNTRY).trim(), CITY: S(r.CITY).trim(), NAME: S(r.NAME).trim(),
    mode: inBase.has(keyOf(r)) && !isRedo(r) ? 'inBase' : 'search',
    redo: isRedo(r),
    countryName,
    dryRun: s.dryRun
  };
});
return [{ json: {
  count: rows.length, total: all.length,
  waiting: paid.length - take.length, rows
} }];`
    }
  }
});

const hasRows = ifElse({
  version: 2.2,
  config: {
    name: 'Has Rows?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.count }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
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

const loopRows = splitInBatches({ version: 3, config: { name: 'Loop Rows', parameters: { batchSize: 1, options: {} } } });

const isInBase = ifElse({
  version: 2.2,
  config: {
    name: 'Already In Base?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.mode }}'), rightValue: 'inBase', operator: { type: 'string', operation: 'equals' } }],
        combinator: 'and'
      }
    }
  }
});

// ── Поиск по строке: цикл «план → действие → сбор находок → снова план» (v005) ──
// Состояние строки (st) идёт по кругу через все узлы; HTTP-узлы его не несут, поэтому Code-узел после HTTP
// берёт st из узла, который запустил этот шаг ($('Plan Step') / $('Harvest Search') — их последний запуск).

// Начальное состояние строки
const initRow = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Init Row',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const s = $('Settings').first().json;
const r = $input.first().json;
// name / city — текущие (после принятого исправления моделью); NAME / CITY — как в строке 03
return [{ json: { ...r, budget: s.creditsPerRow, useExtract: s.useExtract,
  name: r.NAME, city: r.CITY, fix: {}, fixBefore: 0, fixOk: false,
  credits: 0, steps: 0, tried: [], lastSearch: '', results: [], emails: [], answers: [],
  siteCands: [], siteFetched: [], siteUrl: '', siteHost: '', siteFrom: '', siteOk: false, siteRejected: [],
  retryHttp: '', pendingExtract: '', extractHost: '', contactPages: [], homeText: '',
  social: [], facebook: '', instagram: '', searchName: '', description: '',
  scraperNotes: [], fatal: '', fatalError: '', searchErrors: [], log: [] } }];`
    }
  }
});

// Решает, что делать дальше: fetch (сайт, бесплатно) / extract (1 кредит) / search (1 кредит) / finish
const planStep = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Plan Step',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + AGG_FN + NAME_FN + `
const st = JSON.parse(JSON.stringify($input.first().json));
delete st.action; delete st.req; delete st.notesPrompt; delete st.llmPrompt; delete st.askModel;
st.steps = (st.steps || 0) + 1;
// country — поднимает в выдаче свою страну (без него «Brandenburg» уходил в Кентукки). exclude_domains не используем:
// они меняют выдачу (у Nostalgie Café пропадал сайт), агрегаторы отсеиваются после поиска
const CN = { DE: 'germany', FR: 'france', PL: 'poland', UK: 'united kingdom', GB: 'united kingdom', CH: 'switzerland', BE: 'belgium', ES: 'spain', PT: 'portugal', IT: 'italy',
  NL: 'netherlands', CZ: 'czech republic', SK: 'slovakia', SE: 'sweden', RO: 'romania', AT: 'austria', LT: 'lithuania', DK: 'denmark', NO: 'norway', FI: 'finland', HU: 'hungary',
  GR: 'greece', IE: 'ireland', SI: 'slovenia', HR: 'croatia', RS: 'serbia', BG: 'bulgaria', LU: 'luxembourg' };
const country = CN[String(st.COUNTRY || '').toUpperCase()];
const withCountry = q => Object.assign(country ? { country } : {}, q);
const done = k => st.tried.includes(k);
function plan() {
  if (st.fatal) return ['finish', null, 'tavily: ' + st.fatal];
  if (st.steps > 20) return ['finish', null, 'too many steps'];
  const left = st.budget - st.credits;
  if (st.retryHttp) {
    st.siteUrl = st.retryHttp; st.siteHost = host(st.retryHttp); st.retryHttp = '';
    return ['fetch', null, st.siteUrl + ' (https failed)'];
  }
  // сайт не открылся — его главная через Tavily extract (1 кредит)
  if (st.pendingExtract) {
    const h = st.pendingExtract; st.pendingExtract = '';
    if (st.useExtract && left >= 1) { st.extractHost = h; return ['extract', { urls: ['https://' + h + '/'], extract_depth: 'basic' }, h + ' did not load']; }
    st.log.push('   site ' + h + ': not loaded');
  }
  // кандидат в сайты (до 3 на строку, пока ни один не принят) — главная + страницы контактов (бесплатно)
  const nextSite = !st.siteOk && st.siteFetched.length < 3 ? st.siteCands.find(c => !st.siteFetched.includes(host(c.url))) : null;
  if (nextSite) {
    const p = parseUrl(nextSite.url);
    st.siteUrl = p.origin + '/'; st.siteHost = host(st.siteUrl); st.siteFrom = nextSite.from;
    st.siteFetched.push(st.siteHost);
    return ['fetch', null, st.siteHost + ' (' + nextSite.from + ')'];
  }
  const search = (key, q) => { st.tried.push(key); st.lastSearch = key; return ['search', withCountry(q), key + ': ' + q.query]; };
  const nm = st.name, ct = st.city;
  // 1. короткий запрос: Tavily ищет по смыслу всей фразы, длинный хвост уводит к путеводителям
  if (!done('q1')) return search('q1', { query: nm + ' ' + ct });
  const haveEmail = st.emails.some(e => usableEmail(e.email));
  // 2. адресов нет — «… email»: находит адреса в каталогах и фрагментах (берутся только адреса по названию и со своих страниц)
  if (!haveEmail && !done('email') && left >= 1) return search('email', { query: nm + ' ' + ct + ' email' });
  // 3. адресов нет — тот же запрос глубже (advanced, 2 кредита): более длинные куски страниц, в т.ч. «About»
  if (!haveEmail && !done('adv') && left >= 2) return search('adv', { query: nm + ' ' + ct, search_depth: 'advanced', chunks_per_source: 3 });
  // 4. адресов нет — поиск по исправленному моделью названию / городу (если исправление похоже на исходное);
  //    и когда сайт найден, но адресов на нём нет (361grad.de на Wix → «361 Grad Offenburg email»)
  if (!done('fix')) {
    st.tried.push('fix');
    const f = st.fix || {};
    const changed = f.query && nrm(f.query) !== nrm(nm + ' ' + ct);
    if (!haveEmail && changed && left >= 1) {
      if (f.name && !similar(f.name, nm)) st.log.push('   fix rejected: "' + f.name + '"');
      else if (f.city && !similarCity(f.city, ct)) st.log.push('   fix rejected: city "' + f.city + '"');
      else {
        st.fixBefore = st.siteCands.length + st.social.length + st.emails.length;
        st.lastSearch = 'fix';
        return ['search', withCountry({ query: String(f.query) }), 'fix: ' + f.query];
      }
    }
  }
  // 5. исправление принято, адресов нет — «… email» по новому названию (361' Offgrade → 361 Grad: info@361grad.de)
  if (!haveEmail && st.fixOk && !done('email2') && left >= 1) return search('email2', { query: nm + ' ' + ct + ' email' });
  // 6. адресов нет — только Facebook / Instagram
  if (!haveEmail && !done('soc') && left >= 1) return search('soc', { query: nm + ' ' + ct + ' facebook', include_domains: ['facebook.com', 'instagram.com'], max_results: 8 });
  return ['finish', null, haveEmail ? 'emails found' : (left < 1 ? 'credit budget used' : 'nothing more to try')];
}
const [action, req, why] = plan();
st.log.push(st.steps + ' ' + action + ' — ' + why);

// В конце — материал для NOTES (описание + жанры): текст сайта, ответы поиска, фрагменты своих страниц
let notesPrompt = '';
if (action === 'finish') {
  const own = st.results.filter(r => r.own || (st.siteHost && (r.host === st.siteHost || r.host.endsWith('.' + st.siteHost))));
  // площадка не подтверждена (нет своего сайта, страницы соцсети, адреса) — модели только результаты, где назван город,
  // без сводок поиска: иначе NOTES описывают чужое место (Villagefest, Kölsa → VillageFest в Палм-Спрингс)
  const confirmed = st.siteOk || st.social.length > 0 || st.emails.some(e => usableEmail(e.email));
  const cityN = nrm(st.city || st.CITY);
  const other = st.results.filter(r => !own.includes(r) && (confirmed || (cityN.length >= 3 && nrm(r.title + ' ' + r.snippet).includes(cityN))));
  st.notesUnconfirmed = !confirmed;
  st.notesEmpty = !confirmed && !other.length;
  if (!confirmed) st.answers = [];
  const snips = own.concat(other).slice(0, 10).map(r => '- ' + r.title + ' (' + r.url + '): ' + r.snippet.slice(0, 300)).join('\\n');
  notesPrompt = 'Venue: ' + st.NAME + (st.searchName && st.searchName !== st.NAME ? ' (known as ' + st.searchName + ')' : '') +
    '\\nCity: ' + st.CITY + '\\nCountry: ' + st.countryName +
    '\\nOfficial site: ' + (st.siteOk ? st.siteUrl : '(none)') +
    '\\nSocial: ' + ([st.facebook, st.instagram].filter(Boolean).join(', ') || '(none)') +
    '\\n\\nText from the official site:\\n' + (st.homeText || '(none)').slice(0, 1500) +
    '\\n\\nSearch summaries:\\n' + (st.answers.filter(Boolean).map(a => '- ' + a.slice(0, 400)).join('\\n') || '(none)') +
    '\\n\\nSearch results:\\n' + (snips || '(none)');
}
return [{ json: { ...st, action, req, notesPrompt } }];`
    }
  }
});

const route = switchCase({
  version: 3.2,
  config: {
    name: 'Route Step',
    parameters: {
      rules: {
        values: [
          { outputKey: 'search', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'search', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'fetch', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'fetch', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'extract', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'extract', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } },
          { outputKey: 'finish', conditions: { options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' }, conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'finish', operator: { type: 'string', operation: 'equals' } }], combinator: 'and' } }
        ]
      },
      options: {}
    }
  }
});

const tavilySearch = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Tavily Search',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'POST',
      url: TAVILY_URL + '/search',
      authentication: 'genericCredentialType',
      genericAuthType: 'httpHeaderAuth',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify(Object.assign({ search_depth: "basic", max_results: 10, include_answer: "basic", include_raw_content: "markdown" }, $json.req)) }}'),
      options: { timeout: 60000 }
    },
    credentials: TAVILY
  }
});

// Находки поиска → состояние: свои страницы соцсетей, адреса, кандидаты в сайты; проверка исправления названия
const harvest = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Harvest Search',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + AGG_FN + NAME_FN + FATAL_FN + `
const st = JSON.parse(JSON.stringify($('Plan Step').first().json));
const req = st.req || {};
delete st.action; delete st.req; delete st.notesPrompt;
const t = $input.first().json || {};
st.credits += req.search_depth === 'advanced' ? 2 : 1;
const err = t.error ? String(t.error.message || JSON.stringify(t.error)) : '';
const fatal = tavilyFatal(t.error);
if (fatal) { st.fatal = fatal; st.fatalError = err.slice(0, 200); }
else if (err) st.searchErrors.push(st.lastSearch + ': ' + err.slice(0, 150));
// поиск по исправленному названию сверяем с исправленными названием и городом
const isFix = st.lastSearch === 'fix';
const n = isFix ? (st.fix.name || st.name) : st.name;
const c = isFix ? (st.fix.city || st.city) : st.city;
const cN = nrm(c);
// название слитно — целиком и без хвоста-места («3gHub in Cetate» → 3ghubincetate, 3ghub)
const comp = [...new Set([n, cutPlace(n)].map(nrm).filter(x => x.length >= 4))];
// слова названия от 3 букв БЕЗ отбрасывания общих («Rock im Floor» → rock, floor), кроме слов города
const nw = String(n).replace(/\\([^)]*\\)/g, ' ').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().split(/[^a-z0-9]+/).filter(x => x.length >= 3 && !cN.includes(x));
const need = nw.length <= 3 ? nw.length : nw.length - 1;
// домен = название: слитно или все слова (зона тоже считается: nostalgie.cafe); не поддомен чужой платформы (x.mapado.com)
function hostOk(h) {
  const parts = String(h || '').split('.');
  if (parts.length >= 3 && !/^(co|com|org|net)$/.test(parts[parts.length - 2])) {
    const base = parts.slice(-2).join('');
    if (!comp.some(x => base.includes(x)) && nw.filter(x => base.includes(x)).length < need) return false;
  }
  const flat = String(h || '').replace(/[^a-z0-9]/g, '');
  return comp.some(x => flat.includes(x)) || (nw.length > 0 && nw.filter(x => flat.includes(x)).length >= need);
}
const cw = (COUNTRY_WORDS[String(st.COUNTRY || '').toUpperCase()] || []).map(nrm);
const place = x => (!!cN && x.includes(cN)) || cw.some(w => x.includes(w));
const FREE = /(gmail|googlemail|yahoo|hotmail|outlook|gmx|web\\.de|wanadoo|orange|free\\.fr|laposte|wp\\.pl|o2\\.pl|onet|interia|seznam|libero|proton|icloud)/;
// адрес страницы без домена: /p/Имя-<номер>/ → имя-<номер>, profile.php?id=<номер> → номер
function handleRaw(page) {
  let s = String(page || '').replace(/^https?:\\/\\/[^\\/]+\\//, '');
  const id = s.match(/profile\\.php\\?id=(\\d+)/); if (id) return id[1];
  s = s.replace(/^(p|pages)\\//, '').replace(/\\/+$/, '');
  try { s = decodeURIComponent(s); } catch (e) {}
  return s.toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '');
}
// корень страницы соцсети или «About» — адреса в постах чаще чужие (группы, агентства)
function profileRoot(url) {
  const p = String(url).replace(/^https?:\\/\\/[^\\/]+/, '').split(/[?#]/)[0].split('/').filter(Boolean);
  if (p[0] === 'p') return p.length <= 2 || p[2] === 'about';
  return p.length <= 1 || (p.length === 2 && /^(about|about_contact_and_basic_info)$/.test(p[1]));
}
// название с небольшим отличием в написании (OCR): «Musicalausse» — страница facebook.com/Musicausse,
// адрес campag-art.musicausse@… Части адреса страницы / e-mail от 5 букв, похожесть ≥ 0.8; только вместе с городом в тексте
function near(s) {
  const parts = String(s || '').toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').split(/[^a-z0-9]+/).filter(p => p.length >= 5);
  // длина почти та же (опечатка), а не кусок названия: «fetesdebayonne» ≠ «zuzulufetesdebayonne»
  for (const p of parts) for (const x of comp) if (x.length >= 6 && Math.abs(p.length - x.length) <= 3 && dice(p, x) >= 0.8) return p;
  return '';
}
const nearHits = [];
const results = Array.isArray(t.results) ? t.results : [];
const list = results.map((r, i) => {
  const url = String(r.url || ''), title = String(r.title || '');
  const text = String(r.content || '') + ' ' + String(r.raw_content || '');
  const page = socialPage(url);
  const raw = page ? handleRaw(page) : '';
  const hs = nrm(raw);
  // своя страница соцсети: название в адресе страницы (слитно или два слова) И город/страна в тексте результата;
  // без города — только если адрес начинается с полного названия от 8 букв (nostalgiecafe_das… — да, UpInSmokes — нет)
  // …или адрес страницы = город + все характерные слова названия от 5 букв + не больше 3 лишних букв
  // (White Pub, Flaine → flainelewhite; но не StellarSwampBrussels для The Swamp, не fetes_de_bayonne для Zuzulu)
  const sw = nw.filter(x => x.length >= 5 && !NAME_STOP.includes(x));
  const rest = sw.reduce((s, x) => s.replace(x, ''), hs.replace(cN, ''));
  const hm = !!page && (comp.some(x => hs.includes(x)) || (nw.length >= 2 && nw.filter(x => hs.includes(x)).length >= Math.min(2, need)) ||
    (nw.length === 1 && nw[0].length >= 5 && hs.includes(nw[0])) ||
    (cN.length >= 4 && hs.includes(cN) && sw.length > 0 && sw.every(x => hs.includes(x)) && rest.length <= 3));
  const head = raw.split(/[_.\\-]/)[0].replace(/[^a-z0-9]/g, ''), flatH = raw.replace(/[^a-z0-9]/g, '');
  const strong = comp.some(x => x.length >= 8 && (head === x || flatH === x));
  const inPlace = place(nrm(title + ' ' + (r.content || '')));
  let own = hm && (inPlace || strong || (cN.length >= 4 && hs.includes(cN.slice(0, 6))));
  const nh = !!page && !hm && inPlace ? near(raw) : '';
  if (nh) { own = true; nearHits.push(nh); }
  // для адресов с похожим написанием город ищется и в полном тексте страницы (каталог ma-source.info: город — ниже фрагмента)
  const inPlaceLong = inPlace || place(nrm(String(r.raw_content || '').slice(0, 8000)));
  return { n: i + 1, q: st.lastSearch, title, url, host: host(url), page, own, inPlace, inPlaceLong, root: !!page && profileRoot(url),
    snippet: String(r.content || '').replace(/\\s+/g, ' ').slice(0, 400), emails: extractEmails(text), text: text.slice(0, 20000) };
});
// Одна страница Facebook приходит под разными адресами: /p/Название-<номер>/, /<номер>/, profile.php?id=<номер> —
// страница с тем же номером, что у уже признанной своей, тоже своя (Nostalgie Café: адрес был только во фрагменте /61578052082636)
const fbId = p => { const s = String(p || ''); const m = s.match(/facebook\\.com\\/(?:p\\/[^\\/]*?-)?(\\d{8,})\\/?$/) || s.match(/profile\\.php\\?id=(\\d{8,})/); return m ? m[1] : ''; };
const prev = st.results;
const ownIds = new Set(prev.concat(list).filter(r => r.own && isFacebook(r.host)).map(r => fbId(r.page)).filter(Boolean));
for (const r of prev.concat(list)) if (!r.own && r.page && isFacebook(r.host) && ownIds.has(fbId(r.page))) { r.own = true; r.sameId = true; }
const pushCand = (url, from) => { const h = host(url); if (h && !st.siteCands.some(x => host(x.url) === h) && !st.siteFetched.includes(h)) st.siteCands.push({ url, from }); };
const pushEmail = (email, source, kind) => { if (!st.emails.some(e => e.email === email && (e.kind === 'named' || e.kind === 'social'))) st.emails.push({ email, source, kind }); };
const pushSocial = r => {
  if (st.social.includes(r.page)) return;
  st.social.push(r.page);
  if (isFacebook(r.host) && !st.facebook) st.facebook = r.page;
  if (isInstagram(r.host) && !st.instagram) st.instagram = r.page;
};
// страница из прошлых поисков, которая стала своей только сейчас (по номеру), — её адреса тоже берём
for (const r of prev) if (r.sameId) { pushSocial(r); if (r.root) for (const e of r.emails) pushEmail(e, r.page, 'social'); }
// доменная зона ссылки из текста — настоящая (не «zuzulu.baiona» из адреса Instagram)
const TLD_OK = /\\.([a-z]{2}|com|org|net|info|eu|cafe|club|bar|events?|live|music|art|studio|online|site|xyz|bzh|eus|cat|gal|berlin|wien|koeln|hamburg|london|scot|wales|paris)$/;
for (const r of list) {
  // кандидат в сайты: домен по названию (не соцсеть, не агрегатор / каталог / билеты)
  if (r.host && !r.page && !isFacebook(r.host) && !isInstagram(r.host) && !isAggregator(r.host) && hostOk(r.host)) pushCand(r.url, 'domain');
  if (r.own) pushSocial(r);
  for (const e of r.emails) {
    const [loc, dom] = e.split('@');
    const ln = nrm(loc);
    // адрес «по названию»: домен = название, или в имени адреса начало слитного названия / слово названия от 5 букв
    // (agenda.ferro@, csoazapata@) — из любого результата; иначе — только с корня / «About» своей страницы соцсети
    let named = hostOk(dom) || comp.some(x => x.length >= 5 && ln.includes(x.slice(0, 8))) || nw.some(x => x.length >= 5 && ln.includes(x));
    if (!named && r.inPlaceLong) { const nh = near(loc) || near(dom.split('.')[0]); if (nh) { named = true; nearHits.push(nh); } }
    if (named) pushEmail(e, r.url, 'named');
    else if (r.own && r.root) pushEmail(e, r.page || r.url, 'social');
    // домен адреса по названию — тоже кандидат в сайты (abstand@rigaer78.org → rigaer78.org)
    if (named && !FREE.test(dom) && !isAggregator(dom)) pushCand('https://' + dom + '/', 'email');
  }
  // ссылка на сайт в тексте результата (3ghub.ro в посте) — последней очередью
  const re = /(?:https?:\\/\\/)?(?:www\\.)?((?:[a-z0-9-]+\\.)+[a-z]{2,})(?=[\\/\\s"')\\]>,]|$)/gi;
  let m;
  while ((m = re.exec(r.text))) {
    const h = m[1].toLowerCase();
    if (r.text[m.index - 1] === '@') continue;
    if (/\\.(png|jpe?g|gif|webp|svg|ico|pdf|js|css|php|html?|aspx?|mp[34]|zip|json|xml|txt)$/.test(h) || !TLD_OK.test(h)) continue;
    if (!isAggregator(h) && !isFacebook(h) && !isInstagram(h) && hostOk(h)) pushCand('https://' + h + '/', 'link');
  }
}
if (t.answer) st.answers.push(String(t.answer));
st.results = st.results.concat(list.map(r => { const x = { ...r }; delete x.text; return x; })).slice(0, 60);
for (const h of [...new Set(nearHits)]) { const msg = 'similar spelling accepted: "' + h + '"'; if (!st.scraperNotes.includes(msg)) st.scraperNotes.push(msg); }
const found = st.siteCands.length + st.social.length + st.emails.length;
// исправление принимается, только если по нему что-то нашлось
if (isFix) {
  if (found > st.fixBefore) {
    if (nrm(n) !== nrm(st.name)) st.scraperNotes.push('name corrected: "' + st.name + '" → "' + n + '"');
    if (nrm(c) !== nrm(st.city)) st.scraperNotes.push('city corrected: "' + st.city + '" → "' + c + '"');
    st.name = n; st.city = c; st.fixOk = true;
    if (nrm(n) !== nrm(st.NAME)) st.searchName = n;
  } else st.log.push('   fix gave nothing');
}
// после первого поиска — модель: опознана ли площадка, исправленные название / город, официальный сайт
st.askModel = st.lastSearch === 'q1' && !st.fatal && results.length > 0;
const llmPrompt = 'Row: ' + st.COUNTRY + ' | ' + st.CITY + ' | ' + st.NAME +
  '\\nSearch answer: ' + String(t.answer || '').slice(0, 300) + '\\nResults:\\n' +
  results.slice(0, 8).map((x, i) => '[' + (i + 1) + '] ' + x.title + ' | ' + x.url + ' | ' + String(x.content || '').replace(/\\s+/g, ' ').slice(0, 160)).join('\\n');
return [{ json: { ...st, llmPrompt } }];`
    }
  }
});

// topK 1 — всегда самый вероятный ответ: при temperature 0 модель иногда отвечала по-разному на один и тот же запрос
const ollamaModel = languageModel({
  type: '@n8n/n8n-nodes-langchain.lmChatOllama',
  version: 1,
  config: {
    name: 'Ollama Model',
    parameters: { model: 'gemma4:26b', options: { temperature: 0, topK: 1, numCtx: 8192, keepAlive: '15m', think: false } },
    credentials: OLLAMA
  }
});

const sourcesParser = outputParser({
  type: '@n8n/n8n-nodes-langchain.outputParserStructured',
  version: 1.3,
  config: {
    name: 'Sources JSON',
    parameters: {
      schemaType: 'fromJson',
      jsonSchemaExample: '{ "found": true, "name": "Ferro Bar", "city": "Pratteln", "query": "Ferro Bar Pratteln", "confidence": "high", "official_site": "ferrobar.ch", "why": "results show the bar in Pratteln" }'
    }
  }
});

// Модель — только после первого поиска (по остальным поискам сразу к следующему шагу)
const askModel = ifElse({
  version: 2.2,
  config: {
    name: 'Ask Model?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.askModel }}'), operator: { type: 'boolean', operation: 'true', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const pickSources = node({
  type: '@n8n/n8n-nodes-langchain.chainLlm',
  version: 1.9,
  config: {
    name: 'Pick Sources',
    // модель не ответила / ответ не разобран — поиск идёт дальше без её подсказок
    onError: 'continueRegularOutput',
    parameters: {
      promptType: 'define',
      text: expr('{{ $json.llmPrompt }}'),
      hasOutputParser: true,
      messages: {
        messageValues: [
          {
            type: 'SystemMessagePromptTemplate',
            message: `You help a music booking agency. A row (country code, city, venue/festival name) was read by OCR from a tour poster and may contain mistakes (misspelled name or city, region instead of city, abbreviations, location words glued to the name).
Given the row and the titles/snippets of a web search for "<name> <city>", decide:
- "found": true if the search results clearly show this exact venue/festival (allowing small spelling differences), else false.
- "name": the correct name of the venue/festival as used locally (fix OCR errors if you are confident; otherwise repeat the given name).
  If the name joins a venue with an event or a place ("Zuzulu Fetes de Bayonne" = the bar Zuzulu during the Fêtes de Bayonne; "3gHub in Cetate" = 3gHub in the Citadel), the name is the venue ("Zuzulu", "3gHub"): never replace the venue by the event, the building or the city.
- "city": the correct city/town where it is (fix misspellings, replace a region by the town if known; otherwise repeat).
- "query": the best short web search query (2-5 words, no extra words like "official website") to find its own website or Facebook page.
- "confidence": "high" / "medium" / "low" for your correction.
- "official_site": the venue's OWN website domain if it appears in the results (or you know it with certainty), e.g. "fishergatepoint.com"; not ticket shops, listings, aggregators, social networks. Empty string if none.
- "why": max 15 words.
Use the search results and your own knowledge of European music venues and festivals. Return JSON only.`
          }
        ]
      }
    },
    subnodes: { model: ollamaModel, outputParser: sourcesParser }
  }
});

// Ответ модели → состояние: сайт первым в очередь кандидатов (проверяется по названию + городу),
// исправление названия / города — для шага «fix» (принимается, только если похоже и по нему что-то нашлось)
const mergePick = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Merge Pick',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + AGG_FN + `
const st = JSON.parse(JSON.stringify($('Harvest Search').first().json));
delete st.llmPrompt;
const j = $input.first().json || {};
let o = j.output;
if (!o && j.text) { try { o = JSON.parse(String(j.text).replace(/^\`\`\`(json)?|\`\`\`$/g, '')); } catch (e) {} }
o = o || {};
let site = String(o.official_site || '').trim();
if (site && !/^https?:\\/\\//i.test(site)) site = 'https://' + site.replace(/^\\/+/, '');
const sh = host(site);
if (sh && sh.includes('.') && !isAggregator(sh) && !isFacebook(sh) && !isInstagram(sh) && !st.siteFetched.includes(sh)) {
  st.siteCands = st.siteCands.filter(c => host(c.url) !== sh);
  st.siteCands.unshift({ url: 'https://' + sh + '/', from: 'llm' });
}
const S = v => String(v == null ? '' : v).trim().slice(0, 80);
st.fix = { found: o.found, name: S(o.name), city: S(o.city), query: S(o.query), confidence: S(o.confidence), official_site: sh, why: S(o.why) };
st.llm = (st.llm || []).concat([o]);
return [{ json: st }];`
    }
  }
});

const fetchHomepage = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Fetch Homepage',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'GET',
      url: expr('{{ $json.siteUrl }}'),
      sendHeaders: true,
      specifyHeaders: 'keypair',
      headerParameters: BROWSER_HEADERS,
      // полный ответ — нужен код: страница-заглушка 403/418 с текстом — не сайт, его главную возьмёт Tavily extract
      options: { allowUnauthorizedCerts: true, timeout: 15000, response: { response: { fullResponse: true, responseFormat: 'text', neverError: true } } }
    }
  }
});

const findContactPages = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Find Contact Pages',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + AGG_FN + `
const st = JSON.parse(JSON.stringify($('Plan Step').first().json));
delete st.action; delete st.req; delete st.notesPrompt;
const r = $input.first().json || {};
let html = typeof r.body === 'string' ? r.body : (typeof r.data === 'string' ? r.data : '');
const code = Number(r.statusCode || 200);
const base = st.siteUrl;
const plain = html.replace(/<script[\\s\\S]*?<\\/script>|<style[\\s\\S]*?<\\/style>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\\s+/g, ' ').trim();
// Главная не открылась (или пустая) — сайт проверит Tavily extract; сайт принимается (название + город)
// в Collect Site Emails — по главной вместе со страницами контактов
const homeOk = code < 400 && html.length > 500;
if (!homeOk) html = '';
const KEY = /(contact|kontakt|contacto|contato|impressum|booking|o-nas|onas|about|sobre|quem-somos|info|wspolpraca|współpraca|management|press|mentions|qui-sommes|kontakty)/i;
const SKIP = /(privacy|polityka|prywatnosci|cookie|regulamin|terms|rodo|gdpr|politica-de-privacidade|privacidade|newsletter|login|cart|koszyk)/i;
const STRONG = /(contact|kontakt|contacto|contato|booking)/i;
const links = [];
const re = /<a\\s[^>]*href\\s*=\\s*["']([^"'#]+)["'][^>]*>([\\s\\S]*?)<\\/a>/gi;
let m;
while ((m = re.exec(html))) {
  const href = m[1].trim();
  const text = m[2].replace(/<[^>]+>/g, ' ').replace(/\\s+/g, ' ').trim();
  if (/^(mailto|tel|javascript):/i.test(href) || href.includes('@')) continue;
  if (!KEY.test(href) && !KEY.test(text)) continue;
  if (SKIP.test(href)) continue;
  const u = parseUrl(resolveUrl(href, base));
  if (!u) continue;
  const h = host(u.origin);
  if (!(h === st.siteHost || h.endsWith('.' + st.siteHost))) continue;
  if (/\\.(pdf|jpe?g|png|zip)$/i.test(u.pathname)) continue;
  const url = u.origin + u.pathname;
  if (url === base || links.some(l => l.url === url)) continue;
  links.push({ url, score: (STRONG.test(href) || STRONG.test(text)) ? 0 : 1 });
}
links.sort((a, b) => a.score - b.score);
let pages = links.slice(0, 3).map(l => l.url);
// главная не открылась — одна пустышка, чтобы цепочка шла дальше; ссылок нет — типовые адреса
if (!homeOk) pages = [base];
else if (pages.length === 0) pages = [base + 'contact', base + 'kontakt'];
// Ссылки на соцсети с главной официального сайта: страница, в адресе которой есть название (слитно или два слова);
// нет такой — единственная ссылка этого типа (несколько чужих — партнёры, не берём)
const nrm = v => String(v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '');
const cut = n => String(n || '').split(/\\s+(?:in|at|@|im|en|na|w|à|au|del|della|am)\\s+/i)[0];
const compacts = [...new Set([st.NAME, cut(st.NAME), st.searchName, cut(st.searchName)].filter(Boolean).map(nrm).filter(x => x.length >= 4))];
const sl = [];
const reS = /href\\s*=\\s*["'](https?:\\/\\/(?:www\\.|m\\.)?(?:facebook|instagram|fb)\\.com\\/[^"']+)["']/gi;
let ms;
while ((ms = reS.exec(html))) { const p = socialPage(ms[1]); if (p && !sl.includes(p)) sl.push(p); }
const ownLink = p => compacts.some(c => socialHandle(p).includes(c)) || handleMatches(p, venueWords(st.NAME, 3));
const pickLink = test => { const a = sl.filter(p => test(host(p))); return a.find(ownLink) || (a.length === 1 ? a[0] : ''); };
const homeSocial = homeOk ? { facebook: pickLink(isFacebook), instagram: pickLink(isInstagram) } : {};
const home = homeOk ? { emails: extractEmails(html), hints: bookingHinted(html), ok: true, text: plain.slice(0, 3000), full: plain.slice(0, 60000), social: homeSocial }
  : { emails: [], hints: [], ok: false, text: '', full: '', social: {} };
return pages.map((url, i) => ({ json: i === 0 ? { url, st, home } : { url } }));`
    }
  }
});

const fetchContactPages = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Fetch Contact Pages',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'GET',
      url: expr('{{ $json.url }}'),
      sendHeaders: true,
      specifyHeaders: 'keypair',
      headerParameters: BROWSER_HEADERS,
      options: PAGE_OPTIONS
    }
  }
});

const collectSiteEmails = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Collect Site Emails',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + NAME_FN + `
const pages = $('Find Contact Pages').all();
const first = pages[0].json;
const st = first.st;
const h = st.siteHost;
// главная не открылась по https — ещё раз по http (бесплатно; 361grad.de: сбой TLS по https, по http открывается),
// не открылась и так — следующий шаг: Tavily extract этой главной
if (!first.home.ok) {
  if (/^https:/.test(st.siteUrl)) st.retryHttp = 'http://' + h + '/';
  else st.pendingExtract = h;
  st.siteUrl = ''; st.siteHost = '';
  return [{ json: st }];
}
const okPages = [], found = [];
let cpText = '';
// сначала страницы контактов, потом главная — источник берётся по первой странице, где нашёлся адрес
$input.all().forEach((it, i) => {
  const html = typeof it.json.data === 'string' ? it.json.data : '';
  const url = pages[i] ? pages[i].json.url : '';
  if (!(html.length > 500 && !/404|not found|nie znaleziono/i.test(html.slice(0, 2000)))) return;
  okPages.push(url);
  cpText += ' ' + decodeText(html).replace(/<script[\\s\\S]*?<\\/script>|<style[\\s\\S]*?<\\/style>/gi, ' ').replace(/<[^>]+>/g, ' ').slice(0, 20000);
  const hints = bookingHinted(html);
  // cp — адрес есть на странице контактов, но не на главной: указан в самом блоке контактов, а не в общем подвале сайта
  // (3ghub.ro/contact: teatru3g@gmail.com в «Contact Info», contact@3ghub.ro — в подвале каждой страницы)
  const homeEm = first.home.emails || [];
  extractEmails(html).forEach(e => found.push({ email: e, source: url, kind: 'site', hint: hints.includes(e), cp: !!homeEm.length && !homeEm.includes(e) }));
});
// сайт свой, если на нём название и город (rock-floor.de без «Brandenburg», bayonne.fr без «Zuzulu» — нет)
const chk = siteMatches(st, decodeText(first.home.full) + ' ' + cpText, h);
if (!chk.okName || !chk.okCity) {
  rejectSite(st, h, !chk.okName ? 'no name' : 'no city');
  st.siteUrl = ''; st.siteHost = ''; st.siteFrom = '';
  return [{ json: st }];
}
st.siteOk = true;
st.log.push('   site ' + h + ' OK (' + st.siteFrom + ')');
st.emails = st.emails.concat(found);
(first.home.emails || []).forEach(e => st.emails.push({ email: e, source: st.siteUrl, kind: 'site', hint: (first.home.hints || []).includes(e) }));
// соцсети со страницы самого сайта надёжнее выдачи поиска — заменяют найденное раньше
if (first.home.social) {
  if (first.home.social.facebook) st.facebook = first.home.social.facebook;
  if (first.home.social.instagram) st.instagram = first.home.social.instagram;
}
st.contactPages = st.contactPages.concat(okPages);
if (first.home.text) st.homeText = first.home.text;
return [{ json: st }];`
    }
  }
});

const tavilyExtract = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Tavily Extract',
    onError: 'continueRegularOutput',
    parameters: {
      method: 'POST',
      url: TAVILY_URL + '/extract',
      authentication: 'genericCredentialType',
      genericAuthType: 'httpHeaderAuth',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify($json.req) }}'),
      options: { timeout: 60000 }
    },
    credentials: TAVILY
  }
});

const collectExtractEmails = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Collect Extract Emails',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + NAME_FN + FATAL_FN + `
// Главная сайта, которая не открылась у n8n (защита от ботов, JavaScript), — через Tavily extract;
// принимается так же: название + город
const st = JSON.parse(JSON.stringify($('Plan Step').first().json));
delete st.action; delete st.req; delete st.notesPrompt;
const j = $input.first().json || {};
st.credits += 1;
const fatal = tavilyFatal(j.error);
if (fatal) { st.fatal = fatal; st.fatalError = String(j.error.message || j.error).slice(0, 200); }
const h = st.extractHost; st.extractHost = '';
const r = (Array.isArray(j.results) ? j.results : [])[0] || {};
const raw = String(r.raw_content || '');
if (!raw) st.log.push('   site ' + h + ': not loaded (extract too)');
else {
  const chk = siteMatches(st, raw, h);
  if (!chk.okName || !chk.okCity) rejectSite(st, h, (!chk.okName ? 'no name' : 'no city') + ', extract');
  else {
    st.siteOk = true; st.siteHost = h; st.siteUrl = 'https://' + h + '/';
    const c = st.siteCands.find(x => host(x.url) === h);
    st.siteFrom = (c ? c.from : '') + ', extract';
    st.log.push('   site ' + h + ' OK (' + st.siteFrom + ')');
    const hints = bookingHinted(raw);
    extractEmails(raw).forEach(e => st.emails.push({ email: e, source: r.url || st.siteUrl, kind: 'extract', hint: hints.includes(e) }));
    st.homeText = raw.replace(/\\s+/g, ' ').slice(0, 3000);
  }
}
return [{ json: st }];`
    }
  }
});

// NOTES: описание + жанры по всему собранному (бесплатно, локальная модель)
const ollamaNotes = languageModel({
  type: '@n8n/n8n-nodes-langchain.lmChatOllama',
  version: 1,
  config: {
    name: 'Ollama Notes Model',
    parameters: { model: 'gemma4:26b', options: { temperature: 0, topK: 1, numCtx: 8192, keepAlive: '15m', think: false } },
    credentials: OLLAMA
  }
});

const notesParser = outputParser({
  type: '@n8n/n8n-nodes-langchain.outputParserStructured',
  version: 1.3,
  config: {
    name: 'Notes JSON',
    parameters: {
      schemaType: 'fromJson',
      jsonSchemaExample: '{ "description": "an alternative concert club in a former shipyard hall in Gdańsk, Poland", "genres": ["punk", "metal", "indie rock"], "type": "CLUB" }'
    }
  }
});

const writeNotes = node({
  type: '@n8n/n8n-nodes-langchain.chainLlm',
  version: 1.9,
  config: {
    name: 'Write Notes',
    // модель не ответила — строка записывается без NOTES
    onError: 'continueRegularOutput',
    parameters: {
      promptType: 'define',
      text: expr('{{ $json.notesPrompt }}'),
      hasOutputParser: true,
      messages: {
        messageValues: [
          {
            type: 'SystemMessagePromptTemplate',
            message: `You write short notes about music venues for a booking agency's database. Return:
- description: one short English phrase WITHOUT a subject, starting with a lowercase article, like "an alternative concert club in a former shipyard hall in Gdańsk, Poland". Say what kind of place it is (club, bar, cultural centre, festival…) and what happens there; capacity if stated. Max 30 words. Describe the named venue itself, not the building or district it is in; for a festival or event series — the festival.
- genres: up to 5 lowercase music genres / directions the venue books (e.g. "punk", "metal", "indie rock", "techno", "jazz", "hip-hop", "folk", "experimental", "electronic"). Use the evidence: genres named in the texts, and artists or event names you recognise (infer their genres). Approximate is fine. If the place is mostly not about music (comedy, theatre), still list music genres if any concerts are mentioned, else an empty list. Never invent without any hint.
- type: exactly one of "FEST" (a festival or a recurring open-air / multi-day music event), "CLUB" (music club, live club, nightclub), "BAR" (bar, pub, café or restaurant that hosts concerts), "VENUE" (concert hall, theatre, arena, cultural centre, social centre / squat with a stage), "OTHER" (radio, promoter, agency, label, shop, anything else).
Use only the given texts and your knowledge of the artists mentioned in them.`
          }
        ]
      }
    },
    subnodes: { model: ollamaNotes, outputParser: notesParser }
  }
});


const buildResult = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Build Result',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: EXTRACT_FN + AGG_FN + NAME_FN + `const st = $('Plan Step').first().json;
const j = $input.first().json || {};
let nt = j.output;
if (!nt && j.text) { try { nt = JSON.parse(String(j.text).replace(/^\`\`\`(json)?|\`\`\`$/g, '')); } catch (e) {} }
nt = nt || {};
const JUNK_DOMAINS = /(^|\\.)(example\\.(com|org|net)|domain\\.com|email\\.com|yourdomain\\.com|yoursite\\.com|mysite\\.com|company\\.com|test\\.com|sentry\\.io|wixpress\\.com|wix\\.com|w3\\.org|schema\\.org|godaddy\\.com|squarespace\\.com|sentry-next\\.wixpress\\.com|facebookmail\\.com|instagram\\.com|facebook\\.com)$/;
// адреса плагинов и хостингов из кода сайтов (support@seopress.org, …@ovh.net)
const JUNK_HOST = /(^|\\.)(ovh|wix|wixpress|sentry|schedulista|mapado|seopress|wordpress|wpengine|elementor|godaddy|ionos|hostinger|squarespace|webflow|jimdo|strato|1and1)\\.[a-z.]+$/;
// адрес под ROT13 (защита от спама): xbagnxg@… → kontakt@…
const TLD_RE = /\\.(com|org|net|info|eu|de|fr|uk|co\\.uk|pl|pt|es|it|nl|be|ch|at|cz|sk|se|ro|lt|dk|no|fi|hu|gr|ie|lu|si|hr|rs|bg|ee|lv|io|me|cafe|club|bar|events?|live|music|art|studio|online|site|xyz)$/;
const rot13 = e => e.replace(/[a-z]/g, ch => String.fromCharCode((ch.charCodeAt(0) - 97 + 13) % 26 + 97));
const unrot = e => (!TLD_RE.test(e) && TLD_RE.test(rot13(e))) ? rot13(e) : e;
// служебные адреса, на которые не пишут: noreply@, mailer-daemon@ и т.п.
const JUNK_LOCAL = /^(name|your(name|email)?|email|user|username|someone|john\\.?doe|jan\\.?kowalski|example|test|no-?reply|do-?not-?reply|donotreply|mailer-daemon|postmaster|bounces?|notifications?)$/;
const FREE_MAIL = /(^|\\.)(gmail\\.com|googlemail\\.com|outlook\\.\\w+|hotmail\\.\\w+|live\\.\\w+|yahoo\\.\\w+|icloud\\.com|proton(mail)?\\.\\w+|pm\\.me|o2\\.pl|wp\\.pl|interia\\.pl|onet\\.pl|op\\.pl|gazeta\\.pl|tlen\\.pl|sapo\\.pt|gmx\\.\\w+|web\\.de|wanadoo\\.fr|orange\\.fr|free\\.fr|laposte\\.net|sfr\\.fr|libero\\.it|seznam\\.cz|t-online\\.de|autistici\\.org|riseup\\.net)$/;
const c = st;
const words = venueWords(c.NAME);

// Источники по надёжности: сайт (страница контактов первой; + Tavily extract), страницы сайта в выдаче,
// адреса «по названию» (каталоги, фрагменты), адреса с корня / «About» своих страниц соцсетей
const results = c.results || [];
const onSite = h => !!c.siteOk && !!c.siteHost && (h === c.siteHost || h.endsWith('.' + c.siteHost));
const fromSearchSite = results.filter(r => onSite(r.host)).flatMap(r => r.emails.map(e => ({ email: e, source: r.url, kind: 'site' })));
const byKind = k => c.emails.filter(e => e.kind === k);
const all = byKind('site').concat(byKind('extract'), fromSearchSite, byKind('named'), byKind('social'));
const siteHasEmails = all.some(x => x.kind === 'site' || x.kind === 'extract');
const via = siteHasEmails ? 'site' : (all.length ? all[0].kind : 'site');

const seen = new Map();
const kindOf = new Map();
const hinted = new Set();
const contactBlock = new Set();
for (const x0 of all) {
  const x = Object.assign({}, x0, { email: unrot(x0.email) });
  const [local, dom] = x.email.split('@');
  if (!dom || JUNK_DOMAINS.test(dom) || JUNK_HOST.test(dom) || JUNK_LOCAL.test(local) || !validEmail(x.email)) continue;
  if (!seen.has(x.email)) { seen.set(x.email, x.source); kindOf.set(x.email, x.kind); }
  if (x.hint) hinted.add(x.email);
  if (x.cp) contactBlock.add(x.email);
}
// Адрес с опечаткой в домене: из двух адресов с одинаковым именем и почти одинаковым доменом
// оставляем тот, где домен лучше совпадает с названием
function lev(a, b) {
  let prev = Array.from({ length: b.length + 1 }, (x, j) => j);
  for (let i = 1; i <= a.length; i++) {
    const cur = [i];
    for (let j = 1; j <= b.length; j++) cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    prev = cur;
  }
  return prev[b.length];
}
const hits = d => words.filter(w => hostLabel(d).includes(w)).length;
const list0 = [...seen.keys()];
for (const a of list0) for (const b of list0) {
  if (a === b || !seen.has(a) || !seen.has(b)) continue;
  const [la, da] = a.split('@'), [lb, db] = b.split('@');
  if (la !== lb || da === db || lev(da, db) > 2) continue;
  const drop = hits(da) < hits(db) ? a : (hits(db) < hits(da) ? b : (list0.indexOf(a) > list0.indexOf(b) ? a : b));
  seen.delete(drop);
}
// Сайт принят и на нём есть адреса — адреса с чужих доменов не берём; бесплатная почта остаётся
// (kontakt@livemusichall.de — другой клуб в Кёльне — у Live Music Hall Weiher)
// Подпись, склеенная с адресом: «contactenquiries@…», когда есть и «enquiries@…», — склейка, убираем
for (const e of [...seen.keys()]) {
  const [l, d] = e.split('@');
  const m = l.match(/^(contact|contacts|kontakt|email|e-mail|mail|tel|phone)(.+)$/);
  if (m && seen.has(m[2] + '@' + d)) seen.delete(e);
}
// Обрезанный во фрагменте адрес: «art.musicausse@…», когда есть полный «campag-art.musicausse@…»
for (const e of [...seen.keys()]) {
  const [l, d] = e.split('@');
  if ([...seen.keys()].some(o => o !== e && o.endsWith('@' + d) && /[-._]$/.test(o.split('@')[0].slice(0, -l.length)) && o.split('@')[0].endsWith(l))) seen.delete(e);
}
// Адреса, указанные на самом сайте, остаются при любом домене (someprodukt (at) wanadoo.fr на someprodukt.fr)
if (siteHasEmails) {
  for (const e of [...seen.keys()]) {
    const d = e.split('@')[1], k = kindOf.get(e);
    if (k !== 'site' && k !== 'extract' && !onSite(d) && !FREE_MAIL.test(d)) seen.delete(e);
  }
}
const hasOrgMail = [...seen.keys()].some(e => !FREE_MAIL.test(e.split('@')[1]));
function rank(e) {
  const [l, d] = e.split('@');
  let r = hinted.has(e) ? 0.5 : 3;  // подписан на странице как «booking / concerts / program»
  // букинг и программа — первыми (просьба клиента 2026-10-22): booking@, programming@, program@, programm@, bands@,
  // veranstaltung@, live@, concerts@, gigs@, music@, submission@, art@ и их варианты
  // (bokning@ / boka@ — «booking» по-шведски)
  if (/booking|bokning|^(program|veranstaltung|submission|concert|konzert|koncert)/.test(l) ||
    /^(book|boka|bands?|live|gigs?|music|musik|art)([._-]|\\d*$)/.test(l) ||
    /(^|[._-])(booking|bookings|concerts?|koncert[ya]?|konzerte?|gigs?)([._-]|$)/.test(l)) r = 0;
  else if (/^(promo|muzyka|musica|kultur|events?|eventos)/.test(l)) r = Math.min(r, 1);
  else if (/^(info|kontakt|contact|contacto|contato|geral|hello|hi|office|biuro|mail|general|ola)$/.test(l) || /^(info|kontakt|contact)/.test(l)) r = Math.min(r, 2);
  else if (/^(press|media|prensa|imprensa|jobs?|praca|careers?|rodo|gdpr|privacy|iod|dpo|faktur|factur|invoice|ksiegowosc|accounting|billing|newsletter|no-?reply|webmaster|admin|tickets?|ticketing|bilheteira|bilety|kassa|box-?office|vermietung|verwaltung|hausverwaltung|vorstand|buchhaltung|rechnung|datenschutz)/.test(l)) r = Math.max(r, 4);
  // Чужой домен — вниз списка (кроме адресов со страницы контактов самого сайта)
  const fromContactPage = (c.contactPages || []).includes(seen.get(e));
  if (via === 'site' && c.siteHost && !onSite(d) && !FREE_MAIL.test(d) && !fromContactPage) r += 5;
  // Адрес из блока контактов на странице контактов (не из подвала сайта) — сразу после booking@ и программы,
  // выше общих info@/contact@ и без понижения за бесплатную почту: его указала сама площадка
  // (замечание клиента 2026-10-20 по 3g HUB); служебные (press@, касса…) так не поднимаются
  if (contactBlock.has(e) && r <= 3) return Math.min(r, 1.5);
  // Бесплатная почта — ниже адресов организации той же группы
  if (hasOrgMail && FREE_MAIL.test(d)) r += 0.5;
  return r;
}
// Не больше 3 адресов — по одному в EMAIL 1–3 (просьба клиента 2026-10-22; раньше до 10, лишние — через запятую в EMAIL 3)
const emails = [...seen.keys()].map((e, i) => ({ e, i, r: rank(e) })).sort((a, b) => a.r - b.r || a.i - b.i).map(x => x.e).slice(0, 3);

// stopped — кредиты Tavily кончились посреди строки: результат неполный, строку не записываем
let status, error = '';
const mainFailed = c.searchErrors.some(x => x.startsWith('q1:')) && !results.length;
if (c.fatal) { status = 'stopped'; error = c.fatal + (c.fatalError ? ': ' + c.fatalError : ''); }
else if (mainFailed) { status = 'error'; error = 'search failed (' + c.searchErrors[0] + ')'; }
else if (emails.length) status = 'done';
else status = 'no email';

// WEB — страница официального сайта, где нашёлся e-mail (или главная); SOCIAL — Facebook и Instagram через запятую
const src = emails.length ? seen.get(emails[0]) : '';
const siteUrl = c.siteOk ? c.siteUrl : '';
const web = src && siteUrl && onSite(host(src)) ? src : siteUrl;
// SOCIAL: ссылки через пробел и пробел после каждой (включая последнюю) — так Google Таблицы делают кликабельной каждую
// (просьба клиента 2026-10-22; запятые мешали)
const social = [c.facebook, c.instagram].filter(Boolean).map(u => u + ' ').join('');
// NOTES: описание + жанры (для клиента главное — музыкальные направления)
// площадка не подтверждена и о ней (с этим городом) в выдаче ничего — описания нет, жанров нет
const desc = c.notesEmpty ? '' : String(nt.description || '').trim().replace(/[.\\s]+$/, '');
if (c.notesEmpty) { nt.genres = []; }
const genres = (Array.isArray(nt.genres) ? nt.genres : String(nt.genres || '').split(','))
  .map(g => String(g).trim().toLowerCase()).filter(g => g && g.length < 30).slice(0, 5);
// жанров нет — так и пишем (решение владельца 2026-10-20), чтобы при проверке было видно, что их искали;
// модель не ответила вовсе — без пометки
const answered = !!desc || Array.isArray(nt.genres);
const genresText = genres.length ? genres.join(', ') : (answered ? 'no music info found' : '');
const notes = [desc, genresText ? 'Genres: ' + genresText : ''].filter(Boolean).join('. ');
// SCRAPER NOTES — короткий лог по строке: исправления, отвергнутые сайты, откуда адрес, жанры
const sn = (c.scraperNotes || []).slice();
const KIND = { site: 'site', extract: 'site', named: 'named', social: 'social' };
if (emails.length) sn.push('email from ' + (KIND[kindOf.get(emails[0])] || 'site'));
// модель опознала площадку под другим названием, но исправление не принято (не похоже / по нему ничего не нашлось) —
// подсказка проверяющему вместо «ничего не найдено» (Musicalausse → «Le Clan Destin», Livernon)
const fx = c.fix || {};
const possible = fx.found === true && fx.name && !c.fixOk && nrm(fx.name) !== nrm(c.NAME) && nrm(fx.name) !== nrm(c.name)
  ? 'possible match: "' + fx.name + '" (' + (fx.city || c.CITY) + ')' : '';
// только для неподтверждённых строк: у найденных другое название модели («Valhalla Bar») — лишний шум
const confirmed = !!emails.length || !!social || !!siteUrl;
if (!confirmed) sn.push(possible || 'nothing found online — name may be misread');
// площадка не подтверждена (нет своего сайта, страницы соцсети, адреса) — NOTES написаны по чужим результатам поиска
if (!confirmed && desc) sn.push('notes from unconfirmed search results');
if (answered && !genres.length) sn.push('genres not found');
// TYPE (просьба клиента 2026-10-22: главное — отделять фестивали, именно «FEST» большими буквами):
// «fest» / «open air» в названии — FEST всегда; иначе тип от модели, только если площадка подтверждена
const TYPES = ['FEST', 'CLUB', 'BAR', 'VENUE', 'OTHER'];
const mt = String(nt.type || '').trim().toUpperCase();
const type = /fest|open ?air/i.test(c.NAME) ? 'FEST' : (confirmed && TYPES.includes(mt) ? mt : '');
if (c.searchErrors.length) sn.push('search error: ' + c.searchErrors[0].slice(0, 80));
const scraperNotes = [...new Set(sn)].join('; ');

return [{ json: {
  row_number: c.row_number,
  status, error, notes, scraperNotes, type, emails, social, web,
  credits: c.credits,
  dryRun: c.dryRun,
  debug: {
    log: c.log, via, name: c.name, city: c.city, fix: c.fix, siteUrl: c.siteUrl, siteOk: c.siteOk, siteFrom: c.siteFrom,
    siteCands: c.siteCands, siteRejected: c.siteRejected, socialPages: c.social,
    facebook: c.facebook, instagram: c.instagram, contactPages: c.contactPages, credits: c.credits, searchErrors: c.searchErrors,
    allEmails: c.emails.map(x => x.email + ' [' + x.kind + '] <- ' + x.source), notesRaw: nt
  }
} }];`
    }
  }
});

// Что делать со строкой: в базу + отметка в 03 / только отметка в 03 / ничего (dryRun).
// Из 03 строки никогда не удаляются (решение владельца 2026-10-20): отметка «processed …» в Automation Status.
// stop — кредиты Tavily кончились: строка остаётся в 03 с заметкой, проход прекращается.
const rowAction = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Row Action',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const s = $('Settings').first().json;
const row = $('Loop Rows').first().json;
const j = $input.first().json || {};
const out = { row_number: row.row_number, COUNTRY: row.COUNTRY, CITY: row.CITY, NAME: row.NAME,
  status: '', error: '', emails: 0, credits: 0, action: 'none', note: '', stop: false, redo: !!row.redo, baseRow: null };
if (row.mode === 'inBase') {
  out.status = 'in base'; out.action = 'note';
} else if (j.tavilyFatal) {
  out.status = 'stopped'; out.error = j.tavilyFatal + (j.tavilyError ? ': ' + j.tavilyError : '');
} else {
  out.status = j.status; out.error = j.error || ''; out.emails = (j.emails || []).length; out.credits = j.credits || 0;
  if (j.status === 'done' || j.status === 'no email') {
    out.action = 'toBase';
    const e = j.emails || [];
    out.baseRow = { COUNTRY: row.COUNTRY, CITY: row.CITY, NAME: row.NAME, NOTES: j.notes || '',
      TYPE: j.type || '',
      'EMAIL 1': e[0] || '', 'EMAIL 2': e[1] || '', 'EMAIL 3': e[2] || '',
      // дата и время записи (Прага) текстом «yyyy-MM-dd HH:mm:ss»: сортировка по столбцу = по времени,
      // и строки только с датой (до 2026-10-20) встают перед строками того же дня со временем
      SOCIAL: j.social || '', WEB: j.web || '', 'UPDATED AT': $now.setZone('Europe/Prague').toFormat('yyyy-MM-dd HH:mm:ss'),
      'SCRAPER NOTES': j.scraperNotes || '' };
  }
}
if (out.status === 'stopped') {
  out.stop = true; out.action = 'note';
  out.note = 'Tavily: ' + out.error.split(':')[0] + ' on ' + s.today + ' - will retry next run';
} else if (out.status === 'error') {
  out.action = 'note';
  out.note = 'Search error on ' + s.today + ': ' + out.error.slice(0, 150) + ' - will retry next run';
}
// обработанная строка — отметка в «Automation Status» (такие строки следующие запуски пропускают)
if (out.action === 'toBase' || out.status === 'in base') {
  out.note = 'processed ' + $now.setZone('Europe/Prague').toFormat('yyyy-MM-dd HH:mm') + ': ' + (out.status === 'in base' ? 'already in base' :
    (out.status === 'done' ? out.emails + ' email(s)' : 'no email') + (out.redo ? ', redone in base' : ', added to base'));
}
// redo, но поиск оборвался — «redo» остаётся в начале заметки, следующий запуск повторит
if (out.redo && (out.status === 'stopped' || out.status === 'error')) out.note = 'redo — ' + out.note;
if (s.dryRun) out.action = 'none';
return [{ json: out }];`
    }
  }
});

const toBase = ifElse({
  version: 2.2,
  config: {
    name: 'To Base?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'toBase', operator: { type: 'string', operation: 'equals' } }],
        combinator: 'and'
      }
    }
  }
});

const change03 = ifElse({
  version: 2.2,
  config: {
    name: 'Change 03?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.action }}'), rightValue: 'none', operator: { type: 'string', operation: 'notEquals' } }],
        combinator: 'and'
      }
    }
  }
});

// ── Запись строки в базу прямым запросом к Google Sheets API (с v011) ──
// Нужно ради кликабельных ссылок в SOCIAL: Google делает ссылки из нескольких адресов в одной ячейке только
// разметкой кусков текста (textFormatRuns) — для этого нужен номер записанной строки, а узел Google Sheets
// «append» его не возвращает. Credential Google с 2026-10-22 включён для HTTP-узлов (scope spreadsheets).
const SHEETS_API = 'https://sheets.googleapis.com/v4/spreadsheets/';

// Заголовки базы — столбцы ищутся по названиям (клиент добавляет свои столбцы)
const baseHeader = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Base Header',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    parameters: { method: 'GET', url: SHEETS_API + BASE_ID + '/values/%27' + BASE_TAB + '%27!1%3A1', authentication: 'predefinedCredentialType', nodeCredentialType: 'googleApi', options: { timeout: 30000 } },
    credentials: GOOGLE
  }
});

// Строка базы массивом по порядку столбцов; чужие столбцы — пустые (при дописывании)
const prepareBaseRow = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Prepare Base Row',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const a = $('Row Action').first().json;
const head = (($input.first().json.values || [])[0] || []).map(x => String(x || '').trim());
const need = Object.keys(a.baseRow || {});
const missing = need.filter(c => !head.includes(c));
// нет нашего столбца — не пишем вслепую, строка остаётся в 03 с ошибкой
if (!head.length || missing.length) throw new Error('MASTER BASE: нет столбцов ' + (head.length ? missing.join(', ') : '(заголовок не прочитан)'));
const rowValues = head.map(c => (a.baseRow[c] == null ? '' : String(a.baseRow[c])));
return [{ json: { ...a, head, rowValues, socialCol: head.indexOf('SOCIAL') } }];`
    }
  }
});

const appendBase = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Append to Base',
    // без повторов: если Google записал строку, но ответил ошибкой, повтор дал бы дубль в базе
    parameters: {
      method: 'POST',
      // INSERT_ROWS — новая строка вставляется после последней строки с данными, ничего под таблицей не перезаписывается
      url: SHEETS_API + BASE_ID + '/values/%27' + BASE_TAB + '%27!A1:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS',
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'googleApi',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify({ values: [$json.rowValues] }) }}'),
      options: { timeout: 30000 }
    },
    credentials: GOOGLE
  }
});

// ── redo: перезапись строки базы ──
// «redo» в Automation Status: строка ищется заново, результат пишется в ту же строку базы (по COUNTRY + CITY + NAME —
// в базе всегда исходное название из 03, исправленное — только в SCRAPER NOTES). Нет такой строки — дописывается.
const isRedoRow = ifElse({
  version: 2.2,
  config: {
    name: 'Redo?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.redo }}'), operator: { type: 'boolean', operation: 'true', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

// База читается заново прямо перед записью: номер строки должен быть свежим (владелец переставляет строки базы)
const rereadBase = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Re-read Base',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    executeOnce: true,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'read',
      documentId: { __rl: true, mode: 'id', value: BASE_ID },
      sheetName: BASE_SHEET,
      options: {}
    },
    credentials: GOOGLE
  }
});

const findBaseRow = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Find Base Row',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `// из Prepare Base Row — с готовой строкой (rowValues) на случай, если строки в базе нет и её надо дописать
const a = $('Prepare Base Row').first().json;
const S = v => String(v == null ? '' : v);
const norm = v => S(v).normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '');
const keyOf = r => norm(r.COUNTRY) + '|' + norm(r.CITY) + '|' + norm(r.NAME);
const key = keyOf(a);
// несколько одинаковых строк в базе — перезаписывается верхняя
const hits = $input.all().map(i => i.json).filter(r => r.row_number && keyOf(r) === key).sort((x, y) => x.row_number - y.row_number);
return [{ json: { ...a, baseRowNumber: hits.length ? hits[0].row_number : 0, baseMatches: hits.length } }];`
    }
  }
});

const baseRowFound = ifElse({
  version: 2.2,
  config: {
    name: 'Base Row Found?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.baseRowNumber }}'), rightValue: 0, operator: { type: 'number', operation: 'gt' } }],
        combinator: 'and'
      }
    }
  }
});

// COUNTRY / CITY / NAME не трогаем — по ним строка найдена
const updateBase = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Update Base Row',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'update',
      documentId: { __rl: true, mode: 'id', value: BASE_ID },
      sheetName: BASE_SHEET,
      columns: {
        mappingMode: 'defineBelow',
        value: {
          row_number: expr('{{ $json.baseRowNumber }}'),
          'NOTES': expr('{{ $json.baseRow["NOTES"] }}'),
          'TYPE': expr('{{ $json.baseRow["TYPE"] }}'),
          'SCRAPER NOTES': expr('{{ $json.baseRow["SCRAPER NOTES"] }}'),
          'EMAIL 1': expr('{{ $json.baseRow["EMAIL 1"] }}'),
          'EMAIL 2': expr('{{ $json.baseRow["EMAIL 2"] }}'),
          'EMAIL 3': expr('{{ $json.baseRow["EMAIL 3"] }}'),
          'SOCIAL': expr('{{ $json.baseRow["SOCIAL"] }}'),
          'WEB': expr('{{ $json.baseRow["WEB"] }}'),
          'UPDATED AT': expr('{{ $json.baseRow["UPDATED AT"] }}')
        },
        matchingColumns: ['row_number'],
        schema: [
          { id: 'NOTES', displayName: 'NOTES', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'TYPE', displayName: 'TYPE', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'SCRAPER NOTES', displayName: 'SCRAPER NOTES', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'EMAIL 1', displayName: 'EMAIL 1', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'EMAIL 2', displayName: 'EMAIL 2', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'EMAIL 3', displayName: 'EMAIL 3', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'SOCIAL', displayName: 'SOCIAL', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'WEB', displayName: 'WEB', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'UPDATED AT', displayName: 'UPDATED AT', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'row_number', displayName: 'row_number', required: false, defaultMatch: false, display: true, type: 'number', canBeUsedToMatch: true, readOnly: true, removed: false }
        ]
      },
      options: { cellFormat: 'RAW' }
    },
    credentials: GOOGLE
  }
});

// ── Кликабельные ссылки в SOCIAL (просьба клиента 2026-10-22) ──
// Номер строки: после дописывания — из ответа Google (updatedRange «'Sheet1'!A12280:W12280»), после redo — из Find Base Row.
// Каждая ссылка в ячейке размечается отдельно (как делает сам Google, когда ячейку правят руками)
const socialTarget = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Social Target',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const p = $('Prepare Base Row').first().json;
const j = $input.first().json || {};
let row = 0;
const ur = j.updates && j.updates.updatedRange;
if (ur) { const m = String(ur).match(/![A-Z]+(\\d+)/); if (m) row = Number(m[1]); }
else { try { row = Number($('Find Base Row').first().json.baseRowNumber) || 0; } catch (e) {} }
const text = String(p.baseRow.SOCIAL || '');
const runs = [];
let pos = 0;
for (const part of text.split(' ')) {
  if (part) { runs.push({ startIndex: pos, format: { link: { uri: part } } }); runs.push({ startIndex: pos + part.length, format: {} }); }
  pos += part.length + 1;
}
// конец текста — не начало куска (Google не принимает startIndex = длине текста)
const fixed = runs.filter(r => r.startIndex < text.length);
const ok = row > 1 && p.socialCol >= 0 && fixed.some(r => r.format.link);
const req = ok ? { requests: [{ updateCells: {
  range: { sheetId: ` + BASE_GID + `, startRowIndex: row - 1, endRowIndex: row, startColumnIndex: p.socialCol, endColumnIndex: p.socialCol + 1 },
  rows: [{ values: [{ userEnteredValue: { stringValue: text }, textFormatRuns: fixed }] }],
  fields: 'userEnteredValue,textFormatRuns' } }] } : null;
return [{ json: { link: ok, row, req } }];`
    }
  }
});

const hasLinks = ifElse({
  version: 2.2,
  config: {
    name: 'Has Links?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.link }}'), operator: { type: 'boolean', operation: 'true', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

// Не получилось — не беда: текст в ячейке уже записан, просто ссылки не кликабельны
const linkSocial = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.5,
  config: {
    name: 'Link Social',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    onError: 'continueRegularOutput',
    parameters: {
      method: 'POST',
      url: SHEETS_API + BASE_ID + ':batchUpdate',
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'googleApi',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify($json.req) }}'),
      options: { timeout: 30000 }
    },
    credentials: GOOGLE
  }
});

// 03 читаем заново перед отметкой: строка должна быть на своём месте (никто не сортировал / не удалял)
const reread03 = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Re-read New Data',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
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

// op: note — записать отметку в Automation Status; skip — строка сдвинулась, не трогаем
// (уже записанная в базу строка получит отметку следующим запуском как «есть в базе»)
const plan03 = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Plan 03 Change',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const a = $('Row Action').first().json;
const S = v => String(v == null ? '' : v).trim();
const fresh = new Map();
for (const r of $input.all().map(i => i.json)) if (r.row_number) fresh.set(r.row_number, r);
const f = fresh.get(a.row_number);
const op = !f || S(f.COUNTRY) !== a.COUNTRY || S(f.CITY) !== a.CITY || S(f.NAME) !== a.NAME || !a.note ? 'skip' : 'note';
return [{ json: { op, row_number: a.row_number, note: a.note } }];`
    }
  }
});

const isNote = ifElse({
  version: 2.2,
  config: {
    name: 'Note?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.op }}'), rightValue: 'note', operator: { type: 'string', operation: 'equals' } }],
        combinator: 'and'
      }
    }
  }
});

const writeNote = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.7,
  config: {
    name: 'Write Note',
    // сбой Google (500 / таймаут) — ещё 2 попытки через 5 с (запуск 2946: 500 на Re-read New Data оборвал прогон)
    retryOnFail: true,
    maxTries: 3,
    waitBetweenTries: 5000,
    alwaysOutputData: true,
    parameters: {
      authentication: 'serviceAccount',
      resource: 'sheet',
      operation: 'update',
      documentId: { __rl: true, mode: 'id', value: NEWDATA_ID },
      sheetName: NEW_SHEET,
      columns: {
        mappingMode: 'defineBelow',
        value: {
          row_number: expr('{{ $json.row_number }}'),
          'Automation Status': expr('{{ $json.note }}')
        },
        matchingColumns: ['row_number'],
        schema: [
          { id: 'Automation Status', displayName: 'Automation Status', required: false, defaultMatch: false, display: true, type: 'string', canBeUsedToMatch: true },
          { id: 'row_number', displayName: 'row_number', required: false, defaultMatch: false, display: true, type: 'number', canBeUsedToMatch: true, readOnly: true, removed: false }
        ]
      },
      options: { cellFormat: 'RAW' }
    },
    credentials: GOOGLE
  }
});

// Итог по строке (все ветки сходятся сюда)
const rowResult = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Row Result',
    executeOnce: true,
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      jsCode: `const a = $('Row Action').first().json;
const r = Object.assign({}, a, { baseRow: undefined, op: 'none' });
if (a.action !== 'none') r.op = $('Plan 03 Change').first().json.op;
r.inBase = a.action === 'toBase';
return [{ json: r }];`
    }
  }
});

const isStop = ifElse({
  version: 2.2,
  config: {
    name: 'Stop?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
        conditions: [{ leftValue: expr('{{ $json.stop }}'), operator: { type: 'boolean', operation: 'true', singleValue: true } }],
        combinator: 'and'
      }
    }
  }
});

const pause = node({
  type: 'n8n-nodes-base.wait',
  version: 1.1,
  config: { name: 'Pause', parameters: { resume: 'timeInterval', amount: 1, unit: 'seconds' } }
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
      jsCode: `const s = $('Settings').first().json;
const sel = $('Select Rows').first().json;
const res = [];
for (let k = 0; k < 100000; k++) {
  let a = [];
  try { a = $('Row Result').all(0, k); } catch (e) { break; }
  if (!a || !a.length) break;
  res.push(...a.map(i => i.json));
}
const esc = t => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const fmt = x => esc(x.COUNTRY + ' | ' + (x.CITY || '-') + ' | ' + x.NAME);
const n = f => res.filter(f).length;
const dry = s.dryRun;
const withEmail = n(r => r.status === 'done'), noEmail = n(r => r.status === 'no email');
const inBase = n(r => r.status === 'in base'), errors = res.filter(r => r.status === 'error');
const stopped = res.find(r => r.status === 'stopped');
const moved = n(r => r.op === 'skip');
const spent = res.reduce((a, r) => a + (r.credits || 0), 0);
const lines = ['<b>Tour Venues: поиск контактов</b>' + (dry ? ' (тестовый режим, без записи)' : '')];
// из 03 ничего не удаляется: обработанные строки получают отметку «processed …» в Automation Status
lines.push('Необработанных строк в 03_OUT_newData: ' + sel.total + ', обработано сейчас: ' + res.length);
if (sel.total) {
  lines.push((dry ? 'Было бы в' : 'В') + ' MASTER BASE с e-mail: ' + withEmail);
  lines.push((dry ? 'Было бы в' : 'В') + ' MASTER BASE без e-mail (только ссылки): ' + noEmail);
  const redone = n(r => r.redo && r.inBase);
  if (redone) lines.push('Из них заново по «redo» (строка базы ' + (dry ? 'была бы перезаписана' : 'перезаписана') + '): ' + redone);
  if (inBase) lines.push('Уже были в базе (не искали, ' + (dry ? 'были бы отмечены' : 'отмечены') + ' в 03): ' + inBase);
  if (errors.length) {
    lines.push('Ошибка поиска, остались в 03 (повтор в следующий раз): ' + errors.length);
    for (const e of errors.slice(0, 5)) lines.push('• ' + fmt(e) + ' — <i>' + esc(e.error.slice(0, 120)) + '</i>');
  }
  if (moved) lines.push('⚠️ Строки в 03 сдвинулись во время работы, отметка не поставлена: ' + moved + ' (поставится следующим запуском)');
}
const WHY = { 'credits exhausted': 'кредиты закончились', 'API key rejected': 'ключ API не принят', 'rate limit': 'слишком частые запросы, подождите' };
if (stopped) {
  const why = stopped.error.split(':')[0];
  lines.push('', '⚠️ <b>Tavily: ' + esc(WHY[why] || why) + '</b> — проход остановлен на ' + fmt(stopped) + '. Строка осталась в 03, следующий запуск продолжит с неё.');
}
if (sel.waiting && !stopped) lines.push('Ждут следующего запуска: ' + sel.waiting + ' (ограничение maxRows = ' + s.maxRows + ')');
lines.push('Кредитов Tavily потрачено ≈ ' + spent);
return [{ json: { text: lines.join('\\n'), total: sel.total, processed: res.length, withEmail, noEmail, inBase, errors: errors.length,
  stopped: !!stopped, spent, details: res } }];`
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

export default workflow('tourvenues-10', 'TourVenues_10_venue_email_finder_v011')
  .add(start)
  .to(settings)
  .add(fromBot)
  .to(settings)
  .to(readNewData)
  .to(readBase)
  .to(selectRows)
  .to(hasRows
    .onTrue(splitRows.to(loopRows
      .onDone(report)
      .onEachBatch(isInBase
        .onTrue(rowAction)
        .onFalse(initRow.to(planStep)))))
    .onFalse(report))
  .add(planStep)
  .to(route
    .onCase(0, tavilySearch.to(harvest).to(askModel
      .onTrue(pickSources.to(mergePick).to(planStep))
      .onFalse(planStep)))
    .onCase(1, fetchHomepage.to(findContactPages).to(fetchContactPages).to(collectSiteEmails).to(planStep))
    .onCase(2, tavilyExtract.to(collectExtractEmails).to(planStep))
    .onCase(3, writeNotes.to(buildResult).to(rowAction)))
  .add(rowAction)
  .to(toBase
    .onTrue(baseHeader.to(prepareBaseRow).to(isRedoRow
      .onTrue(rereadBase.to(findBaseRow).to(baseRowFound
        .onTrue(updateBase.to(socialTarget))
        .onFalse(appendBase)))
      .onFalse(appendBase.to(socialTarget.to(hasLinks
        .onTrue(linkSocial.to(reread03))
        .onFalse(reread03))))))
    .onFalse(change03
      .onTrue(reread03)
      .onFalse(rowResult)))
  .add(reread03)
  .to(plan03)
  .to(isNote
    .onTrue(writeNote.to(rowResult))
    .onFalse(rowResult))
  .add(rowResult)
  .to(isStop
    .onTrue(report)
    .onFalse(pause.to(nextBatch(loopRows))))
  .add(report)
  .to(recipients)
  .to(telegram);
