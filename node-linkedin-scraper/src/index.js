import fs from 'fs';
import path from 'path';
import process from 'process';
import axios from 'axios';
import xlsx from 'xlsx';
import { chromium } from 'playwright';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { createHash } from 'crypto';

const BASE_URL = 'https://www.linkedin.com';
const LOGIN_URL = 'https://www.linkedin.com/login';
const FEED_URL = 'https://www.linkedin.com/feed/';
const SEARCH_URL_TMPL = 'https://www.linkedin.com/search/results/content/?keywords={query}&origin=GLOBAL_SEARCH_HEADER';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sleepRandom(minMs = 1200, maxMs = 3000) {
  const ms = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
  return sleep(ms);
}

function normalizeWhitespace(text) {
  if (!text) return text;
  return text.split(/\s+/).join(' ').trim();
}

function ensureFullUrl(href) {
  if (!href) return null;
  if (href.startsWith('http://') || href.startsWith('https://')) return href;
  if (href.startsWith('/')) return new URL(href, BASE_URL).toString();
  return href;
}

function extractProfileId(url) {
  try {
    if (!url) return null;
    const u = new URL(url);
    const segments = u.pathname.split('/').filter(Boolean);
    if (!segments.length) return null;
    if ((segments[0] === 'in' || segments[0] === 'company') && segments.length >= 2) return segments[1];
    return segments[0];
  } catch {
    return null;
  }
}

function sha1(str) {
  return createHash('sha1').update(str).digest('hex');
}

async function loginWithCredentials(page, email, password) {
  try {
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await sleepRandom(500, 1200);
    await page.fill('#username', email, { timeout: 15000 });
    await sleepRandom(100, 400);
    await page.fill('#password', password, { timeout: 15000 });
    await sleepRandom(100, 400);
    await page.click("button[type='submit']", { timeout: 15000 });
    try {
      await page.waitForURL('**/feed/**', { timeout: 30000 });
      return true;
    } catch {
      try { await page.waitForURL('**/checkpoint/**', { timeout: 20000 }); } catch {}
      try {
        await page.waitForSelector('nav', { timeout: 15000 });
        return true;
      } catch {
        return false;
      }
    }
  } catch {
    return false;
  }
}

async function ensureLoggedIn(context, { email, password, headless }) {
  const page = await context.newPage();
  try {
    await page.goto(FEED_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    if (!page.url().includes('login')) return true;
  } catch {}

  if (email && password) {
    const ok = await loginWithCredentials(page, email, password);
    if (ok) return true;
  }

  if (!headless) {
    try {
      await page.waitForURL('**/feed/**', { timeout: 120000 });
      return true;
    } catch {
      return false;
    }
  }
  return false;
}

async function firstText(scope, selector) {
  try {
    const el = scope.locator(selector).first();
    const count = await el.count();
    if (count === 0) return null;
    const txt = normalizeWhitespace(await el.innerText());
    return txt || null;
  } catch {
    return null;
  }
}

async function firstHref(scope, selector) {
  try {
    const el = scope.locator(selector).first();
    const count = await el.count();
    if (count === 0) return null;
    const href = await el.getAttribute('href');
    return ensureFullUrl(href);
  } catch {
    return null;
  }
}

async function extractPostFields(postEl) {
  const post_text = (await firstText(postEl, "div[dir='ltr']"))
    || (await firstText(postEl, "span[dir='ltr']"))
    || (await firstText(postEl, 'p'))
    || null;

  const poster_profile_url = (await firstHref(postEl, "a[href*='/in/']"))
    || (await firstHref(postEl, "a[href*='linkedin.com/in/']"));
  const poster_name = (await firstText(postEl, "a[href*='/in/']"))
    || (await firstText(postEl, 'span.feed-shared-actor__name'))
    || null;

  const post_url = (await firstHref(postEl, "a[href*='/posts/']"))
    || (await firstHref(postEl, "a[href*='/activity/']"))
    || (await firstHref(postEl, "a[href*='/feed/update/urn:']"));

  const timestamp_text = (await firstText(postEl, 'time'))
    || (await firstText(postEl, 'span.update-components-actor__sub-description'))
    || null;

  return {
    post_text: post_text ? normalizeWhitespace(post_text) : null,
    poster_name: poster_name ? normalizeWhitespace(poster_name) : null,
    poster_profile_url: poster_profile_url ? ensureFullUrl(poster_profile_url) : null,
    post_url: post_url ? ensureFullUrl(post_url) : null,
    timestamp_text: timestamp_text ? normalizeWhitespace(timestamp_text) : null,
  };
}

async function scrollAndCollectPosts(page, maxPosts) {
  const collected = [];
  const seen = new Set();
  let stagnant = 0;
  let lastHeight = 0;

  while (collected.length < maxPosts && stagnant < 5) {
    try { await page.waitForSelector('article', { timeout: 8000 }); } catch {}
    const articles = page.locator('article');
    const count = await articles.count();
    for (let i = 0; i < count && collected.length < maxPosts; i++) {
      const el = articles.nth(i);
      const data = await extractPostFields(el);
      const key = JSON.stringify(data);
      if (seen.has(key)) continue;
      seen.add(key);
      collected.push(data);
    }

    await page.mouse.wheel(0, 2000);
    await sleepRandom(800, 1600);
    try {
      const h = await page.evaluate(() => document.body.scrollHeight);
      if (h === lastHeight) stagnant += 1; else { stagnant = 0; lastHeight = h; }
    } catch {}
  }
  return collected;
}

async function searchPosts(page, query, maxPosts) {
  const url = SEARCH_URL_TMPL.replace('{query}', encodeURIComponent(query));
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await sleepRandom(1200, 2200);
  try {
    const postsTab = page.getByRole('link', { name: 'Posts' });
    if (await postsTab.count()) {
      await postsTab.first().click({ timeout: 5000 });
      await sleepRandom(1000, 1800);
    }
  } catch {}
  return await scrollAndCollectPosts(page, maxPosts);
}

async function ollamaExtract(baseUrl, model, text) {
  const prompt = [
    'You extract hiring info from LinkedIn posts.',
    'Return strictly minified JSON only, no code fences or prose.',
    'Fields: role_title (string), min_years_experience (int), max_years_experience (int), skills (array of strings), location (string), job_type (full-time/part-time/intern/contract), contact (string), verdict_relevant (boolean: true only if role is Data Analyst or very close AND total experience required fits 0-2 years).',
    'If unsure about a field, use null, except skills should be [].',
    "Examples of relevant: 'Looking for a Data Analyst (freshers welcome)', 'Hiring Junior Data Analyst, 0-2 yrs'.",
    "Examples of NOT relevant: 'Senior Data Scientist 5+ years', 'Business Analyst 3-5 years'.",
    '',
    `Text: """${text}"""`,
    'Respond with a single JSON object only.'
  ].join('\n');

  try {
    const resp = await axios.post(`${baseUrl.replace(/\/$/, '')}/api/generate`, {
      model,
      prompt,
      options: { temperature: 0.1 },
      stream: false
    }, { timeout: 60000 });

    let raw = (resp.data?.response || '').trim();
    if (raw.startsWith('```json') || raw.startsWith('```')) raw = raw.replace(/^```[a-z]*\n?|```$/g, '').trim();
    try { return JSON.parse(raw); } catch {
      const start = raw.indexOf('{');
      const end = raw.lastIndexOf('}');
      if (start !== -1 && end !== -1 && end > start) return JSON.parse(raw.slice(start, end + 1));
      return null;
    }
  } catch {
    return null;
  }
}

function loadExistingExcel(filePath) {
  if (!fs.existsSync(filePath)) return [];
  try {
    const wb = xlsx.readFile(filePath);
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(ws, { defval: '' });
    return rows;
  } catch {
    return [];
  }
}

function appendAndSaveExcel(filePath, newRows) {
  const EXPECTED_COLUMNS = [
    'timestamp', 'post_url', 'poster_name', 'poster_profile_url', 'poster_linkedin_id',
    'role_title', 'min_years_experience', 'max_years_experience', 'skills', 'location', 'job_type', 'contact', 'post_excerpt'
  ];
  const existing = loadExistingExcel(filePath);
  const combined = [];
  const seen = new Set();

  function key(r) { return `${r.post_url || ''}|${r.post_excerpt || ''}`; }

  for (const row of [...existing, ...newRows]) {
    const normalized = Object.fromEntries(EXPECTED_COLUMNS.map(c => [c, row[c] ?? '']));
    const k = key(normalized);
    if (seen.has(k)) continue;
    seen.add(k);
    combined.push(normalized);
  }

  const ws = xlsx.utils.json_to_sheet(combined, { header: EXPECTED_COLUMNS });
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, 'Sheet1');
  xlsx.writeFile(wb, filePath);
}

async function runScrape({ query, maxPosts, output, storageStatePath, headless, email, password, ollamaUrl, ollamaModel }) {
  const browser = await chromium.launch({ headless });
  let context;
  if (fs.existsSync(storageStatePath)) {
    context = await browser.newContext({ storageState: storageStatePath, viewport: { width: 1366, height: 900 } });
  } else {
    context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  }

  const ok = await ensureLoggedIn(context, { email, password, headless });
  if (!ok) {
    console.error('[ERROR] Not logged in. Use --login first or provide --email/--password.');
    await context.close(); await browser.close();
    process.exit(1);
  }
  try { await context.storageState({ path: storageStatePath }); } catch {}

  const page = await context.newPage();
  const results = await searchPosts(page, query, maxPosts);
  console.log(`[INFO] Collected ${results.length} raw posts; sending to LLM for filtering/extraction...`);

  const kept = [];
  for (let i = 0; i < results.length; i++) {
    const raw = results[i];
    const text = (raw.post_text || '').trim();
    if (!text) continue;

    const extraction = await ollamaExtract(ollamaUrl, ollamaModel, text);
    let relevant = false;
    let extracted = {
      role_title: null, min_years_experience: null, max_years_experience: null,
      skills: [], location: null, job_type: null, contact: null, verdict_relevant: false
    };
    if (extraction && typeof extraction === 'object') {
      extracted = Object.assign(extracted, extraction);
      relevant = !!extracted.verdict_relevant;
    } else {
      const t = text.toLowerCase();
      const hasRole = t.includes('data analyst') || t.includes('junior data analyst');
      const hasExp = ['0-2', '0 to 2', 'freshers', 'fresher', 'entry level', 'junior'].some(m => t.includes(m));
      relevant = hasRole && hasExp;
      if (relevant) {
        extracted.role_title = 'Data Analyst';
        extracted.min_years_experience = 0;
        extracted.max_years_experience = 2;
        extracted.verdict_relevant = true;
      }
    }
    if (!relevant) continue;

    const posterId = extractProfileId(raw.poster_profile_url);
    const excerpt = raw.post_text && raw.post_text.length > 500 ? raw.post_text.slice(0, 497) + '...' : raw.post_text;
    kept.push({
      timestamp: raw.timestamp_text || '',
      post_url: raw.post_url || '',
      poster_name: raw.poster_name || '',
      poster_profile_url: raw.poster_profile_url || '',
      poster_linkedin_id: posterId || '',
      role_title: extracted.role_title || '',
      min_years_experience: extracted.min_years_experience ?? '',
      max_years_experience: extracted.max_years_experience ?? '',
      skills: Array.isArray(extracted.skills) && extracted.skills.length ? extracted.skills.join(', ') : '',
      location: extracted.location || '',
      job_type: extracted.job_type || '',
      contact: extracted.contact || '',
      post_excerpt: excerpt || ''
    });

    if ((i + 1) % 5 === 0) await sleepRandom(800, 1500);
  }

  if (kept.length) {
    appendAndSaveExcel(output, kept);
    console.log(`[OK] Wrote ${kept.length} records to ${output}`);
  } else {
    console.log('[INFO] No relevant posts found based on the criteria.');
  }

  await context.close();
  await browser.close();
}

async function runLoginOnly({ storageStatePath, email, password, headless }) {
  const browser = await chromium.launch({ headless });
  const context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  const page = await context.newPage();

  let success = false;
  if (email && password) success = await loginWithCredentials(page, email, password);

  if (!success) {
    if (headless) {
      console.error('[ERROR] Headless login failed and manual login not possible in headless mode.');
      await context.close(); await browser.close();
      process.exit(1);
    }
    console.log('[ACTION] Please complete login in the opened browser window. Waiting up to 2 minutes...');
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    try { await page.waitForURL('**/feed/**', { timeout: 120000 }); success = true; } catch { success = false; }
  }

  if (success) {
    try { await context.storageState({ path: storageStatePath }); } catch {}
    console.log(`[OK] Saved login session to ${storageStatePath}`);
  } else {
    console.error('[ERROR] Login not completed.');
    process.exit(1);
  }

  await context.close();
  await browser.close();
}

async function main() {
  const argv = yargs(hideBin(process.argv))
    .option('query', { type: 'string', default: 'Data Analyst hiring' })
    .option('max-posts', { type: 'number', default: 40 })
    .option('output', { type: 'string', default: 'linkedin_data_analyst_posts.xlsx' })
    .option('storage-state', { type: 'string', default: 'storage_state.json' })
    .option('login', { type: 'boolean', default: false })
    .option('headless', { type: 'boolean', default: true })
    .option('email', { type: 'string', default: process.env.LINKEDIN_EMAIL || '' })
    .option('password', { type: 'string', default: process.env.LINKEDIN_PASSWORD || '' })
    .option('ollama-url', { type: 'string', default: process.env.OLLAMA_HOST || 'http://localhost:11434' })
    .option('ollama-model', { type: 'string', default: process.env.OLLAMA_MODEL || 'llama3' })
    .help()
    .parse();

  const opts = {
    query: argv.query,
    maxPosts: argv['max-posts'],
    output: path.resolve(process.cwd(), argv.output),
    storageStatePath: path.resolve(process.cwd(), argv['storage-state']),
    headless: !!argv.headless,
    email: argv.email || undefined,
    password: argv.password || undefined,
    ollamaUrl: argv['ollama-url'],
    ollamaModel: argv['ollama-model'],
  };

  if (argv.login) {
    await runLoginOnly({ storageStatePath: opts.storageStatePath, email: opts.email, password: opts.password, headless: opts.headless });
    return;
  }

  await runScrape(opts);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});