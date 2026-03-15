const DATA_SHEET_ID = '1j165dsa1a-DDapOCgyBLrJQ_UBa4LzCWdWez4_obLD0';
const ACTUALS_TAB   = 'Actuals_DummyData';
const TARGETS_TAB   = 'Capex Targets By Resource';
const CACHE_KEY     = 'capex_data_v2';
const CACHE_TTL     = 21600; // 6 hours

// ── Entry point ────────────────────────────────────────────────────────────────
function doGet() {
  try {
    const data = getCapexData();
    return ContentService
      .createTextOutput(JSON.stringify(data))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (e) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: e.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ── Called from client via google.script.run.getCapexData() ───────────────────
// Returns pre-aggregated compact data instead of raw rows, dramatically
// reducing payload size and eliminating client-side heavy processing.
function getCapexData() {
  const cache  = CacheService.getScriptCache();
  const cached = getChunks_(cache);
  if (cached) return JSON.parse(cached);

  const ss = SpreadsheetApp.openById(DATA_SHEET_ID);

  // ── Read targets first (small table) ────────────────────────────────────────
  const targetsSheet = ss.getSheetByName(TARGETS_TAB);
  if (!targetsSheet) throw new Error('Tab not found: ' + TARGETS_TAB);
  const tVals    = targetsSheet.getDataRange().getValues();
  const targetsMap = {};
  for (let i = 1; i < tVals.length; i++) {
    const name = String(tVals[i][0] || '').trim();
    if (!name) continue;
    let raw = String(tVals[i][1] || '').trim();
    let val;
    if (raw.endsWith('%')) val = parseFloat(raw) / 100;
    else { val = parseFloat(raw); if (val > 1) val /= 100; }
    targetsMap[name] = isNaN(val) ? null : val;
  }

  // ── Read actuals and pre-aggregate to minimum grain ─────────────────────────
  // Grain: (year, month, l4, l5, l6, dm, resource) → { capex_hrs, opex_hrs }
  // This collapses thousands of raw rows into a compact summary, reducing the
  // client payload by ~80% and eliminating per-row processing in the browser.
  const actualsSheet = ss.getSheetByName(ACTUALS_TAB);
  if (!actualsSheet) throw new Error('Tab not found: ' + ACTUALS_TAB);
  const vals    = actualsSheet.getDataRange().getValues();
  const headers = vals[0].map(String);

  function col(name) { return headers.indexOf(name); }
  const iL4   = col('Level 4 Mgr');
  const iL5   = col('Level 5 Mgr');
  const iL6   = col('Level 6 Mgr');
  const iDM   = col('Direct Manager');
  const iRes  = col('Resource Name');
  const iHrs  = col('Worklog Hours');
  const iType = col('CapEx/OpEx');
  const iYr   = col('Year of Worklog');
  const iMo   = col('Month of Worklog');

  const agg  = {};
  const years = new Set(), months = new Set(), l4Set = new Set(), l5Set = new Set();

  const SEP = '\x01'; // non-printable separator for keys
  for (let i = 1; i < vals.length; i++) {
    const row = vals[i];
    const res = String(row[iRes] || '').trim();
    if (!res) continue;

    const l4  = String(row[iL4]  || '').trim() || '(No L4)';
    const l5  = String(row[iL5]  || '').trim() || '(No L5)';
    const l6  = String(row[iL6]  || '').trim() || '(No L6)';
    const dm  = String(row[iDM]  || '').trim() || '(No DM)';
    const yr  = String(row[iYr]  || '').trim();
    const mo  = String(row[iMo]  || '').trim();
    const hrs = parseFloat(row[iHrs]) || 0;
    const cap = String(row[iType] || '') === 'CapEx';

    const key = [yr, mo, l4, l5, l6, dm, res].join(SEP);
    if (!agg[key]) agg[key] = { yr, mo, l4, l5, l6, dm, res, c: 0, o: 0 };
    if (cap) agg[key].c += hrs;
    else     agg[key].o += hrs;

    if (yr) years.add(yr);
    if (mo) months.add(mo);
    l4Set.add(l4); l5Set.add(l5);
  }

  const MONTH_ORDER = ['January','February','March','April','May','June',
                       'July','August','September','October','November','December'];

  const result = {
    rows: Object.values(agg),   // compact summary rows
    targetsMap,
    fo: {                        // filter options (pre-computed)
      years:  [...years].sort(),
      months: MONTH_ORDER.filter(m => months.has(m)),
      l4s:    [...l4Set].filter(v => v !== '(No L4)').sort(),
      l5s:    [...l5Set].filter(v => v !== '(No L5)').sort()
    }
  };

  putChunks_(cache, JSON.stringify(result));
  return result;
}

// ── Cache helpers ──────────────────────────────────────────────────────────────
function putChunks_(cache, json) {
  try {
    const CHUNK = 90000;
    const total = Math.ceil(json.length / CHUNK);
    const pairs = { '__capex_chunks__': String(total) };
    for (let i = 0; i < total; i++) {
      pairs[CACHE_KEY + '_' + i] = json.slice(i * CHUNK, (i + 1) * CHUNK);
    }
    cache.putAll(pairs, CACHE_TTL);
  } catch (e) { console.log('Cache write failed:', e); }
}

function getChunks_(cache) {
  try {
    const meta = cache.get('__capex_chunks__');
    if (!meta) return null;
    const total  = parseInt(meta);
    const keys   = Array.from({ length: total }, (_, i) => CACHE_KEY + '_' + i);
    const stored = cache.getAll(keys);
    if (Object.keys(stored).length !== total) return null;
    return keys.map(k => stored[k]).join('');
  } catch (e) { return null; }
}

// ── Utilities (run from editor) ────────────────────────────────────────────────
function clearCapexCache() {
  const cache = CacheService.getScriptCache();
  cache.remove('__capex_chunks__');
  Logger.log('Cache cleared.');
}

function warmCache() {
  clearCapexCache();
  getCapexData();
  Logger.log('Cache warmed at ' + new Date().toLocaleString());
}

function createWarmCacheTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'warmCache')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('warmCache').timeBased().everyHours(4).create();
  Logger.log('Warm-cache trigger created.');
}

function testDataAccess() {
  clearCapexCache();
  const data = getCapexData();
  Logger.log('Summary rows: '     + data.rows.length);
  Logger.log('Target resources: ' + Object.keys(data.targetsMap).length);
  Logger.log('Filter options: '   + JSON.stringify(data.fo));
}

// ── Run once from Script Editor to give each resource a realistic CapEx split ─
// Rewrites the CapEx/OpEx column so each resource's rows reflect their target %
// (with ±15% random noise), making the split bars show meaningful variance.
// This replaces the old random-per-row assignment that caused every bar to
// look ~50/50 due to the law of large numbers.
function seedCapexAffinityByTarget() {
  const ss           = SpreadsheetApp.openById(DATA_SHEET_ID);
  const actualsSheet = ss.getSheetByName(ACTUALS_TAB);
  const targetsSheet = ss.getSheetByName(TARGETS_TAB);
  if (!actualsSheet) throw new Error('Tab not found: ' + ACTUALS_TAB);
  if (!targetsSheet) throw new Error('Tab not found: ' + TARGETS_TAB);

  // Build targets map (resource → 0–1 float)
  const tVals = targetsSheet.getDataRange().getValues();
  const targetsMap = {};
  for (let i = 1; i < tVals.length; i++) {
    const name = String(tVals[i][0] || '').trim();
    if (!name) continue;
    let raw = String(tVals[i][1] || '').trim();
    let val;
    if (raw.endsWith('%')) val = parseFloat(raw) / 100;
    else { val = parseFloat(raw); if (val > 1) val /= 100; }
    if (!isNaN(val)) targetsMap[name] = val;
  }

  // Read actuals
  const allVals = actualsSheet.getDataRange().getValues();
  const headers = allVals[0].map(String);
  const resIdx  = headers.indexOf('Resource Name');
  const typeIdx = headers.indexOf('CapEx/OpEx');
  if (resIdx  === -1) throw new Error('"Resource Name" column not found');
  if (typeIdx === -1) throw new Error('"CapEx/OpEx" column not found');

  // Group row indices by resource
  const resRows = {}; // resource → [row indices in allVals]
  for (let i = 1; i < allVals.length; i++) {
    const res = String(allVals[i][resIdx] || '').trim();
    if (!res) continue;
    if (!resRows[res]) resRows[res] = [];
    resRows[res].push(i);
  }

  // Assign CapEx/OpEx per resource based on target with ±15pt noise
  // Resources without a target get ~50% CapEx
  const newTypeCol = allVals.slice(1).map(r => [String(r[typeIdx] || '')]);

  Object.entries(resRows).forEach(([res, indices]) => {
    let baseRate = targetsMap[res] !== undefined ? targetsMap[res] : 0.50;
    // Apply small noise per-resource (±15%), clamped 0.10–0.90
    const noise    = (Math.random() - 0.5) * 0.30;
    const capRate  = Math.min(0.90, Math.max(0.10, baseRate + noise));

    // Shuffle and assign: first capRate% as CapEx, rest as OpEx
    // Use deterministic per-row decision based on position
    indices.forEach((rowIdx, pos) => {
      const isCapex = pos < Math.round(indices.length * capRate);
      newTypeCol[rowIdx - 1] = [isCapex ? 'CapEx' : 'OpEx'];
    });
  });

  // Write updated column back
  actualsSheet.getRange(2, typeIdx + 1, newTypeCol.length, 1).setValues(newTypeCol);
  clearCapexCache();

  const summary = Object.entries(resRows).map(([res, indices]) => {
    const target = targetsMap[res] !== undefined ? (targetsMap[res] * 100).toFixed(0) + '%' : 'no target';
    return `  ${res}: target=${target}, rows=${indices.length}`;
  }).join('\n');
  Logger.log('Done! Seeded CapEx affinity for ' + Object.keys(resRows).length +
             ' resources.\n' + summary + '\nCache cleared — reload the web app.');
}
