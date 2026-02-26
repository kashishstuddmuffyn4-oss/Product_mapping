import streamlit as st
import pandas as pd
import time
import random
import threading
import queue
import io
from datetime import datetime

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Product Name Scraper",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;600;800&display=swap');

*, *::before, *::after { box-sizing: border-box; }

html, body, [data-testid="stAppViewContainer"] {
    background: #0a0a0f;
    color: #e2e8f0;
    font-family: 'Syne', sans-serif;
}
[data-testid="stAppViewContainer"] { background: #0a0a0f; }
[data-testid="stHeader"] { background: transparent; }

.hero-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 2.8rem;
    background: linear-gradient(135deg, #a78bfa, #38bdf8, #34d399);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: -1px;
    margin: 0;
    line-height: 1.1;
}
.hero-sub {
    color: #64748b;
    font-size: 0.95rem;
    margin-top: 0.4rem;
    letter-spacing: 0.05em;
}
.section-label {
    font-size: 0.68rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #334155;
    margin-bottom: 0.8rem;
    font-family: 'JetBrains Mono', monospace;
    border-left: 2px solid #a78bfa;
    padding-left: 0.6rem;
}
.stats-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.9rem;
    margin-bottom: 1rem;
}
.stat-card {
    background: #13131a;
    border: 1px solid #1e1e2e;
    border-radius: 12px;
    padding: 1.1rem;
    text-align: center;
}
.stat-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.9rem;
    font-weight: 700;
    line-height: 1;
}
.stat-label {
    font-size: 0.68rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #475569;
    margin-top: 0.4rem;
}
.stat-purple { color: #a78bfa; }
.stat-green  { color: #34d399; }
.stat-red    { color: #f87171; }
.stat-blue   { color: #38bdf8; }

.log-box {
    background: #06060e;
    border: 1px solid #1a1a2e;
    border-radius: 12px;
    padding: 1rem 1.2rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.76rem;
    height: 300px;
    overflow-y: auto;
    color: #64748b;
    line-height: 1.8;
}
.log-success { color: #34d399; }
.log-error   { color: #f87171; }
.log-warn    { color: #fbbf24; }
.log-done    { color: #a78bfa; font-weight: 600; }

.progress-wrap {
    background: #1a1a2e;
    border-radius: 999px;
    height: 6px;
    overflow: hidden;
    margin: 0.5rem 0 0.3rem;
}
.progress-fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #a78bfa 0%, #38bdf8 100%);
    transition: width 0.35s ease;
}
.pct-label {
    text-align: right;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: #334155;
    margin-top: 0.2rem;
}

/* selector chain table */
.sel-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    margin-top: 0.5rem;
}
.sel-table td {
    padding: 0.3rem 0.5rem;
    border-bottom: 1px solid #1a1a2e;
    color: #64748b;
    vertical-align: top;
}
.sel-table td:first-child {
    color: #a78bfa;
    white-space: nowrap;
    font-weight: 700;
    width: 28px;
}
.sel-new td:first-child { color: #34d399 !important; }
.sel-new td            { color: #94a3b8 !important; }

.col-pill {
    display: inline-block;
    border-radius: 5px;
    padding: 0.2rem 0.6rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    margin: 0.18rem;
    border: 1px solid #1e1e2e;
    color: #a78bfa;
    background: #13131a;
}
.col-pill.found   { border-color: #34d399; color: #34d399; }
.col-pill.missing { border-color: #f87171; color: #f87171; }

[data-testid="stButton"] > button {
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    letter-spacing: 0.06em;
    border-radius: 10px;
    padding: 0.6rem 2rem;
    font-size: 0.88rem;
}
.hdivider {
    border: none;
    border-top: 1px solid #12121e;
    margin: 1.4rem 0;
}
label { color: #64748b !important; font-size: 0.8rem !important; }
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: #0a0a0f; }
::-webkit-scrollbar-thumb { background: #1e1e2e; border-radius: 999px; }
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
REQUIRED_COLS = ["Account name", "Website URL", "Campaign ID",
                 "Reporting starts", "Reporting ends", "Portal"]

SELECTOR_DOCS = [
    ("S1", ".product-item__product-title > a"),
    ("S2", ".product__title (any tag)"),
    ("S3", "h1[class*=product__title]"),
    ("S4", "div.product__title → h2.h1"),
    ("S5", "h3.card__heading → a.full-unstyled-link (first)"),
    ("S6", "h3.card__heading.h5 → a[id^=CardLink] (first card)", True),
]

# ─── Session State ────────────────────────────────────────────────────────────
for k, v in {
    "df": None, "result_df": None, "logs": [],
    "running": False, "processed": 0, "found": 0,
    "not_found": 0, "total": 0, "start_time": None, "done": False,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACTOR  — 6 selectors, returns on first match
# ══════════════════════════════════════════════════════════════════════════════
def extract_product_name(driver, wait):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    # ── S1: .product-item__product-title > a ─────────────────────────────────
    try:
        el   = wait.until(EC.presence_of_element_located(
                   (By.CLASS_NAME, 'product-item__product-title')))
        name = el.find_element(By.TAG_NAME, 'a').text.strip()
        if name: return name, "S1"
    except: pass

    # ── S2: first element with class product__title ───────────────────────────
    try:
        el   = wait.until(EC.presence_of_element_located(
                   (By.CLASS_NAME, 'product__title')))
        name = el.text.strip()
        if name: return name, "S2"
    except: pass

    # ── S3: h1[contains(@class,'product__title')] ────────────────────────────
    try:
        el   = driver.find_element(By.XPATH,
                   "//h1[contains(@class,'product__title')]")
        name = el.text.strip()
        if name: return name, "S3"
    except: pass

    # ── S4: div.product__title → h2.h1 ───────────────────────────────────────
    try:
        el   = driver.find_element(By.XPATH,
                   "//div[contains(@class,'product__title')]"
                   "//h2[contains(@class,'h1')]")
        name = el.text.strip()
        if name: return name, "S4"
    except: pass

    # ── S5: h3.card__heading → first a.full-unstyled-link ────────────────────
    try:
        el   = driver.find_element(By.XPATH,
                   "//h3[contains(@class,'card__heading')]"
                   "//a[contains(@class,'full-unstyled-link')]")
        name = el.text.strip()
        if name: return name, "S5"
    except: pass

    # ── S6: FIRST card only — h3.card__heading.h5 → a[id^='CardLink'] ────────
    #   Targets exactly:
    #   <h3 class="card__heading h5">
    #     <a id="CardLink-..." class="full-unstyled-link">Product Name</a>
    #   </h3>
    #   Uses [1] index so we always grab the FIRST product card on the page.
    try:
        el   = driver.find_element(By.XPATH,
                   "(//h3[contains(@class,'card__heading') and contains(@class,'h5')]"
                   "//a[starts-with(@id,'CardLink') and "
                   "contains(@class,'full-unstyled-link')])[1]")
        name = el.text.strip()
        if name: return name, "S6"
    except: pass

    return None, "NF"


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPER THREAD
# ══════════════════════════════════════════════════════════════════════════════
def run_scraper(df, log_q, result_q, delay_min, delay_max, timeout):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait

    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--ignore-certificate-errors')
    opts.add_argument('--blink-settings=imagesEnabled=false')
    opts.add_argument('--window-size=1920,1080')
    opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36')
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

    result_df = df.copy()
    result_df['Product Name'] = ''
    processed = found = not_found = 0

    try:
        for index, row in df.iterrows():
            url     = str(row.get('Website URL', '')).strip()
            account = str(row.get('Account name', 'Unknown'))[:42]

            if not url or url.lower() == 'nan':
                log_q.put(("warn", f"Row {index+1}: {account} → Skipped (no URL)"))
                continue

            try:
                driver.get(url)
                wait = WebDriverWait(driver, timeout)
                name, sel = extract_product_name(driver, wait)

                if name:
                    result_df.at[index, 'Product Name'] = name
                    found += 1
                    log_q.put(("success",
                               f"Row {index+1}: {account} → [{sel}] {name[:55]}"))
                else:
                    result_df.at[index, 'Product Name'] = 'NOT FOUND'
                    not_found += 1
                    log_q.put(("error",
                               f"Row {index+1}: {account} → Not Found"))

            except Exception as e:
                result_df.at[index, 'Product Name'] = 'NOT FOUND'
                not_found += 1
                log_q.put(("error",
                           f"Row {index+1}: {account} → ERR: {str(e)[:65]}"))

            processed += 1
            log_q.put(("progress", (processed, found, not_found)))
            time.sleep(random.uniform(delay_min, delay_max))

    finally:
        driver.quit()

    result_q.put(result_df)
    log_q.put(("done",
               f"━━ Done — {found}/{processed} products found "
               f"({int(found/processed*100) if processed else 0}% success) ━━"))


# ══════════════════════════════════════════════════════════════════════════════
#  UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════
stats_ph = None
log_ph   = None

def render_stats(processed, found, not_found, total, elapsed=1):
    rate = processed / elapsed * 60 if elapsed > 0 else 0
    pct  = int(processed / total * 100) if total > 0 else 0
    rem  = total - processed
    stats_ph.markdown(f"""
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value stat-blue">{processed}</div>
        <div class="stat-label">Processed</div>
      </div>
      <div class="stat-card">
        <div class="stat-value stat-green">{found}</div>
        <div class="stat-label">Found</div>
      </div>
      <div class="stat-card">
        <div class="stat-value stat-red">{not_found}</div>
        <div class="stat-label">Not Found</div>
      </div>
      <div class="stat-card">
        <div class="stat-value stat-purple">{rate:.0f}</div>
        <div class="stat-label">Rows / Min</div>
      </div>
    </div>
    <div class="progress-wrap">
      <div class="progress-fill" style="width:{pct}%"></div>
    </div>
    <div class="pct-label">{pct}% complete · {rem} remaining</div>
    """, unsafe_allow_html=True)


def render_logs(logs):
    cm = {"success":"log-success","error":"log-error",
          "warn":"log-warn","done":"log-done"}
    html = "".join(
        f'<div class="{cm.get(t,"")}">{m}</div>'
        for t, m in logs[-100:]
    )
    log_ph.markdown(f'<div class="log-box">{html}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════════

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<p class="hero-title">⬡ Product Scraper</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="hero-sub">Selenium · 6-Selector fallback chain · '
    'First-card extraction · Live log</p>',
    unsafe_allow_html=True
)
st.markdown('<hr class="hdivider">', unsafe_allow_html=True)

# ── 01 Upload  +  02 Config ───────────────────────────────────────────────────
left, right = st.columns([3, 2], gap="large")

with left:
    st.markdown('<div class="section-label">01 · Upload File</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader("Excel file (.xlsx)", type=["xlsx"],
                                label_visibility="collapsed")

    if uploaded:
        try:
            df = pd.read_excel(uploaded, engine='openpyxl', dtype={'Campaign ID': str})
            st.session_state.df    = df
            st.session_state.total = len(df)

            found_c   = [c for c in REQUIRED_COLS if c in df.columns]
            missing_c = [c for c in REQUIRED_COLS if c not in df.columns]

            st.markdown("**Column validation:**")
            pills = "".join(
                f'<span class="col-pill {"found" if c in df.columns else "missing"}">'
                f'{"✓" if c in df.columns else "✗"} {c}</span>'
                for c in REQUIRED_COLS
            )
            st.markdown(pills, unsafe_allow_html=True)

            if missing_c:
                st.warning(f"Missing columns: {', '.join(missing_c)}")

            st.markdown(
                f"<br>📄 &nbsp;<b>{len(df):,} rows</b> &nbsp;·&nbsp; "
                f"<b>{len(df.columns)} columns</b>",
                unsafe_allow_html=True
            )
            st.dataframe(df[found_c].head(5), use_container_width=True, height=175)

        except Exception as e:
            st.error(f"Read error: {e}")

with right:
    st.markdown('<div class="section-label">02 · Configuration</div>', unsafe_allow_html=True)
    delay_min = st.number_input("Min delay (sec)", 0.5, 10.0,  1.5, 0.5)
    delay_max = st.number_input("Max delay (sec)", 0.5, 15.0,  2.5, 0.5)
    timeout   = st.number_input("Page timeout (sec)", 3,  30,   8,   1)

    st.markdown("<br>**Selector chain** (S1 → S6, stops on first hit):",
                unsafe_allow_html=True)

    rows_html = ""
    for item in SELECTOR_DOCS:
        label, desc = item[0], item[1]
        is_new      = len(item) == 3
        tr_cls      = ' class="sel-new"' if is_new else ""
        new_badge   = ' <span style="color:#34d399;font-size:0.6rem">✦ NEW</span>' if is_new else ""
        rows_html  += f"<tr{tr_cls}><td>{label}</td><td>{desc}{new_badge}</td></tr>"

    st.markdown(
        f'<table class="sel-table">{rows_html}</table>',
        unsafe_allow_html=True
    )

st.markdown('<hr class="hdivider">', unsafe_allow_html=True)

# ── 03 Run ────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-label">03 · Run Scraper</div>', unsafe_allow_html=True)

btn_col, _ = st.columns([2, 8])
with btn_col:
    start_btn = st.button(
        "▶  Start Scraping", type="primary",
        disabled=(st.session_state.df is None or st.session_state.running)
    )

# placeholders (order matters)
stats_ph = st.empty()
log_ph   = st.empty()

# ── Start ─────────────────────────────────────────────────────────────────────
if start_btn and st.session_state.df is not None:
    st.session_state.update({
        "running": True, "processed": 0, "found": 0,
        "not_found": 0, "logs": [], "done": False,
        "result_df": None, "start_time": time.time()
    })

    log_q    = queue.Queue()
    result_q = queue.Queue()

    threading.Thread(
        target=run_scraper,
        args=(st.session_state.df, log_q, result_q, delay_min, delay_max, timeout),
        daemon=True
    ).start()

    # live polling loop
    while True:
        drained = False
        while not log_q.empty():
            drained = True
            typ, msg = log_q.get()
            if typ == "progress":
                p, f, n = msg
                st.session_state.processed = p
                st.session_state.found     = f
                st.session_state.not_found = n
            else:
                st.session_state.logs.append((typ, msg))
                if typ == "done":
                    break

        elapsed = time.time() - st.session_state.start_time
        render_stats(st.session_state.processed, st.session_state.found,
                     st.session_state.not_found, st.session_state.total, elapsed)
        render_logs(st.session_state.logs)

        # check if scraper signalled done
        if any(t == "done" for t, _ in st.session_state.logs):
            break

        time.sleep(0.5)

    if not result_q.empty():
        st.session_state.result_df = result_q.get()

    st.session_state.running = False
    st.session_state.done    = True
    st.rerun()

# ── Persist stats/logs on rerun ───────────────────────────────────────────────
if not st.session_state.running and st.session_state.processed > 0:
    render_stats(st.session_state.processed, st.session_state.found,
                 st.session_state.not_found, st.session_state.total)
    render_logs(st.session_state.logs)

# ── 04 Results & Download ─────────────────────────────────────────────────────
if st.session_state.done and st.session_state.result_df is not None:
    st.markdown('<hr class="hdivider">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">04 · Results & Download</div>',
                unsafe_allow_html=True)

    res   = st.session_state.result_df
    total = st.session_state.total
    fc    = st.session_state.found
    pct   = int(fc / total * 100) if total else 0

    st.markdown(f"""
    <div style='background:#0d1117;border:1px solid #1e1e2e;border-radius:10px;
                padding:1rem 1.4rem;margin-bottom:1rem;
                font-family:"JetBrains Mono",monospace;font-size:0.82rem'>
      <span style='color:#34d399'>✓</span>
      &nbsp;Complete &nbsp;·&nbsp;
      <span style='color:#34d399'>{fc}</span> /
      <span style='color:#94a3b8'>{total}</span> products found
      &nbsp;·&nbsp;
      <span style='color:#a78bfa'>{pct}%</span> success rate
    </div>
    """, unsafe_allow_html=True)

    preview_cols = [c for c in
        ['Account name', 'Website URL', 'Campaign ID',
         'Reporting starts', 'Reporting ends', 'Portal', 'Product Name']
        if c in res.columns]

    st.dataframe(res[preview_cols].head(25), use_container_width=True, height=320)

    # ── Downloads ─────────────────────────────────────────────────────────────
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine='openpyxl') as w:
        res.to_excel(w, index=False, sheet_name='Results')
    xlsx_buf.seek(0)

    d1, d2 = st.columns(2)
    with d1:
        st.download_button(
            "⬇  Download Excel",
            data=xlsx_buf.getvalue(),
            file_name=f"scraping_results_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
    with d2:
        st.download_button(
            "⬇  Download CSV",
            data=res.to_csv(index=False).encode(),
            file_name=f"scraping_results_{ts}.csv",
            mime="text/csv"
        )
