#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install lxml


# In[3]:


# ==========================
# BLOCK 1 — Imports & Setup
# ==========================
import os, re, time, json, logging, hashlib
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---- Exa AI config (set env: EXA_API_KEY) ----
EXA_API_KEY = os.getenv("EXA_API_KEY", "d906a649-ab82-457c-9ece-3ae8d581d7a7").strip()
EXA_ENDPOINT = "https://api.exa.ai/search"

# ---- Logging ----
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- HTTP session with retries/backoff ----
USER_AGENT = "PCF-Harvester/3.0 (+https://yourproject.example)"
REQUEST_TIMEOUT = 30
THROTTLE_SEC = 0.8

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
})

retry = Retry(
    total=6, connect=3, read=3,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)


# In[5]:


# ==========================
# BLOCK 2 — Utilities
# ==========================
def get_html(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[BeautifulSoup]:
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e1:
        logging.warning("GET primary failed for %s: %s", url, e1)
        # second try with Referer
        try:
from urllib.parse import urlparse
ref = {"Referer": f"{urlparse(url).scheme}://{urlparse(url).netloc}/"}
            r2 = SESSION.get(url, headers=ref, timeout=timeout)
            r2.raise_for_status()
            return BeautifulSoup(r2.text, "lxml")
        except Exception as e2:
            logging.error("GET failed for %s: %s", url, e2)
            return None

def fetch_html(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logging.warning("GET %s failed: %s", url, e)
        return None

def is_pdf_url(url: str) -> bool:
    u = url.lower()
    return u.endswith(".pdf") or ".pdf?" in u

def etld(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def product_type_tokens(ptype: str) -> List[str]:
    p = (ptype or "").strip().lower()
    tokens = []

    if not p:
        return []

    if "laptop" in p or "notebook" in p:
        # Accept any string that ends with "book", like MacBook, Chromebook, Ultrabook
        tokens.extend(["laptop", "notebook", "chromebook", "macbook", "ultrabook", "book"])
    elif "desktop" in p or "pc" in p:
        tokens.extend(["desktop", "pc", "tower", "mini"])
    elif "monitor" in p or "display" in p:
        tokens.extend(["monitor", "display"])
    elif "server" in p:
        tokens.extend(["server"])
    else:
        tokens.append(p)

    return tokens

def merge_pdf_lists(*lists: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen, merged = set(), []
    for lst in lists:
        for p in lst:
            u = p.get("url")
            if not u or u in seen: 
                continue
            seen.add(u); merged.append(p)
    return merged


# In[ ]:


# ==========================
# BLOCK 3 — Exa AI client + fallback
# ==========================
def exa_search(query: str, top_k: int = 20) -> List[Dict[str, Any]]:
    if not EXA_API_KEY:
        logging.error("EXA_API_KEY not set. Exa search disabled.")
        return []
    headers = {
        "x-api-key": EXA_API_KEY,
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"query": query, "numResults": top_k}
    try:
        r = SESSION.post(EXA_ENDPOINT, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        return data.get("results") or data.get("documents") or []
    except Exception as e:
        logging.error("Exa API error: %s", e)
        return []

PCF_PDF_KEYWORDS = [
    '"product carbon footprint"', '"Product Environmental Report"',
    '"life cycle assessment"', 'LCA', 'EPD', '"kg CO2"', '"kg CO2e"'
]

def harvest_pcf_pdfs_via_search(brand: str, product_type: str, brand_domain: str,
                                top_k_per_query: int = 30) -> List[Dict[str, str]]:
    syns = product_type_synonyms(product_type)
    syn_clause = "(" + " OR ".join(f'"{s}"' for s in syns) + ")" if syns else ""
    queries: List[str] = []
    for kw in PCF_PDF_KEYWORDS:
        queries.append(f'site:{brand_domain} filetype:pdf {brand} {kw}')
        if syn_clause:
            queries.append(f'site:{brand_domain} filetype:pdf {brand} {syn_clause} {kw}')

    seen, out = set(), []
    for q in queries:
        logging.info("Exa fallback query: %s", q)
        hits = exa_search(q, top_k=top_k_per_query)
        for h in hits:
            url = (h.get("url") or "").strip()
            title = (h.get("title") or "").strip()
            if not url or not url.lower().endswith(".pdf"):
                continue
            if brand_domain not in etld(url):
                continue
            if url in seen:
                continue
            seen.add(url)
            out.append({"url": url, "product_text": title or url.rsplit("/", 1)[-1]})
        time.sleep(THROTTLE_SEC)
    logging.info("Exa fallback harvested %d PDF(s) for %s/%s", len(out), brand, product_type)
    return out


# In[ ]:


# ==========================
# BLOCK 4 — Landing navigation
# ==========================
#def follow_view_pcfs(landing_url: str) -> str:
 #   soup = get_html(landing_url)
  #  if not soup:
       # return landing_url
  #  cta_needles = [
     #   "view pcfs", "see pcfs", "product carbon footprints",
      #  "product environmental report", "environmental report",
       # "view all", "see all"
   # ]
    #for a in soup.find_all("a", href=True):
     #   txt = (a.get_text(" ") or "").strip().lower()
      #  if any(k in txt for k in cta_needles):
       #     return urljoin(landing_url, a["href"])
    #return landing_url
from urllib.parse import urlparse

def _score_cta_link(base_url: str, a) -> int:
    href = a.get("href", "")
    txt = (a.get_text(" ") or "").strip().lower()
    if not href:
        return -999

    abs_url = urljoin(base_url, href)
    s = f"{txt} {abs_url}".lower()

    # ❌ Kill immediately if irrelevant section
    bad = ["support", "services", "drivers", "partners", "marketing", "blog", "press", "solution", "contact"]
    if any(b in s for b in bad):
        return -999

    # 🎯 Jackpot: direct PDF with strong indicators
    if href.lower().endswith(".pdf") and any(k in s for k in ["pcf", "carbon", "footprint", "lca", "epd"]):
        return 999  # stop right there, valid report found

    score = 0

    # ✅ Strong PCF-related content
    if any(t in s for t in [
        "product-carbon", "carbon-footprint", "product carbon footprint",
        "environmental", "product environmental report", "epd", "sustainab", "pcf"
    ]):
        score += 10

    # 💡 Bonus: product-specific mentions
# Match product types and any '...book' variants (e.g., MacBook, Notebook, Chromebook)
    if any(p in s for p in ["laptop", "desktop", "monitor", "server", "device"]) or "book" in s:
       score += 3


    # 🔒 Domain and structure boost
    base = urlparse(base_url)
    p = urlparse(abs_url)
    if p.netloc != base.netloc:
        score -= 5
    if abs_url.startswith(base_url.rstrip("/")):
        score += 2

    if "#" in href:
        score += 1

    return score

def follow_view_pcfs(landing_url: str) -> str:
    soup = get_html(landing_url)
    if not soup:
        return landing_url

    best_url, best_score = None, -10**9
    for a in soup.find_all("a", href=True):
        sc = _score_cta_link(landing_url, a)
        if sc > best_score:
            best_score = sc
            best_url = urljoin(landing_url, a["href"])

    # Only follow if it looks *strongly* like a PCF hub; else stay put
    if best_url and best_score >= 8:
        logging.info(f"follow_view_pcfs: following best link (score {best_score}) → {best_url}")
        return best_url

    logging.info("follow_view_pcfs: staying on landing page (no strong PCF CTA found)")
    return landing_url

def _norm_type(s: str) -> str:
    return (s or "").strip().lower().rstrip("s")

def resolve_product_tab(pcfs_url: str, product_type: str) -> str:
    soup = get_html(pcfs_url)
    if not soup:
        return pcfs_url

    wanted = _norm_type(product_type)
    base = pcfs_url.split("#")[0]

    # A) Exact tab anchor by text
    for a in soup.find_all("a", href=True):
        label = _norm_type(a.get_text(" "))
        if label == wanted:
            href = a["href"]
            # accept only same-page anchors or same-family URLs
            if href.startswith("#"):
                return base + href
            absu = urljoin(pcfs_url, href)
            if absu.startswith(base):  # same page family
                return absu

    # B) aria-controls / data-bs-target panel
    for a in soup.find_all("a", href=True):
        label = _norm_type(a.get_text(" "))
        if label != wanted:
            continue
        panel_id = a.get("aria-controls") or a.get("data-bs-target") or ""
        if panel_id.startswith("#"): 
            panel_id = panel_id[1:]
        if panel_id:
            return f"{base}#{panel_id}"

    # C) headings with ids
    for h in soup.find_all(["h2", "h3", "h4"]):
        if _norm_type(h.get_text(" ")) == wanted and h.get("id"):
            return f"{base}#{h['id']}"

    # If nothing reliable found, do NOT jump to some other random page
    logging.info("resolve_product_tab: no product tab found, staying on PCF hub")
    return pcfs_url


# In[ ]:


# ==========================
# BLOCK 5 — Robust per-page PDF extraction
# ==========================
PDF_REGEX = re.compile(r'https?://[^\s"\'<>]+\.pdf(?:\?[^\s"\'<>]*)?', re.I)

def _collect_pdf_links_from_html(url: str, html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    found: List[Dict[str, str]] = []

    # 1) <a href="...pdf">
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            found.append({"url": urljoin(url, href), "product_text": (a.get_text(" ") or "").strip()})

    # 2) Common data-* attributes
    ATTRS = ["data-href","data-url","data-download","data-asset-url"]
    for tag in soup.find_all(True):
        for attr in ATTRS:
            val = tag.get(attr)
            if val and ".pdf" in val.lower():
                found.append({"url": urljoin(url, val), "product_text": (tag.get_text(" ") or "").strip()})

    # 3) Regex sweep across raw HTML (captures inline JSON/script)
    for m in PDF_REGEX.finditer(html):
        found.append({"url": urljoin(url, m.group(0)), "product_text": ""})

    # Dedupe
    seen, out = set(), []
    for x in found:
        u = x["url"]
        if u in seen: 
            continue
        seen.add(u); out.append(x)
    return out

def extract_pdfs_page_robust(page_url: str, require_tokens: Optional[List[str]] = None) -> List[Dict[str, str]]:
    html = fetch_html(page_url, REQUEST_TIMEOUT)
    if html is None:
        return []
    pdfs = _collect_pdf_links_from_html(page_url, html)
    if require_tokens:
        toks = [t.lower() for t in require_tokens]
        keep: List[Dict[str, str]] = []
        for p in pdfs:
            ctx = (p.get("product_text") or "").lower()
            if any(t in ctx for t in toks) or re.search(r"\\b\\w*book\\b", ctx): # <- match "MacBook", "Chromebook", etc.
                keep.append(p)
            logging.info(f"Filtered PDFs using tokens + 'book' logic → kept {len(keep)} of {len(pdfs)}")
            return keep
    return pdfs


# In[ ]:


#block 5.5
def extract_model_pdfs_by_section(soup: BeautifulSoup, product_tokens: List[str]) -> List[Dict[str, str]]:
    """
    Grouped extraction: find PDFs under sections likely referring to the product type.
    Uses headings (h2/h3), list items, and anchor tags.
    """
    results = []
    current_section = None
    section_map = {}

    # Normalize token list for matching
    tokens = [t.lower() for t in product_tokens]

    for tag in soup.find_all(["h2", "h3", "li", "a"]):
        text = (tag.get_text(" ") or "").strip().lower()

        if tag.name in ["h2", "h3"]:
            # Start of a new section
            current_section = text
        elif tag.name == "li" and any(tok in text for tok in tokens):
            # List items that may contain model names
            current_section = text
        elif tag.name == "a" and tag.has_attr("href") and tag["href"].endswith(".pdf"):
            href = urljoin(soup.base_url or "", tag["href"])
            if current_section and any(tok in current_section for tok in tokens):
                results.append({
                    "url": href,
                    "product_text": tag.get_text(" ").strip() or current_section
                })

    return results


# In[ ]:


# ==========================
# BLOCK 6 — Same-domain BFS (depth 2)
# ==========================
from collections import deque

def _same_domain(u: str, domain: str) -> bool:
    return etld(u).endswith(domain.lower())

def _normalize_link(base: str, href: str) -> Optional[str]:
    if not href or href.startswith("javascript:") or href.startswith("mailto:"):
        return None
    return urljoin(base, href)

def is_product_pdf(url: str, product_tokens: Optional[List[str]] = None) -> bool:
    """
    Match PDF filenames to known product tokens or patterns like *book.
    """
    if not url.lower().endswith(".pdf"):
        return False
    name = url.lower()

    # Regex match: anything ending in 'book' (e.g., MacBook, Notebook, Ultrabook)
    if re.search(r"\b\w*book\b", name):
        return True

    if product_tokens:
        return any(tok in name for tok in product_tokens)
    
    return True  # fallback if no tokens provided

def bfs_collect_pdfs(start_url: str,
                     domain: str,
                     max_pages: int = 60,
                     max_depth: int = 2,
                     per_page_sleep: float = 0.4,
                     require_tokens: Optional[List[str]] = None) -> List[Dict[str, str]]:
    visited, q = set(), deque([(start_url, 0)])
    all_pdfs: List[Dict[str, str]] = []
    pages_seen = 0

    while q and pages_seen < max_pages:
        url, depth = q.popleft()
        if url in visited:
            continue
        visited.add(url)
        pages_seen += 1

        html = fetch_html(url, REQUEST_TIMEOUT)
        if html is None:
            continue

        soup = BeautifulSoup(html, "lxml")

        # ---- Extract all PDF links ----
        page_pdfs = []
        for a in soup.find_all("a", href=True):
            href = _normalize_link(url, a["href"])
            if not href or not is_pdf_url(href):
                continue
            if not _same_domain(href, domain):
                continue
            if is_product_pdf(href, product_tokens=require_tokens):
                page_pdfs.append({"url": href, "source": url})

        all_pdfs = merge_pdf_lists(all_pdfs, page_pdfs)

        # ---- Queue next HTML pages ----
        if depth >= max_depth:
            continue

        for a in soup.find_all("a", href=True):
            href = _normalize_link(url, a["href"])
            if not href or href.lower().endswith(".pdf"):
                continue
            if not _same_domain(href, domain):
                continue
            q.append((href, depth + 1))

        time.sleep(per_page_sleep)

    return all_pdfs


# In[ ]:


# ==========================
# BLOCK 7 — Download PDFs
# ==========================
def download_all(pdfs: List[Dict[str, str]], brand: str, out_dir: str = "data/pcf") -> List[Dict[str, Any]]:
    os.makedirs(os.path.join(out_dir, brand.lower()), exist_ok=True)
    saved: List[Dict[str, Any]] = []
    for i, p in enumerate(pdfs, 1):
        url = p["url"]
        try:
            head = SESSION.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            ct = (head.headers.get("Content-Type") or "").lower()
            if "pdf" not in ct and not is_pdf_url(url):
                logging.info("Skip non-PDF: %s (ct=%s)", url, ct)
                continue
        except Exception:
            pass
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            blob = r.content
            digest = sha256_bytes(blob)[:16]
            fname = f"{digest}.pdf"
            fpath = os.path.join(out_dir, brand.lower(), fname)
            with open(fpath, "wb") as f:
                f.write(blob)
            saved.append({
                "url": url,
                "file": fpath,
                "bytes": len(blob),
                "product_text": p.get("product_text","")
            })
            logging.info("[%d/%d] Saved %s", i, len(pdfs), fpath)
            time.sleep(0.3)
        except Exception as e:
            logging.warning("Download failed %s: %s", url, e)
    return saved


# In[ ]:


# ==========================
# BLOCK 8 — Orchestrator
# ==========================
def run_brand_producttype(
    brand: str,
    product_type: str,
    landing_url: str,
    pcfs_url: str,
    is_pdf_listing_page: bool = False,
    min_expected: int = 20
) -> Dict[str, Any]:
    """
    1) Follow landing → PCFs hub.
    2) If page already lists PDFs, extract directly.
    3) Otherwise, resolve product tab and crawl.
    4) Fallback to Exa search if needed.
    """
    brand = brand.strip()
    product_type = product_type.strip()
    domain = etld(landing_url)

    # ⚠️ Case 1: landing is itself a direct PDF
    if pcfs_url.lower().endswith(".pdf"):
        logging.info("Landing page is a direct PDF — skipping crawl.")
        saved = download_all(
            [{"url": pcfs_url, "product_text": os.path.basename(pcfs_url)}],
            brand=brand
        )
        return {
            "brand": brand,
            "product_type": product_type,
            "landing_url": landing_url,
            "pcfs_url": pcfs_url,
            "tab_url": pcfs_url,
            "found_count": 1,
            "downloaded_count": len(saved),
            "saved": saved
        }

    # ⚠️ Case 2: user confirms the landing page itself contains all relevant PDFs
    if is_pdf_listing_page:
        logging.info("User confirmed this is the PDF listing page. Skipping tab + BFS.")
        page_pdfs = extract_pdfs_page_robust(pcfs_url, require_tokens=product_type_tokens(product_type))
        crawled = []
        tab_url = pcfs_url
    else:
        # ✅ Standard flow: try to resolve product tab and crawl
        tab_url = resolve_product_tab(pcfs_url, product_type)
        time.sleep(THROTTLE_SEC)

        html = fetch_html(tab_url)
        if html:
            soup = BeautifulSoup(html, "lxml")
            soup.base_url = tab_url
            page_pdfs = extract_model_pdfs_by_section(soup, product_type_tokens(product_type))
        else:
            page_pdfs = []

        crawled = bfs_collect_pdfs(
            start_url=tab_url, domain=domain,
            max_pages=60, max_depth=2, per_page_sleep=0.4,
            require_tokens=product_type_tokens(product_type)
        )

    # ✅ Merge page + crawl results
    merged = merge_pdf_lists(page_pdfs, crawled)
    logging.info("After HTML + BFS: %d PDF(s)", len(merged))

    # 🔁 Fallback to Exa if PDFs are insufficient
    if len(merged) < min_expected:
        logging.warning("Only %d PDFs found; switching to Exa fallback.", len(merged))
        exa_pdfs = harvest_pcf_pdfs_via_search(brand, product_type, domain, top_k_per_query=40)
        merged = merge_pdf_lists(merged, exa_pdfs)
        logging.info("After Exa merge: %d PDF(s)", len(merged))

    # 💾 Download results
    saved = download_all(merged, brand=brand)

    return {
        "brand": brand,
        "product_type": product_type,
        "landing_url": landing_url,
        "pcfs_url": pcfs_url,
        "tab_url": tab_url,
        "found_count": len(merged),
        "downloaded_count": len(saved),
        "saved": saved
    }


# In[1]:


# ==========================
# BLOCK 9 — Landing Page Auto-Discovery
# ==========================
def find_landing_url(brand: str) -> str:
    """Use Exa AI to auto-discover the PCF landing page for a given brand."""
    logging.info(f"Querying Exa for landing page of {brand} ...")
    payload = {
        "query": f'site:{brand}.com ("product carbon footprint" OR "Product Environmental Report" OR sustainability)',
        "numResults": 10
    }
    headers = {"x-api-key": EXA_API_KEY, "User-Agent": USER_AGENT}
    try:
        r = requests.post(EXA_ENDPOINT, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        for item in data.get("results", []):
            url = item.get("url", "")
            if any(k in url.lower() for k in ["footprint", "sustainab", "product-carbon", "epd"]):
                return url
    except Exception as e:
        logging.error("Exa error while finding landing page: %s", e)
    return ""


# In[ ]:


# ==========================
# BLOCK 10 — Main (brand-agnostic)
# ==========================
if __name__ == "__main__":
    brand = input("Brand (e.g., dell, acer, hp): ").strip()
    product_type = input("Product Type (e.g., Laptops): ").strip()
    landing_url = input("PCF Landing URL (press Enter to auto-find): ").strip()

    if not landing_url:
        logging.info(f"Searching Exa for {brand} PCF landing page...")
        landing_url = find_landing_url(brand)
        if not landing_url:
            raise SystemExit(f"Could not auto-locate PCF page for {brand}.")
        logging.info(f"Auto-detected landing page: {landing_url}")
   # Ask user if this page already contains all PDFs
    use_as_is = input("Does this page ALREADY contain the PCF reports? (y/n): ").strip().lower()
    pcfs_url = landing_url if use_as_is.startswith("y") else follow_view_pcfs(landing_url)


    # Dynamic threshold: if brand tends to have lots of PDFs, raise it
    summary = run_brand_producttype(brand, product_type, landing_url,pcfs_url,is_pdf_listing_page=use_as_is.startswith("y"), min_expected=20)

    print("\nSummary")
    print("-------")
    print(f"Brand / Type:  {summary['brand']} / {summary['product_type']}")
    print(f"Landing URL:   {summary['landing_url']}")
    print(f"PCFs hub:      {summary['pcfs_url']}")
    print(f"Tab URL:       {summary['tab_url']}")
    print(f"Found PDFs:    {summary['found_count']}")
    print(f"Downloaded:    {summary['downloaded_count']}")


# In[ ]:




