Here’s the short version:

Goal: Find and download Product Carbon Footprint (PCF) PDFs for a given brand and product type (e.g., laptops) from the brand’s own website, with a smart fallback to Exa AI search.

HTTP setup: Uses a shared requests session with retries/backoff, custom headers, and throttling for polite, resilient crawling.

Helpers: Utilities to parse HTML, detect PDFs, normalize domains, hash files, and expand product-type tokens (e.g., “laptop” → includes “notebook/Chromebook/MacBook/…book”).

Landing navigation: Scores links on a brand’s sustainability/PCF page to find the most likely “View PCFs/Reports” hub, and can jump to the correct tab/section (e.g., “laptops”) via anchors/ARIA/headers.

PDF extraction (page-level): Collects PDF links from <a> tags, common data-* attributes, and a regex sweep over raw HTML (catches links embedded in inline JSON).

Site crawl (BFS): Crawls same-domain pages up to depth 2, collecting only same-domain PDFs whose filenames/text match product tokens (including *book patterns).

Search fallback: If not enough PDFs are found, queries Exa with site/filetype filters + PCF keywords to capture additional on-domain PDFs.

Downloading: Verifies content, downloads PDFs, names them by SHA-256 digest, and stores under data/pcf/<brand>/.

Orchestrator: run_brand_producttype(...) ties it all together and returns a summary: pages used, counts found/downloaded, and file metadata.

Bonus helper: find_landing_url(brand) uses Exa to auto-discover a likely sustainability/PCF landing page if you don’t have one
