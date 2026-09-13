import os
import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor

pt_dir = "/home/awerkori/.Projects/Project-Nox/fonte-extensoes/src/pt"
extensions = sorted(os.listdir(pt_dir))

all_meta = []
for ext in extensions:
    path = os.path.join(pt_dir, ext)
    gradle_file = os.path.join(path, "build.gradle.kts")
    name = ext
    theme = "custom"
    base_url = ""
    is_nsfw = False
    
    if os.path.exists(gradle_file):
        with open(gradle_file, "r", encoding="utf-8", errors="ignore") as f:
            c = f.read()
            m_name = re.search(r'name\s*=\s*"([^"]+)"', c)
            if m_name: name = m_name.group(1)
            m_theme = re.search(r'theme\s*=\s*"([^"]+)"', c)
            if m_theme: theme = m_theme.group(1)
            m_url = re.search(r'baseUrl\s*=\s*"([^"]+)"', c)
            if not m_url:
                m_url = re.search(r'custom\("([^"]+)"\)', c)
            if m_url: base_url = m_url.group(1)
            if "NSFW" in c: is_nsfw = True
            
    all_meta.append({
        "id": ext,
        "name": name,
        "theme": theme,
        "base_url": base_url,
        "is_nsfw": is_nsfw
    })

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=1, pool_connections=30, pool_maxsize=30)
session.mount("https://", adapter)
session.mount("http://", adapter)

def probe(item):
    url = item["base_url"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    try:
        resp = session.get(url, headers=headers, timeout=(4.0, 5.0), allow_redirects=True, verify=False)
        item["status_code"] = resp.status_code
        item["final_url"] = str(resp.url)
        body = resp.text[:4096]
        m_t = re.search(r"<title>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
        title = m_t.group(1).strip() if m_t else ""
        item["title"] = title
        
        if resp.status_code == 200:
            if "Just a moment..." in title or "Attention Required!" in title or ("cloudflare" in body.lower() and "challenge" in body.lower()):
                item["probe_result"] = "CF_CHALLENGE"
                item["detail"] = f"Cloudflare interactive challenge ({title})"
            else:
                item["probe_result"] = "200_OK"
                item["detail"] = f"OK ({title[:30]})"
        elif resp.status_code == 403:
            item["probe_result"] = "HTTP_403_BLOCKED"
            item["detail"] = "Cloudflare/WAF 403 Forbidden"
        elif resp.status_code == 404:
            item["probe_result"] = "HTTP_404_NOT_FOUND"
            item["detail"] = "HTTP 404 Not Found"
        elif resp.status_code in (500, 502, 503, 504):
            item["probe_result"] = f"HTTP_{resp.status_code}_SERVER_ERROR"
            item["detail"] = f"Server Error {resp.status_code}"
        else:
            item["probe_result"] = f"HTTP_{resp.status_code}"
            item["detail"] = f"HTTP {resp.status_code}"
    except requests.exceptions.SSLError as e:
        item["status_code"] = 0
        item["probe_result"] = "SSL_ERROR"
        item["detail"] = f"SSL Error: {str(e)[:30]}"
    except requests.exceptions.ConnectionError as e:
        item["status_code"] = 0
        err_s = str(e)
        if "Name or service not known" in err_s or "Failed to resolve" in err_s or "NXDOMAIN" in err_s:
            item["probe_result"] = "DNS_NXDOMAIN"
            item["detail"] = "Domínio inexistente (DNS NXDOMAIN)"
        elif "Connection refused" in err_s:
            item["probe_result"] = "CONN_REFUSED"
            item["detail"] = "Conexão recusada"
        else:
            item["probe_result"] = "CONN_ERROR"
            item["detail"] = f"Connection error: {err_s[:30]}"
    except requests.exceptions.Timeout:
        item["status_code"] = 0
        item["probe_result"] = "TIMEOUT"
        item["detail"] = "Timeout de conexão (>5s)"
    except Exception as e:
        item["status_code"] = 0
        item["probe_result"] = "ERROR"
        item["detail"] = str(e)[:30]
        
    return item

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print(f"Probing {len(all_meta)} sources with requests...")
with ThreadPoolExecutor(max_workers=25) as pool:
    results = list(pool.map(probe, all_meta))

with open("scripts/probe_137_precise.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

summary = {}
for r in results:
    res = r.get("probe_result", "UNKNOWN")
    summary[res] = summary.get(res, 0) + 1

print("\nProbe Results Summary:")
for k, v in sorted(summary.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")
