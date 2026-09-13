import os
import glob
import re
import json
import urllib.request
import urllib.error
import socket
from concurrent.futures import ThreadPoolExecutor

pt_dir = "/home/awerkori/.Projects/Project-Nox/fonte-extensoes/src/pt"
extensions = sorted(os.listdir(pt_dir))

def get_ext_info(ext):
    path = os.path.join(pt_dir, ext)
    kts = glob.glob(os.path.join(path, "**/*.kt"), recursive=True)
    name = ext
    base_url = ""
    classes = []
    has_cf = False
    for kf in kts:
        with open(kf, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
            m_name = re.search(r'override\s+val\s+name\s*=\s*"([^"]+)"', content)
            if m_name:
                name = m_name.group(1)
            m_url = re.search(r'(?:baseUrl|defaultBaseUrl)\s*=\s*"([^"]+)"', content)
            if m_url and not base_url:
                base_url = m_url.group(1)
            m_cls = re.findall(r'class\s+(\w+)[^{:]*:\s*([^{\n]+)', content)
            for c, sc in m_cls:
                classes.append(f"{c}:{sc.strip()}")
            if "Cloudflare" in content or "turnstile" in content.lower() or "cloudflare" in content.lower():
                has_cf = True
                
    return {
        "id": ext,
        "name": name,
        "base_url": base_url,
        "classes": "; ".join(classes),
        "has_cf_mention": has_cf
    }

ext_infos = [get_ext_info(e) for e in extensions]

def probe(info):
    url = info["base_url"]
    if not url:
        info["probe"] = "NO_URL"
        info["status_code"] = 0
        return info
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=7) as resp:
            info["status_code"] = resp.getcode()
            body = resp.read(2048).decode("utf-8", errors="ignore")
            m_t = re.search(r"<title>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
            t = m_t.group(1).strip() if m_t else ""
            info["title"] = t
            info["probe"] = f"200 OK ({t[:25]})"
    except urllib.error.HTTPError as e:
        info["status_code"] = e.code
        info["probe"] = f"HTTP {e.code}"
    except urllib.error.URLError as e:
        info["status_code"] = 0
        err = str(e.reason)
        if "Name or service not known" in err or "ENOTFOUND" in err:
            info["probe"] = "DNS_NXDOMAIN"
        elif "timed out" in err:
            info["probe"] = "TIMEOUT"
        elif "certificate" in err.lower():
            info["probe"] = "SSL_ERROR"
        else:
            info["probe"] = f"URL_ERROR: {err[:25]}"
    except Exception as e:
        info["status_code"] = 0
        info["probe"] = f"ERR: {str(e)[:25]}"
    return info

with ThreadPoolExecutor(max_workers=20) as pool:
    results = list(pool.map(probe, ext_infos))

with open("/home/awerkori/.Projects/project-nox-importer/scripts/deep_audit_raw.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"Audit completed for {len(results)} sources.")
status_counts = {}
for r in results:
    code = r.get("status_code", 0)
    probe_type = "200 OK" if code == 200 else ("403 Blocked" if code == 403 else ("404/500 Fail" if code in (404, 500, 502, 503) else ("DNS/Timeout Fail" if code == 0 else f"HTTP {code}")))
    status_counts[probe_type] = status_counts.get(probe_type, 0) + 1

print("Probe Summary:")
for k, v in sorted(status_counts.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")
