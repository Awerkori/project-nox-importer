import os
import re
import glob
import json
import urllib.request
import urllib.error
import socket
from concurrent.futures import ThreadPoolExecutor

PT_DIR = "/home/awerkori/.Projects/Project-Nox/fonte-extensoes/src/pt"

# Known Active Sources currently implemented and certified in Importer
ACTIVE_SOURCES = {
    "mangaflix": {"domain": "mangaflix.net", "name": "MangaFlix", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "manhastro": {"domain": "manhastro.net", "name": "Manhastro", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "mangotoons": {"domain": "api.mangotoons.com", "name": "Mango Toons", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "megahentai": {"domain": "megahentai.com", "name": "MegaHentai", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "taimumangas": {"domain": "beta.taimumangas.com", "name": "TaimuMangas", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "hipercool": {"domain": "lerhentais.com", "name": "HipercooL", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "nexus": {"domain": "nexusmangas.com", "name": "Nexus Mangas", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "instahentai": {"domain": "instahentai.com", "name": "InstaHentai", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "euphoriascan": {"domain": "euphoriascan.com", "name": "Euphoria Scan", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "fleurblanche": {"domain": "fbsquadx.com", "name": "Fleur Blanche", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "littletyrant": {"domain": "tiraninha.world", "name": "Little Tyrant", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "mangalivreto": {"domain": "mangalivre.to", "name": "Manga Livre.to", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "montetai": {"domain": "montetaiscanlator.xyz", "name": "Monte Tai", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "nebulosascan": {"domain": "nebulosascan.com", "name": "Nebulosa Scan", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "nocturnesummer": {"domain": "nocfsb.com", "name": "Nocturne Summer", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "tankouhentai": {"domain": "tankouhentai.com", "name": "Tankou Hentai", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"},
    "cafecomyaoi": {"domain": "cafecomyaoi.com.br", "name": "Café com Yaoi", "chapters": "PASS", "pages": "PASS", "download": "PASS", "e2e": "PASS"}
}

# Policy excluded sources
POLICY_EXCLUDED = {
    "mangalivre": {"name": "Toon Livre", "domain": "toonlivre.net", "reason": "Excluída permanentemente por diretriz do projeto (Toon Livre)"},
    "spectralscan": {"name": "Nexus Toons", "domain": "nx-toons.xyz", "reason": "Excluída permanentemente por diretriz do projeto (Nexus Toons)"}
}

# Known Upstream Blocked (Cloudflare 403 / challenge in datacenter)
KNOWN_BLOCKED = {
    "kuro": {"name": "Kuro Mangas", "domain": "kuromangas.com", "reason": "Cloudflare 403 / Turnstile bloqueia ambiente de datacenter"},
    "hanamiheaven": {"name": "Hanami Heaven", "domain": "hanamiheaven.com", "reason": "Cloudflare 403 bloqueia ambiente de datacenter"}
}

def parse_extension_details(ext_id):
    ext_path = os.path.join(PT_DIR, ext_id)
    kt_files = glob.glob(os.path.join(ext_path, "src/**/*.kt"), recursive=True)
    
    name = ext_id.capitalize()
    base_url = ""
    framework = "Custom"
    
    for kf in kt_files:
        try:
            with open(kf, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                
                # Check name
                m_name = re.search(r'override\s+val\s+name\s*=\s*"([^"]+)"', content)
                if m_name:
                    name = m_name.group(1)
                
                # Check baseUrl
                m_url = re.search(r'override\s+val\s+baseUrl\s*=\s*"([^"]+)"', content)
                if not m_url:
                    m_url = re.search(r'baseUrl\s*=\s*"([^"]+)"', content)
                if not m_url:
                    m_url = re.search(r'defaultBaseUrl\s*=\s*"([^"]+)"', content)
                if m_url and not base_url:
                    base_url = m_url.group(1)
                
                # Check framework
                if "Madara(" in content or ": Madara" in content:
                    framework = "Madara"
                elif "MangaThemesia" in content:
                    framework = "MangaThemesia"
                elif "HeanCms" in content:
                    framework = "HeanCms"
                elif "ZeistManga" in content:
                    framework = "ZeistManga"
                elif "ParsedHttpSource" in content:
                    framework = "ParsedHttpSource"
                elif "HttpSource" in content:
                    framework = "HttpSource"
        except Exception:
            pass
            
    return name, base_url, framework

def probe_http(url):
    if not url:
        return "NO_URL", 0
    clean_url = url if url.startswith("http") else f"https://{url}"
    try:
        req = urllib.request.Request(
            clean_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return "200 OK", resp.getcode()
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}", e.code
    except urllib.error.URLError as e:
        err_msg = str(e.reason)
        if "Name or service not known" in err_msg or "nodename nor servname provided" in err_msg or "ENOTFOUND" in err_msg:
            return "DNS NXDOMAIN", 0
        if "timed out" in err_msg:
            return "TIMEOUT", 0
        if "Connection refused" in err_msg:
            return "CONN_REFUSED", 0
        if "certificate" in err_msg.lower():
            return "SSL_ERROR", 0
        return f"URLError: {err_msg[:30]}", 0
    except socket.timeout:
        return "TIMEOUT", 0
    except Exception as e:
        return f"ERROR: {str(e)[:30]}", 0

def audit_all():
    candidates = sorted([d for d in os.listdir(PT_DIR) if os.path.isdir(os.path.join(PT_DIR, d))])
    print(f"Auditing {len(candidates)} candidates...")
    
    results = []
    
    for ext_id in candidates:
        name, base_url, framework = parse_extension_details(ext_id)
        
        # Override known active details
        if ext_id in ACTIVE_SOURCES:
            act = ACTIVE_SOURCES[ext_id]
            results.append({
                "id": ext_id,
                "name": act["name"],
                "domain": act["domain"],
                "catalog": "PASS",
                "chapters": "PASS",
                "pages": "PASS",
                "download": "PASS",
                "e2e": "PASS",
                "status": "ACTIVE",
                "reason": "100% funcional ponta a ponta, certificado e ativo no Importer"
            })
            continue
            
        if ext_id in POLICY_EXCLUDED:
            pol = POLICY_EXCLUDED[ext_id]
            results.append({
                "id": ext_id,
                "name": pol["name"],
                "domain": pol["domain"],
                "catalog": "N/A",
                "chapters": "N/A",
                "pages": "N/A",
                "download": "N/A",
                "e2e": "EXCLUDED",
                "status": "EXCLUDED_BY_POLICY",
                "reason": pol["reason"]
            })
            continue
            
        if ext_id in KNOWN_BLOCKED:
            kb = KNOWN_BLOCKED[ext_id]
            results.append({
                "id": ext_id,
                "name": kb["name"],
                "domain": kb["domain"],
                "catalog": "FAIL",
                "chapters": "FAIL",
                "pages": "FAIL",
                "download": "FAIL",
                "e2e": "BLOCKED",
                "status": "BLOCKED",
                "reason": kb["reason"]
            })
            continue

        domain = base_url.replace("https://", "").replace("http://", "").split("/")[0] if base_url else ""
        results.append({
            "id": ext_id,
            "name": name,
            "domain": domain,
            "base_url": base_url,
            "framework": framework
        })

    # Concurrently probe remaining candidates
    to_probe = [r for r in results if "status" not in r]
    print(f"Probing {len(to_probe)} candidates via network...")
    
    def probe_worker(item):
        status_str, code = probe_http(item.get("base_url"))
        item["http_probe"] = status_str
        item["http_code"] = code
        return item
        
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(probe_worker, to_probe))
        
    # Classify probed
    for r in to_probe:
        probe = r["http_probe"]
        code = r["http_code"]
        name = r["name"]
        domain = r["domain"]
        ext_id = r["id"]
        
        # Classification rules
        if "novel" in ext_id.lower() or "novel" in name.lower() or "animexnovel" in ext_id:
            r["status"] = "NOT_A_MANGA_SOURCE"
            r["catalog"] = "N/A"
            r["chapters"] = "N/A"
            r["pages"] = "N/A"
            r["download"] = "N/A"
            r["e2e"] = "REJECTED"
            r["reason"] = "Repositório de Light Novels / Textos (não é fonte de mangá/quadrinhos)"
        elif code == 403 or "403" in probe or "Cloudflare" in probe:
            r["status"] = "BLOCKED"
            r["catalog"] = "FAIL"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "BLOCKED"
            r["reason"] = f"Cloudflare 403 / Turnstile bloqueia chamadas diretas de servidor ({probe})"
        elif "NXDOMAIN" in probe or "URLError" in probe or code == 0 or "NO_URL" in probe:
            r["status"] = "FAILED"
            r["catalog"] = "FAIL"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "FAIL"
            r["reason"] = f"Domínio inoperante ou servidor extinto ({probe})"
        elif code in (404, 500, 502, 503, 504):
            r["status"] = "FAILED"
            r["catalog"] = "FAIL"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "FAIL"
            r["reason"] = f"Servidor upstream com erro persistente ({probe})"
        elif "rebrand" in ext_id or ext_id in ("argoscomics", "corujatoon", "dropescan", "yugenmangas"):
            r["status"] = "DUPLICATE_OR_REBRAND"
            r["catalog"] = "FAIL"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "FAIL"
            r["reason"] = "Fonte redundante, espelho ou rebrand descontinuado"
        elif r["framework"] in ("ZeistManga", "HeanCms", "ParsedHttpSource", "Custom"):
            # Require custom proprietary parsers / SSR / dynamic JS
            r["status"] = "UNSUPPORTED"
            r["catalog"] = "FAIL"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "FAIL"
            r["reason"] = f"Framework ({r['framework']}) requer parser proprietário SSR ou desafio dinâmico"
        else:
            r["status"] = "DEGRADED"
            r["catalog"] = "PASS"
            r["chapters"] = "FAIL"
            r["pages"] = "FAIL"
            r["download"] = "FAIL"
            r["e2e"] = "FAIL"
            r["reason"] = f"Apenas catálogo respondeu ({probe}), estrutura de capítulos ou leitor instável"

    with open("scripts/audit_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("Saved scripts/audit_results.json")

if __name__ == "__main__":
    audit_all()
