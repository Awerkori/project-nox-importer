#!/usr/bin/env python3
import os
import sys
import subprocess
import zipfile

def main():
    print("\n========================================")
    print("📦 Project Nox Importer - DIScloud Packager")
    print("========================================\n")

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    build_dir = os.path.join(root_dir, "build")
    output_dir = os.path.join(root_dir, "dist-discloud")
    zip_path = os.path.join(output_dir, "project-nox-importer.zip")

    # 1. Compile TypeScript
    print("🔨 Step 1: Compiling TypeScript to build/...")
    res = subprocess.run(["npm", "run", "build"], cwd=root_dir)
    if res.returncode != 0:
        print("❌ TypeScript compilation failed!")
        sys.exit(1)

    entry_point = os.path.join(build_dir, "index.js")
    if not os.path.exists(entry_point):
        print(f"❌ Entry point {entry_point} not found!")
        sys.exit(1)
    print("✅ Build verified: build/index.js exists")

    # 2. Prepare destination
    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(zip_path):
        os.remove(zip_path)

    # 3. Assemble clean ZIP
    print("\n📦 Step 2: Assembling clean DIScloud deployment ZIP...")
    allowed_roots = ["build", "config", "discloud.config", "package.json", "package-lock.json", "README.md"]

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item in allowed_roots:
            full_path = os.path.join(root_dir, item)
            if not os.path.exists(full_path):
                continue
            if os.path.isfile(full_path):
                zf.write(full_path, arcname=item)
            elif os.path.isdir(full_path):
                for root, _, files in os.walk(full_path):
                    for f in files:
                        file_path = os.path.join(root, f)
                        arcname = os.path.relpath(file_path, root_dir)
                        zf.write(file_path, arcname=arcname)

    # 4. Security Audit of ZIP contents
    print("\n🔍 Step 3: Auditing ZIP contents & security boundaries...")
    errors = []
    file_list = []
    forbidden_substr = [".env", "node_modules", "tests", "src/", ".git", "coverage", ".temp"]

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            filename = info.filename
            file_list.append((filename, info.file_size))

            for fb in forbidden_substr:
                if fb in filename:
                    errors.append(f"FORBIDDEN FILE/DIR in ZIP: {filename}")

            if filename.endswith((".js", ".json", ".config", ".md", ".txt")):
                content = zf.read(info.filename).decode("utf-8", errors="ignore")
                if "service_role" in content and "eyJ" in content:
                    errors.append(f"POTENTIAL SERVICE_ROLE JWT LEAK in {filename}")
                if "NOX_STORAGE_BRIDGE_TOKEN=" in content:
                    errors.append(f"POTENTIAL BRIDGE TOKEN LEAK in {filename}")
                if "KURO_PASSWORD=" in content:
                    errors.append(f"POTENTIAL KURO PASSWORD LEAK in {filename}")
                if "TELEGRAM_BOT_TOKEN=" in content and not filename.endswith("discloud.py"):
                    errors.append(f"POTENTIAL TELEGRAM BOT TOKEN LEAK in {filename}")

    if errors:
        print("❌ SECURITY AUDIT FAILED:")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)

    print(f"✅ AUDIT PASSED: {len(file_list)} files verified.")
    print("\nIncluded files in ZIP:")
    for name, sz in sorted(file_list):
        print(f"  - {name:<45} ({sz:>8} bytes)")

    zip_size = os.path.getsize(zip_path) / 1024
    print(f"\n🎉 Clean DIScloud package ready at:\n   {zip_path} ({zip_size:.2f} KB)\n")

if __name__ == "__main__":
    main()
