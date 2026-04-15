import json
import os
import re

def bundle_dbt_docs(project_path):
    target_path = os.path.join(project_path, 'target')
    docs_path = os.path.join(project_path, 'docs')
    
    if not os.path.exists(docs_path):
        os.makedirs(docs_path)

    print(f"📦 Bundling dbt docs from {target_path}...")

    # Read the files
    with open(os.path.join(target_path, 'index.html'), 'r') as f:
        html = f.read()

    with open(os.path.join(target_path, 'manifest.json'), 'r') as f:
        manifest = json.load(f)

    with open(os.path.join(target_path, 'catalog.json'), 'r') as f:
        catalog = json.load(f)

    # Convert JSON to string
    manifest_json = json.dumps(manifest)
    catalog_json = json.dumps(catalog)

    # Patterns to look for (different dbt versions have different minification)
    patterns = [
        # Pattern 1: n = [o("manifest", "manifest.json" + t), o("catalog", "catalog.json" + t)]
        (r'n\s*=\s*\[o\("manifest",\s*"manifest.json"\s*\+\s*t\),\s*o\("catalog",\s*"catalog.json"\s*\+\s*t\)\]', 
         f'n = [{{label: "manifest", data: {manifest_json}}}, {{label: "catalog", data: {catalog_json}}}]'),
        
        # Pattern 2: o=[i("manifest","any.json")]
        (r'o=\[i\("manifest","[^"]+"\)\]', 
         f'o=[{{label: "manifest", data: {manifest_json}}}]'),
        
        # Pattern 3: p=[i("catalog","any.json")]
        (r'p=\[i\("catalog","[^"]+"\)\]', 
         f'p=[{{label: "catalog", data: {catalog_json}}}]')
    ]

    found_any = False
    for pattern, replacement in patterns:
        if re.search(pattern, html):
            # Use a targeted replacement to avoid re.sub escape issues
            # Actually, standard replace is safer for such large strings
            match = re.search(pattern, html).group(0)
            html = html.replace(match, replacement)
            print(f"✅ Injected data using pattern: {pattern[:50]}...")
            found_any = True

    if not found_any:
        print("⚠️ Warning: No injection patterns found. The documentation might still try to load external files.")
        # Fallback for some newer layouts that use MANIFEST.JSON INLINE DATA
        if 'MANIFEST.JSON INLINE DATA' in html:
            html = html.replace('"MANIFEST.JSON INLINE DATA"', manifest_json)
            html = html.replace('"CATALOG.JSON INLINE DATA"', catalog_json)
            print("✅ Injected data into placeholder strings.")
            found_any = True

    output_file = os.path.join(docs_path, 'index.html')
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"✅ Successfully bundled dbt docs into {output_file}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bundle_dbt_docs(current_dir)
