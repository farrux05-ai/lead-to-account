import json
import os

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

    # Step 1: Look for the manifest and catalog variables in index.html
    # In modern dbt, they look something like:
    # o=[i("manifest","7.json")]
    # p=[i("catalog","7.json")]

    # We use a simple replacement for the most common pattern
    # The pattern in index.html is often minified.
    
    # Replacement for Manifest
    # Search for the pattern o=[i("manifest","any_version.json")] and replace with data
    import re
    
    # We use string find/replace to avoid the \u escape issue in re.sub
    # We find the string that looks like o=[i("manifest","...")]
    
    manifest_target = re.search(r'o=\[i\("manifest","[^"]+"\)\]', html)
    if manifest_target:
        target_str = manifest_target.group(0)
        html = html.replace(target_str, f'o=[{{label: "manifest", data: {manifest_json}}}]')
        print("✅ Injected Manifest")

    catalog_target = re.search(r'p=\[i\("catalog","[^"]+"\)\]', html)
    if catalog_target:
        target_str = catalog_target.group(0)
        html = html.replace(target_str, f'p=[{{label: "catalog", data: {catalog_json}}}]')
        print("✅ Injected Catalog")

    output_file = os.path.join(docs_path, 'index.html')
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"✅ Successfully bundled dbt docs into {output_file}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bundle_dbt_docs(current_dir)
