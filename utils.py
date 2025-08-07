import re

import html

import oci

 

def extract_oci_info(oci_path):

    """

    Extract bucket, namespace, and prefix from an OCI URI.

    Example: oci://my-bucket@namespace-name/path/to/folder

    """

    match = re.match(r"oci://([^@]+)@([^/]+)/(.+)", oci_path)

    if not match:

        raise ValueError(f"Invalid OCI path: {oci_path}")

    return match.group(1), match.group(2), match.group(3)

 

 

def list_existing_parquet_folders(output_path):

    """

    List existing folder prefixes in the given OCI Object Storage output path.

    Used to skip already-processed Excel sheets.

    """

    try:

        signer = oci.auth.signers.get_resource_principals_signer()

        object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)

        bucket, namespace, prefix = extract_oci_info(output_path)

        prefix = prefix.rstrip("/") + "/"

 

        response = object_storage.list_objects(namespace, bucket, prefix=prefix, delimiter="/")

        return set([

            html.unescape(p.rstrip("/").split("/")[-1])

            for p in response.data.prefixes

        ])

    except Exception as e:

        print(f"Failed to list existing parquet folders: {e}")

        return set()

 

 

def make_unique_columns(columns):

    """

    1. Trim whitespace, replace non‑alphanumerics with '_'

    2. Lower‑case everything (optional but avoids 'Name' vs 'name')

    3. Replace blanks/None with 'col'

    4. Deduplicate with numeric suffixes _1, _2 ...

    """

    seen = {}

    unique = []

 

    for raw in columns:

        # 1‑2. normalise

        col = "" if raw is None else str(raw).strip()

        col = re.sub(r"[^\w]", "_", col)         # keep A‑Z a‑z 0‑9 _

        col = col.lower()                        # comment this out if you need case

 

        # 3. handle empty after cleaning

        if not col:

            col = "col"

 

        # 4. dedupe

        base = col

        count = seen.get(base, 0)

        new_col = base if count == 0 else f"{base}_{count}"

        while new_col in seen:

            count += 1

            new_col = f"{base}_{count}"

 

        unique.append(new_col)

        seen[base] = count + 1

        seen[new_col] = 1      # mark the generated name too

 

    return unique
