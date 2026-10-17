"""Create a reproducible Python dependency archive; no dependencies or network."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def package_code(destination, source=None, profile="ingest"):
    source = source or Path(__file__).resolve().parent
    if profile not in ("ingest", "streaming", "ml"):
        raise ValueError("Unknown application archive profile")
    packages = ["ecommerce_analytics"]
    if profile == "streaming":
        packages.extend(("ecommerce_streaming", "ecommerce_behavior"))
    if profile == "ml":
        packages.append("ecommerce_ml")
    destination = Path(destination)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(path for package in packages for path in (source / package).rglob("*.py")):
            entry = zipfile.ZipInfo(path.relative_to(source).as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o644 << 16
            archive.writestr(entry, path.read_bytes())
    return {"archive": str(destination), "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--profile", choices=("ingest", "streaming", "ml"), default="ingest")
    args = parser.parse_args()
    print(json.dumps(package_code(args.output, profile=args.profile)))
