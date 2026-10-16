#!/usr/bin/env bash
# Usage: bash download.sh CACHE_DIR FILENAME SHA512 URL [FALLBACK_URL ...]
# Call with an exclusive BuildKit cache mount (sharing=locked).
set -euo pipefail
if (( $# < 4 )); then
    echo 'Usage: download.sh CACHE_DIR FILENAME SHA512 URL [FALLBACK_URL ...]' >&2
    exit 2
fi
cache=$1 filename=$2 checksum=$3
shift 3
if [[ ! $filename =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ || ! $checksum =~ ^[a-f0-9]{128}$ ]]; then
    echo 'Invalid filename or SHA-512' >&2
    exit 2
fi
directory="$cache/$checksum"
destination="$directory/$filename"
mkdir -p "$directory"
valid() {
    [[ -f $1 ]] && printf '%s  %s\n' "$checksum" "$1" | sha512sum --check --status
}
promote() {
    mv -f "$partial" "$destination"
    echo "Verified SHA-512: $filename"
}
trap 'echo "Download interrupted; partial file retained in builder cache" >&2; exit 130' INT TERM
if valid "$destination"; then
    echo "Cache hit (SHA-512 verified): $filename"
    exit 0
fi
if [[ -e $destination ]]; then
    echo "Discarding corrupt cached binary: $filename" >&2
    rm -f "$destination"
fi
deadline=$((SECONDS + 1800))
for url in "$@"; do
    # Never append bytes from different mirrors to the same partial file.
    source_key=$(printf '%s' "$url" | sha256sum)
    partial="$directory/${source_key%% *}.part"
    for attempt in 1 2; do
        if valid "$partial"; then
            promote
            exit 0
        fi
        remaining=$((deadline - SECONDS))
        if (( remaining <= 0 )); then
            echo "Download budget exhausted (30 minutes): $filename; partial files retained" >&2
            exit 1
        fi
        offset=0
        if [[ -f $partial ]]; then offset=$(stat -c %s "$partial"); fi
        echo "Downloading $filename: source=$url attempt=$attempt resume_bytes=$offset"
        code=0
        transfer=$(curl --fail --location --proto '=http,https' --proto-redir '=https' \
            --connect-timeout 15 --max-time "$remaining" \
            --speed-limit 65536 --speed-time 60 --continue-at - \
            --write-out '%{http_code} %{size_download} %{speed_download}' \
            --output "$partial" "$url") || code=$?
        http_code=${transfer%% *}
        echo "Transfer metrics (HTTP status, downloaded bytes, B/s): $transfer"
        if (( code == 130 || code == 143 )); then exit 130; fi
        if valid "$partial"; then
            promote
            exit 0
        fi
        if (( code == 0 )); then
            echo "SHA-512 mismatch: $filename; discarding response and trying next source" >&2
            rm -f "$partial"
            break
        elif (( code == 33 || code == 36 )); then
            echo 'Server cannot resume; restarting this source from byte zero' >&2
            rm -f "$partial"
        elif (( code == 22 )); then
            # Includes HTTP 416 for an oversized/stale partial. A complete valid
            # partial was already accepted above. Try fresh once, then fallback.
            if [[ $http_code == 416 ]] && (( offset > 0 )); then
                rm -f "$partial"
            else
                break
            fi
        fi
        echo "Transfer failed (curl $code); retry/fallback will preserve reusable partial data" >&2
    done
    echo "Switching download source: $filename" >&2
done
echo "All download sources failed: $filename; partial files retained in builder cache" >&2
exit 1
