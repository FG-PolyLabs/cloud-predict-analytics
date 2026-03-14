#!/usr/bin/env bash
# backfill-resume.sh — resume backfill from where it left off after network interruption.
# miami was interrupted at 2026-02-16 (that execution launched OK, resume from 2026-02-17).
# Still need: miami (2026-02-17 onward), toronto, buenos-aires, tokyo.

set -euo pipefail

PROJECT_ID="fg-polylabs"
REGION="us-central1"
JOB_NAME="weather-polymarket"
END_DATE="2026-03-11"

fire_city() {
  local CITY="$1"
  local START_DATE="$2"
  echo "==> Backfilling city=${CITY}  from=${START_DATE}  to=${END_DATE}"

  local CURRENT="${START_DATE}"
  while [[ "${CURRENT}" < "${END_DATE}" || "${CURRENT}" == "${END_DATE}" ]]; do
    echo "    Firing ${CITY} ${CURRENT} ..."
    gcloud run jobs execute "${JOB_NAME}" \
      --region="${REGION}" \
      --project="${PROJECT_ID}" \
      --args="--date=${CURRENT},--city=${CITY},--no-volume" \
      --no-wait
    CURRENT=$(date -d "${CURRENT} + 1 day" +%Y-%m-%d)
  done

  echo "    Done firing for ${CITY}."
}

TOTAL_FIRED=0

# miami: resume from 2026-02-17 (2026-02-16 was already launched)
fire_city "miami" "2026-02-17"
fire_city "toronto" "2026-02-03"
fire_city "buenos-aires" "2026-02-03"
fire_city "tokyo" "2026-03-10"

echo ""
echo "==> Resume backfill complete."
