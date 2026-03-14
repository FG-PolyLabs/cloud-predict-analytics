#!/usr/bin/env bash
# backfill-all.sh — fire historical backfill jobs for all cities (excluding london and singapore).
# London is already backfilling; singapore has no history (started today).
#
# Usage: ./scripts/backfill-all.sh

set -euo pipefail

PROJECT_ID="fg-polylabs"
REGION="us-central1"
JOB_NAME="weather-polymarket"
END_DATE="2026-03-11"

# city -> start_date (first market date)
declare -A CITY_START_DATES=(
  [buenos-aires]="2026-02-03"
  [ankara]="2026-02-03"
  [seoul]="2026-02-03"
  [dallas]="2026-02-03"
  [miami]="2026-02-03"
  [nyc]="2026-02-03"
  [toronto]="2026-02-03"
  [chicago]="2026-02-03"
  [paris]="2026-02-15"
  [tokyo]="2026-03-10"
)

TOTAL_FIRED=0

for CITY in "${!CITY_START_DATES[@]}"; do
  START_DATE="${CITY_START_DATES[$CITY]}"
  echo "==> Backfilling city=${CITY}  from=${START_DATE}  to=${END_DATE}"

  CURRENT="${START_DATE}"
  while [[ "${CURRENT}" < "${END_DATE}" || "${CURRENT}" == "${END_DATE}" ]]; do
    echo "    Firing ${CITY} ${CURRENT} ..."
    gcloud run jobs execute "${JOB_NAME}" \
      --region="${REGION}" \
      --project="${PROJECT_ID}" \
      --args="--date=${CURRENT},--city=${CITY},--no-volume" \
      --no-wait
    TOTAL_FIRED=$((TOTAL_FIRED + 1))
    CURRENT=$(date -d "${CURRENT} + 1 day" +%Y-%m-%d)
  done

  echo "    Done firing for ${CITY}."
done

echo ""
echo "==> Backfill complete. Total jobs fired: ${TOTAL_FIRED}"
