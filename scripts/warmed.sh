#!/bin/bash

HOST="${1:-https://clob.polymarket.com/}"
ITERATIONS="${2:-100}"

# Build curl args: reuse one connection for all requests
CURL_ARGS="-s"
FORMAT=""
for i in $(seq 1 "$ITERATIONS"); do
  CURL_ARGS="$CURL_ARGS -o /dev/null"
  FORMAT="$FORMAT%{time_starttransfer}\n"
done

echo "Benchmarking $HOST ($ITERATIONS requests, single connection)"
echo "-------------------------------------------"

# Run all requests on one persistent connection, collect TTFBs
ttfbs=$(curl $CURL_ARGS -w "$FORMAT" $(printf "$HOST %.0s" $(seq 1 "$ITERATIONS")))

i=0
sum=0
min=""
max=""

while IFS= read -r ttfb; do
  i=$((i + 1))
  ms=$(echo "$ttfb * 1000" | bc)

  if [ -z "$min" ] || [ "$(echo "$ttfb < $min" | bc)" -eq 1 ]; then min=$ttfb; fi
  if [ -z "$max" ] || [ "$(echo "$ttfb > $max" | bc)" -eq 1 ]; then max=$ttfb; fi

  sum=$(echo "$sum + $ttfb" | bc)

  if [ "$i" -eq 1 ]; then
    printf "%3d  TTFB: %ss (cold - includes DNS/TLS)\n" "$i" "$ttfb"
  else
    printf "%3d  TTFB: %ss\n" "$i" "$ttfb"
  fi
done <<< "$ttfbs"

# Averages: overall and warmed-only (excluding first request)
avg_all=$(echo "scale=6; $sum / $i" | bc)
sum_warm=$(echo "$sum - $(echo "$ttfbs" | head -1)" | bc)
avg_warm=$(echo "scale=6; $sum_warm / ($i - 1)" | bc)
min_ms=$(echo "$min * 1000" | bc)
max_ms=$(echo "$max * 1000" | bc)
avg_all_ms=$(echo "$avg_all * 1000" | bc)
avg_warm_ms=$(echo "$avg_warm * 1000" | bc)

echo "-------------------------------------------"
printf "Avg TTFB (all):        %ss  (%sms)\n" "$avg_all" "$avg_all_ms"
printf "Avg TTFB (warmed):     %ss  (%sms)\n" "$avg_warm" "$avg_warm_ms"
printf "Min:                   %ss  (%sms)\n" "$min" "$min_ms"
printf "Max:                   %ss  (%sms)\n" "$max" "$max_ms"
