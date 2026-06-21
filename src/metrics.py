from prometheus_client import Counter, Histogram

events_synced_total = Counter(
    "omnisync_events_synced_total",
    "Total number of events synced",
    ["source", "status"],
)

sync_duration_seconds = Histogram(
    "omnisync_sync_duration_seconds",
    "Duration of sync operations in seconds",
    ["source"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)
