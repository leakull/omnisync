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

dlq_events_total = Counter(
    "omnisync_dlq_events_total",
    "Total number of events sent to the dead-letter queue",
    ["source", "operation"],
)

outbox_published_total = Counter(
    "omnisync_outbox_published_total",
    "Total number of outbox events published to downstream consumers",
    ["status"],
)

s3_storage_failures_total = Counter(
    "omnisync_s3_storage_failures_total",
    "Total number of S3/MinIO storage failures (fell back to DB)",
    ["operation"],
)
