from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.config import settings
from src.logging_config import logger

_tracer_provider = None


def init_otel():
    global _tracer_provider

    resource = Resource.create(
        {
            "service.name": "omnisync",
            "service.version": "0.1.0",
        }
    )

    exporter = OTLPSpanExporter(
        endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT,
        insecure=True,
    )

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer_provider = provider

    logger.info("otel_initialized", endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT)


def shutdown_otel():
    if _tracer_provider:
        _tracer_provider.shutdown()
        logger.info("otel_shutdown")


def get_tracer(name: str = "omnisync") -> trace.Tracer:
    return trace.get_tracer(name)
