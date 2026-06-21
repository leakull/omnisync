from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.config import settings
from src.logging_config import logger

_tracer_provider = None
_celery_instrumented = False


def init_otel(service_name: str = "omnisync"):
    global _tracer_provider

    if _tracer_provider is not None:
        return _tracer_provider

    resource = Resource.create(
        {
            "service.name": service_name,
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

    logger.info(
        "otel_initialized", endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT, service=service_name
    )
    return provider


def instrument_fastapi(app: Any) -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except Exception as e:
        logger.warning("otel_fastapi_instrument_failed", error=str(e))


def instrument_sqlalchemy(engine: Any) -> None:
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        # AsyncEngine wraps a sync_engine which is what the instrumentor expects.
        target = getattr(engine, "sync_engine", engine)
        SQLAlchemyInstrumentor().instrument(engine=target)
    except Exception as e:
        logger.warning("otel_sqlalchemy_instrument_failed", error=str(e))


def instrument_celery() -> None:
    global _celery_instrumented
    if _celery_instrumented:
        return
    try:
        from opentelemetry.instrumentation.celery import CeleryInstrumentor

        CeleryInstrumentor().instrument()
        _celery_instrumented = True
    except Exception as e:
        logger.warning("otel_celery_instrument_failed", error=str(e))


def shutdown_otel():
    if _tracer_provider:
        _tracer_provider.shutdown()
        logger.info("otel_shutdown")


def get_tracer(name: str = "omnisync") -> trace.Tracer:
    return trace.get_tracer(name)
