"""Runtime configuration for the nornir dashboard, resolved from the environment.

Environment variables mirror the names used by ``nornir_shared.mqtt_config`` so a
single set of variables configures both publishers and the dashboard.
"""
import logging
import os

logger = logging.getLogger(__name__)

# Stale detection is always on; ``NORNIR_DASHBOARD_STALE_AFTER<=0`` is rejected.
DEFAULT_STALE_AFTER_SECONDS = 600.0
DEFAULT_STALE_SWEEP_INTERVAL = 60.0


def _positive_or_default(raw: float, default: float, env_name: str) -> float:
    """Return *raw* when positive; otherwise log and return *default*."""
    if raw > 0:
        return raw
    logger.warning(
        "%s=%s is invalid (stale sweep cannot be disabled); using %s",
        env_name, raw, default,
    )
    return default


class DashboardConfig:
    """Resolved dashboard configuration values."""

    mqtt_host: str
    mqtt_port: int
    mqtt_keepalive: int
    topic_root: str
    database_path: str
    http_host: str
    http_port: int
    max_events_per_run: int
    stale_after_seconds: float
    stale_sweep_interval: float
    retention_days: float
    retention_sweep_interval: float

    def __init__(self) -> None:
        self.mqtt_host = os.environ.get("NORNIR_MQTT_HOST", "127.0.0.1")
        self.mqtt_port = int(os.environ.get("NORNIR_MQTT_PORT", "1883"))
        self.mqtt_keepalive = int(os.environ.get("NORNIR_MQTT_KEEPALIVE", "60"))
        self.topic_root = os.environ.get("NORNIR_MQTT_RUN_TOPIC_ROOT", "nornir/run")
        self.database_path = os.environ.get(
            "NORNIR_DASHBOARD_DB", os.path.join(os.getcwd(), "nornir-dashboard.db")
        )
        self.http_host = os.environ.get("NORNIR_DASHBOARD_HOST", "0.0.0.0")
        self.http_port = int(os.environ.get("NORNIR_DASHBOARD_PORT", "8087"))
        self.max_events_per_run = int(
            os.environ.get("NORNIR_DASHBOARD_MAX_EVENTS", "0")
        )
        self.stale_after_seconds = _positive_or_default(
            float(os.environ.get("NORNIR_DASHBOARD_STALE_AFTER",
                                 str(DEFAULT_STALE_AFTER_SECONDS))),
            DEFAULT_STALE_AFTER_SECONDS,
            "NORNIR_DASHBOARD_STALE_AFTER",
        )
        self.stale_sweep_interval = _positive_or_default(
            float(os.environ.get("NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL",
                                 str(DEFAULT_STALE_SWEEP_INTERVAL))),
            DEFAULT_STALE_SWEEP_INTERVAL,
            "NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL",
        )
        self.retention_days = float(
            os.environ.get("NORNIR_DASHBOARD_RETENTION_DAYS", "30")
        )
        self.retention_sweep_interval = float(
            os.environ.get("NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL", "86400")
        )


def load_config() -> DashboardConfig:
    """Return dashboard configuration resolved from the current environment."""
    return DashboardConfig()
