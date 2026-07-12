"""Runtime configuration for the nornir dashboard, resolved from the environment.

Environment variables mirror the names used by ``nornir_shared.mqtt_config`` so a
single set of variables configures both publishers and the dashboard.
"""
import os


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
            os.environ.get("NORNIR_DASHBOARD_MAX_EVENTS", "5000")
        )


def load_config() -> DashboardConfig:
    """Return dashboard configuration resolved from the current environment."""
    return DashboardConfig()
