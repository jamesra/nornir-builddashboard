"""Runtime configuration for the nornir dashboard, resolved from the environment.

Environment variables mirror the names used by ``nornir_shared.mqtt_config`` so a
single set of variables configures both publishers and the dashboard.

Malformed values never abort startup: each numeric variable falls back to its
documented default and logs a warning, so a typo in a Compose file degrades to
default behavior instead of crashing the service.
"""
import logging
import os

logger = logging.getLogger(__name__)

# Stale detection is always on; ``NORNIR_DASHBOARD_STALE_AFTER<=0`` is rejected.
DEFAULT_STALE_AFTER_SECONDS = 600.0
DEFAULT_STALE_SWEEP_INTERVAL = 60.0
DEFAULT_RETENTION_DAYS = 30.0
DEFAULT_RETENTION_SWEEP_INTERVAL = 86400.0

# Bind to loopback unless told otherwise. The dashboard has no authentication of
# its own and exposes a destructive DELETE, so a network-visible default would
# hand run-history deletion to anything that can reach the port. The container
# image sets NORNIR_DASHBOARD_HOST=0.0.0.0 explicitly, which is correct there
# because the container boundary is the network boundary.
DEFAULT_HTTP_HOST = "127.0.0.1"


def _positive_or_default(raw: float, default: float, env_name: str) -> float:
    """Return *raw* when positive; otherwise log and return *default*."""
    if raw > 0:
        return raw
    logger.warning(
        "%s=%s is invalid (stale sweep cannot be disabled); using %s",
        env_name, raw, default,
    )
    return default


def _env_number(env_name: str, default: float, cast, *, require_positive: bool) -> float:
    """Read a numeric environment variable, falling back to *default* on bad input."""
    raw = os.environ.get(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        value = cast(raw)
    except (TypeError, ValueError):
        logger.warning(
            "%s=%r is not a number; using %s", env_name, raw, default)
        return default
    if require_positive and value <= 0:
        return _positive_or_default(value, default, env_name)
    return value


def _env_int(env_name: str, default: int, *, require_positive: bool = False) -> int:
    return int(_env_number(env_name, default, int, require_positive=require_positive))


def _env_float(env_name: str, default: float, *, require_positive: bool = False) -> float:
    return float(_env_number(env_name, default, float, require_positive=require_positive))


def _env_bool(env_name: str, default: bool) -> bool:
    """Read a boolean environment variable ("1/true/yes/on" versus "0/false/no/off")."""
    raw = os.environ.get(env_name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    logger.warning("%s=%r is not a boolean; using %s", env_name, raw, default)
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
    auth_token: str | None
    allow_delete: bool

    def __init__(self) -> None:
        self.mqtt_host = os.environ.get("NORNIR_MQTT_HOST", "127.0.0.1")
        self.mqtt_port = _env_int("NORNIR_MQTT_PORT", 1883, require_positive=True)
        self.mqtt_keepalive = _env_int("NORNIR_MQTT_KEEPALIVE", 60, require_positive=True)
        self.topic_root = os.environ.get("NORNIR_MQTT_RUN_TOPIC_ROOT", "nornir/run")
        self.database_path = os.environ.get(
            "NORNIR_DASHBOARD_DB", os.path.join(os.getcwd(), "nornir-dashboard.db")
        )
        self.http_host = os.environ.get("NORNIR_DASHBOARD_HOST", DEFAULT_HTTP_HOST)
        self.http_port = _env_int("NORNIR_DASHBOARD_PORT", 8087, require_positive=True)

        # 0 means "keep every event"; retention is then the only bound on growth.
        self.max_events_per_run = _env_int("NORNIR_DASHBOARD_MAX_EVENTS", 0)
        if self.max_events_per_run <= 0:
            self.max_events_per_run = 0

        self.stale_after_seconds = _env_float(
            "NORNIR_DASHBOARD_STALE_AFTER", DEFAULT_STALE_AFTER_SECONDS,
            require_positive=True)
        self.stale_sweep_interval = _env_float(
            "NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL", DEFAULT_STALE_SWEEP_INTERVAL,
            require_positive=True)

        # retention_days <= 0 legitimately disables retention, so it is not
        # forced positive; the sweep *interval* is, because a zero interval
        # turns the sweeper into a busy loop.
        self.retention_days = _env_float(
            "NORNIR_DASHBOARD_RETENTION_DAYS", DEFAULT_RETENTION_DAYS)
        self.retention_sweep_interval = _env_float(
            "NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL", DEFAULT_RETENTION_SWEEP_INTERVAL,
            require_positive=True)

        token = os.environ.get("NORNIR_DASHBOARD_TOKEN", "").strip()
        self.auth_token = token or None
        self.allow_delete = _env_bool("NORNIR_DASHBOARD_ALLOW_DELETE", True)

        self._warn_about_unbounded_growth()

    def _warn_about_unbounded_growth(self) -> None:
        """Warn when neither per-run pruning nor retention bounds the events table."""
        if self.max_events_per_run > 0:
            return
        if self.retention_days > 0:
            logger.info(
                "NORNIR_DASHBOARD_MAX_EVENTS is unset: event history is bounded only by "
                "the %.0f-day retention window", self.retention_days)
            return
        logger.warning(
            "NORNIR_DASHBOARD_MAX_EVENTS=0 and NORNIR_DASHBOARD_RETENTION_DAYS<=0: "
            "the events table has no bound and will grow until the disk fills")


def load_config() -> DashboardConfig:
    """Return dashboard configuration resolved from the current environment."""
    return DashboardConfig()
