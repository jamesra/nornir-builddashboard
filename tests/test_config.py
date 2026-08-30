"""Configuration parsing contracts for the dashboard (chunk 11 findings)."""
import os
import unittest
from unittest.mock import patch

from nornir_dashboard.config import (
    DEFAULT_HTTP_HOST,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SWEEP_INTERVAL,
    DEFAULT_STALE_AFTER_SECONDS,
    DashboardConfig,
)

_NUMERIC_VARS = (
    "NORNIR_MQTT_PORT",
    "NORNIR_MQTT_KEEPALIVE",
    "NORNIR_DASHBOARD_PORT",
    "NORNIR_DASHBOARD_MAX_EVENTS",
    "NORNIR_DASHBOARD_STALE_AFTER",
    "NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL",
    "NORNIR_DASHBOARD_RETENTION_DAYS",
    "NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL",
)


def _clean_env(**overrides: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("NORNIR_MQTT_", "NORNIR_DASHBOARD_"))}
    env.update(overrides)
    return env


class TestNumericEnvParsing(unittest.TestCase):
    """C11-B016: a malformed numeric variable must not abort startup."""

    def test_garbage_numeric_values_fall_back_to_defaults(self) -> None:
        garbage = {name: "not-a-number" for name in _NUMERIC_VARS}
        with patch.dict(os.environ, _clean_env(**garbage), clear=True):
            config = DashboardConfig()

        self.assertEqual(config.mqtt_port, 1883)
        self.assertEqual(config.mqtt_keepalive, 60)
        self.assertEqual(config.http_port, 8087)
        self.assertEqual(config.max_events_per_run, 0)
        self.assertEqual(config.stale_after_seconds, DEFAULT_STALE_AFTER_SECONDS)
        self.assertEqual(config.retention_days, DEFAULT_RETENTION_DAYS)
        self.assertEqual(config.retention_sweep_interval,
                         DEFAULT_RETENTION_SWEEP_INTERVAL)

    def test_empty_values_fall_back_to_defaults(self) -> None:
        blanks = {name: "   " for name in _NUMERIC_VARS}
        with patch.dict(os.environ, _clean_env(**blanks), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.mqtt_port, 1883)
        self.assertEqual(config.retention_days, DEFAULT_RETENTION_DAYS)

    def test_valid_values_are_honored(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_PORT="9001",
                NORNIR_DASHBOARD_MAX_EVENTS="1500",
                NORNIR_DASHBOARD_RETENTION_DAYS="2.5"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.http_port, 9001)
        self.assertEqual(config.max_events_per_run, 1500)
        self.assertEqual(config.retention_days, 2.5)


class TestRetentionSweepInterval(unittest.TestCase):
    """C11-B007: a non-positive sweep interval would busy-loop the sweeper."""

    def test_zero_interval_falls_back_to_default(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL="0"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.retention_sweep_interval,
                         DEFAULT_RETENTION_SWEEP_INTERVAL)

    def test_negative_interval_falls_back_to_default(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL="-5"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.retention_sweep_interval,
                         DEFAULT_RETENTION_SWEEP_INTERVAL)

    def test_retention_days_zero_still_disables_retention(self) -> None:
        """retention_days<=0 is a supported way to disable retention entirely."""
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_RETENTION_DAYS="0"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.retention_days, 0.0)


class TestMaxEventsNormalization(unittest.TestCase):
    """C11-D002: negative limits normalize to the documented "unlimited" value."""

    def test_negative_max_events_normalizes_to_zero(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_MAX_EVENTS="-10"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.max_events_per_run, 0)


class TestBindAndAuthDefaults(unittest.TestCase):
    """C11-S001: default bind is loopback and the token is opt-in."""

    def test_default_host_is_loopback(self) -> None:
        with patch.dict(os.environ, _clean_env(), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.http_host, DEFAULT_HTTP_HOST)
        self.assertEqual(config.http_host, "127.0.0.1")
        self.assertIsNone(config.auth_token)
        self.assertTrue(config.allow_delete)

    def test_explicit_host_is_honored(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_HOST="0.0.0.0"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.http_host, "0.0.0.0")

    def test_token_and_delete_flag(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_TOKEN="  s3cret  ",
                NORNIR_DASHBOARD_ALLOW_DELETE="false"), clear=True):
            config = DashboardConfig()
        self.assertEqual(config.auth_token, "s3cret")
        self.assertFalse(config.allow_delete)

    def test_blank_token_is_none(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_TOKEN="   "), clear=True):
            config = DashboardConfig()
        self.assertIsNone(config.auth_token)

    def test_unparsable_delete_flag_keeps_default(self) -> None:
        with patch.dict(os.environ, _clean_env(
                NORNIR_DASHBOARD_ALLOW_DELETE="maybe"), clear=True):
            config = DashboardConfig()
        self.assertTrue(config.allow_delete)


if __name__ == "__main__":
    unittest.main()
