"""C11-D005: logging joins the unified Nornir session when one is configured."""
import os
import sys
import types
import unittest
from unittest.mock import patch


class TestSetupLogging(unittest.TestCase):
    def setUp(self) -> None:
        from nornir_dashboard.main import _setup_logging

        self._setup_logging = _setup_logging

    def test_uses_nornir_shared_when_log_root_is_set(self) -> None:
        calls = []
        fake_misc = types.ModuleType("nornir_shared.misc")
        fake_misc.SetupLogging = lambda **kwargs: calls.append(kwargs)  # type: ignore[attr-defined]
        fake_pkg = types.ModuleType("nornir_shared")
        fake_pkg.misc = fake_misc  # type: ignore[attr-defined]

        modules = {"nornir_shared": fake_pkg, "nornir_shared.misc": fake_misc}
        with patch.dict(sys.modules, modules), \
                patch.dict(os.environ, {"NORNIR_LOG_ROOT": "/tmp/logs"}), \
                patch("logging.basicConfig") as basic_config:
            self._setup_logging()

        self.assertEqual(len(calls), 1)
        basic_config.assert_not_called()

    def test_falls_back_to_basic_config_without_log_root(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "NORNIR_LOG_ROOT"}
        with patch.dict(os.environ, env, clear=True), \
                patch("logging.basicConfig") as basic_config:
            self._setup_logging()
        basic_config.assert_called_once()

    def test_falls_back_when_nornir_shared_is_absent(self) -> None:
        real_import = __import__

        def blocked_import(name, *args, **kwargs):
            if name.startswith("nornir_shared"):
                raise ImportError("no nornir_shared in the container image")
            return real_import(name, *args, **kwargs)

        with patch.dict(os.environ, {"NORNIR_LOG_ROOT": "/tmp/logs"}), \
                patch("builtins.__import__", side_effect=blocked_import), \
                patch("logging.basicConfig") as basic_config:
            self._setup_logging()
        basic_config.assert_called_once()


if __name__ == "__main__":
    unittest.main()
