import importlib.machinery
import importlib.util
import io
import subprocess
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
LOADER = importlib.machinery.SourceFileLoader("crawler_manager", str(ROOT / "crawler-manager"))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
crawler_manager = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(crawler_manager)


class ProcessInspectionTests(unittest.TestCase):
    def test_parse_ps_process_line_preserves_command_arguments(self):
        line = (
            " 26280 S+ 0.4 12345 05:17 "
            "/usr/local/bin/python /Users/example/railpull/crawler-manager _supervise"
        )

        info = crawler_manager._parse_ps_process_line(line)

        self.assertEqual(info["pid"], 26280)
        self.assertEqual(info["state"], "S+")
        self.assertEqual(info["cpuPercent"], "0.4")
        self.assertEqual(info["rssKiB"], "12345")
        self.assertEqual(info["elapsed"], "05:17")
        self.assertTrue(info["command"].endswith("crawler-manager _supervise"))

    def test_process_info_falls_back_to_ps_without_procfs(self):
        expected = {
            "pid": 26280,
            "state": "S+",
            "command": "/usr/local/bin/python /Users/example/railpull/crawler-manager _supervise",
        }

        with patch.object(crawler_manager.os, "kill"), patch.object(
            crawler_manager, "_procfs_process_info", return_value=None
        ), patch.object(crawler_manager, "_ps_process_info", return_value=expected):
            actual = crawler_manager._process_info(26280)

        self.assertEqual(actual, expected)

    def test_manager_and_crawler_commands_are_identified_from_ps_command_line(self):
        manager_info = {
            "pid": 10,
            "state": "S+",
            "command": "/usr/bin/python /Users/example/railpull/crawler-manager _supervise",
        }
        crawler_info = {
            "pid": 11,
            "state": "S",
            "command": "/usr/bin/python -u /Users/example/railpull/ntes/crawl.py",
        }

        with patch.object(crawler_manager, "_process_info", return_value=manager_info):
            self.assertTrue(crawler_manager.manager_process(10))
        with patch.object(crawler_manager, "_process_info", return_value=crawler_info):
            self.assertTrue(crawler_manager.crawler_process(11))

    def test_zombie_process_is_not_considered_alive(self):
        zombie = {"pid": 12, "state": "Z", "command": "[python] <defunct>"}

        with patch.object(crawler_manager.os, "kill"), patch.object(
            crawler_manager, "_procfs_process_info", return_value=None
        ), patch.object(crawler_manager, "_ps_process_info", return_value=zombie):
            self.assertIsNone(crawler_manager._process_info(12))

    def test_ps_stats_are_available_without_procfs(self):
        info = {
            "pid": 11,
            "state": "S",
            "command": "/usr/bin/python -u /Users/example/railpull/ntes/crawl.py",
            "cpuPercent": "1.25",
            "rssKiB": "2048",
            "elapsed": "01:02:03",
        }

        with patch.object(crawler_manager, "_ps_process_info", return_value=info), patch.object(
            crawler_manager.time, "time", return_value=100000.0
        ):
            stats = crawler_manager._ps_process_stats(11)

        self.assertEqual(stats["cpuPercent"], 1.25)
        self.assertEqual(stats["memoryBytes"], 2048 * 1024)
        self.assertEqual(stats["runtimeSeconds"], 3723.0)
        self.assertEqual(stats["startedEpoch"], 96277.0)


class StatusTests(unittest.TestCase):
    def test_persisted_running_state_is_reported_stale_without_processes(self):
        crawl = {"phase": "starting", "runStartedAt": "2026-09-06T11:13:13+00:00"}
        state = {"status": "RUNNING", "managerStartedAt": "2026-09-06T11:13:13+00:00"}
        output = io.StringIO()

        with patch.object(crawler_manager, "read_pid", side_effect=[26280, 33005]), patch.object(
            crawler_manager, "manager_process", return_value=False
        ), patch.object(crawler_manager, "crawler_process", return_value=False), patch.object(
            crawler_manager, "load_json", side_effect=[crawl, state]
        ), redirect_stdout(output):
            crawler_manager.print_status()

        rendered = output.getvalue()
        self.assertIn("Status: STALE", rendered)
        self.assertIn("Last error: manager and crawler processes are not running", rendered)
        self.assertNotIn("Status: RUNNING", rendered)


class PsCommandTests(unittest.TestCase):
    def test_ps_query_uses_no_shell_and_parses_macos_style_output(self):
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=" 26280 S+ 0.0 8192 05:17 /usr/bin/python crawler-manager _supervise\n",
            stderr="",
        )

        with patch.object(crawler_manager.subprocess, "run", return_value=completed) as run:
            info = crawler_manager._ps_process_info(26280)

        self.assertEqual(info["pid"], 26280)
        self.assertEqual(run.call_args.kwargs["timeout"], crawler_manager.PROCESS_QUERY_TIMEOUT)
        self.assertFalse(run.call_args.kwargs.get("shell", False))
        self.assertEqual(run.call_args.args[0][0], "ps")


if __name__ == "__main__":
    unittest.main()
