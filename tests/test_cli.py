import pytest
from unittest.mock import Mock, patch
from click.testing import CliRunner

from celery_throttle.cli.main import cli, get_celery_app


class TestCLI:
    """Test cases for CLI commands."""

    @pytest.fixture
    def runner(self):
        """Provide a Click CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def mock_queue_manager(self):
        """Provide a mock queue manager."""
        manager = Mock()
        manager.create_queue.return_value = "test_queue"
        manager.list_queues.return_value = []
        return manager

    def test_cli_help(self, runner):
        """Test CLI help command."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Celery Throttle" in result.output

    def test_cli_with_redis_options(self, runner):
        """Test CLI with Redis connection options."""
        with patch("celery_throttle.cli.main.get_queue_manager"):
            result = runner.invoke(
                cli,
                [
                    "--redis-host",
                    "test-redis",
                    "--redis-port",
                    "6380",
                    "--redis-db",
                    "1",
                    "queue",
                    "list",
                ],
            )
            # Command should execute (may fail if no queues, but shouldn't error on args)
            assert "Error" not in result.output or result.exit_code == 0

    def test_queue_create_command(self, runner, mock_queue_manager):
        """Test queue create command."""
        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "create", "10/60s"])

            assert result.exit_code == 0
            assert "Created queue" in result.output
            mock_queue_manager.create_queue.assert_called_once_with("10/60s")

    def test_queue_create_command_invalid_rate_limit(self, runner, mock_queue_manager):
        """Test queue create command with invalid rate limit."""
        mock_queue_manager.create_queue.side_effect = ValueError("Invalid rate limit")

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "create", "invalid"])

            assert result.exit_code == 1
            assert "Error creating queue" in result.output

    def test_queue_list_command_empty(self, runner, mock_queue_manager):
        """Test queue list command with no queues."""
        mock_queue_manager.list_queues.return_value = []

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "list"])

            assert result.exit_code == 0
            assert "No queues found" in result.output

    def test_queue_list_command_with_queues(self, runner, mock_queue_manager):
        """Test queue list command with queues."""
        from datetime import datetime
        from celery_throttle.queue.manager import QueueConfig
        from celery_throttle.core.rate_limiter import RateLimit

        mock_queues = [
            QueueConfig(
                name="queue1",
                rate_limit=RateLimit.from_string("10/60s"),
                created_at=datetime.now(),
                active=True,
            ),
            QueueConfig(
                name="queue2",
                rate_limit=RateLimit.from_string("20/120s"),
                created_at=datetime.now(),
                active=False,
            ),
        ]
        mock_queue_manager.list_queues.return_value = mock_queues

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "list"])

            assert result.exit_code == 0
            assert "queue1" in result.output
            assert "queue2" in result.output
            assert "ACTIVE" in result.output
            assert "INACTIVE" in result.output

    def test_queue_remove_command(self, runner, mock_queue_manager):
        """Test queue remove command."""
        mock_queue_manager.remove_queue.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "remove", "test_queue"])

            assert result.exit_code == 0
            assert "Removed queue: test_queue" in result.output
            mock_queue_manager.remove_queue.assert_called_once_with("test_queue")

    def test_queue_remove_command_nonexistent(self, runner, mock_queue_manager):
        """Test removing non-existent queue."""
        mock_queue_manager.remove_queue.return_value = False

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "remove", "nonexistent"])

            assert result.exit_code == 1
            assert "not found" in result.output

    def test_queue_show_command(self, runner, mock_queue_manager):
        """Test queue show command."""
        from datetime import datetime
        from celery_throttle.queue.manager import QueueConfig, QueueStats
        from celery_throttle.core.rate_limiter import RateLimit

        mock_config = QueueConfig(
            name="test_queue",
            rate_limit=RateLimit.from_string("10/60s"),
            created_at=datetime.now(),
            active=True,
        )

        mock_stats = QueueStats(
            name="test_queue",
            rate_limit=RateLimit.from_string("10/60s"),
            tasks_waiting=5,
            tasks_processing=2,
            tasks_completed=100,
            tasks_failed=3,
            created_at=datetime.now(),
            active=True,
        )

        mock_rate_status = {
            "available_tokens": 5.0,
            "capacity": 10.0,
            "refill_rate": 0.1667,
            "next_token_in": 0.0,
        }

        mock_queue_manager.get_queue_config.return_value = mock_config
        mock_queue_manager.get_queue_stats.return_value = mock_stats
        mock_queue_manager.get_rate_limit_status.return_value = mock_rate_status

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "show", "test_queue"])

            assert result.exit_code == 0
            assert "test_queue" in result.output
            assert "Waiting: 5" in result.output
            assert "Processing: 2" in result.output
            assert "Completed: 100" in result.output

    def test_queue_show_command_nonexistent(self, runner, mock_queue_manager):
        """Test showing non-existent queue."""
        mock_queue_manager.get_queue_config.return_value = None

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "show", "nonexistent"])

            assert result.exit_code == 1
            assert "not found" in result.output

    def test_queue_update_command(self, runner, mock_queue_manager):
        """Test queue update command."""
        mock_queue_manager.update_rate_limit.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "update", "test_queue", "20/120s"])

            assert result.exit_code == 0
            assert "Updated test_queue rate limit to 20/120s" in result.output
            mock_queue_manager.update_rate_limit.assert_called_once_with(
                "test_queue", "20/120s"
            )

    def test_queue_update_command_nonexistent(self, runner, mock_queue_manager):
        """Test updating non-existent queue."""
        mock_queue_manager.update_rate_limit.return_value = False

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "update", "nonexistent", "20/120s"])

            assert result.exit_code == 1
            assert "not found" in result.output

    def test_queue_activate_command(self, runner, mock_queue_manager):
        """Test queue activate command."""
        mock_queue_manager.activate_queue.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "activate", "test_queue"])

            assert result.exit_code == 0
            assert "Activated queue: test_queue" in result.output

    def test_queue_deactivate_command(self, runner, mock_queue_manager):
        """Test queue deactivate command."""
        mock_queue_manager.deactivate_queue.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "deactivate", "test_queue"])

            assert result.exit_code == 0
            assert "Deactivated queue: test_queue" in result.output

    def test_queue_cleanup_all_command_with_confirmation(
        self, runner, mock_queue_manager
    ):
        """Test cleanup all queues command with user confirmation."""
        from datetime import datetime
        from celery_throttle.queue.manager import QueueConfig
        from celery_throttle.core.rate_limiter import RateLimit

        mock_queues = [
            QueueConfig(
                name="queue1",
                rate_limit=RateLimit.from_string("10/60s"),
                created_at=datetime.now(),
                active=True,
            )
        ]
        mock_queue_manager.list_queues.return_value = mock_queues
        mock_queue_manager.remove_queue.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "cleanup-all"], input="y\n")

            assert result.exit_code == 0
            assert "Removed queue1" in result.output

    def test_queue_cleanup_all_command_cancelled(self, runner, mock_queue_manager):
        """Test cleanup all queues command when user cancels."""
        from datetime import datetime
        from celery_throttle.queue.manager import QueueConfig
        from celery_throttle.core.rate_limiter import RateLimit

        mock_queues = [
            QueueConfig(
                name="queue1",
                rate_limit=RateLimit.from_string("10/60s"),
                created_at=datetime.now(),
                active=True,
            )
        ]
        mock_queue_manager.list_queues.return_value = mock_queues

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "cleanup-all"], input="n\n")

            assert result.exit_code == 0
            assert "cancelled" in result.output

    def test_queue_cleanup_empty_command(self, runner, mock_queue_manager):
        """Test cleanup empty queues command."""
        from datetime import datetime
        from celery_throttle.queue.manager import QueueConfig, QueueStats
        from celery_throttle.core.rate_limiter import RateLimit

        mock_queues = [
            QueueConfig(
                name="empty_queue",
                rate_limit=RateLimit.from_string("10/60s"),
                created_at=datetime.now(),
                active=True,
            )
        ]

        mock_stats = QueueStats(
            name="empty_queue",
            rate_limit=RateLimit.from_string("10/60s"),
            tasks_waiting=0,
            tasks_processing=0,
            tasks_completed=0,
            tasks_failed=0,
            created_at=datetime.now(),
            active=True,
        )

        mock_queue_manager.list_queues.return_value = mock_queues
        mock_queue_manager.get_queue_stats.return_value = mock_stats
        mock_queue_manager.remove_queue.return_value = True

        with patch(
            "celery_throttle.cli.main.get_queue_manager",
            return_value=mock_queue_manager,
        ):
            result = runner.invoke(cli, ["queue", "cleanup-empty"], input="y\n")

            assert result.exit_code == 0
            assert "empty_queue" in result.output

    def test_dispatcher_command(self, runner):
        """Test dispatcher command."""
        mock_throttle = Mock()

        with patch("celery_throttle.cli.main.get_celery_app") as mock_get_app:
            with patch(
                "celery_throttle.cli.main.CeleryThrottle", return_value=mock_throttle
            ):
                mock_app = Mock()
                mock_get_app.return_value = mock_app

                # Mock run_dispatcher to stop immediately
                mock_throttle.run_dispatcher.side_effect = KeyboardInterrupt()

                runner.invoke(
                    cli,
                    [
                        "dispatcher",
                        "--celery-app",
                        "myapp.celery:app",
                        "--interval",
                        "0.5",
                    ],
                )

                # Should initialize and attempt to run
                mock_throttle.run_dispatcher.assert_called_once_with(0.5)

    def test_get_celery_app_success(self):
        """Test successfully importing Celery app."""
        with patch("celery_throttle.cli.main.sys.path", []):
            with patch("importlib.import_module") as mock_import:
                mock_module = Mock()
                mock_app = Mock()
                mock_module.app = mock_app
                mock_import.return_value = mock_module

                app = get_celery_app("mymodule:app")

                assert app == mock_app
                mock_import.assert_called_once_with("mymodule")

    def test_get_celery_app_import_error(self):
        """Test handling import errors when loading Celery app."""
        with patch("celery_throttle.cli.main.sys.path", []):
            with patch("importlib.import_module") as mock_import:
                with patch("celery_throttle.cli.main.click.echo"):
                    mock_import.side_effect = ImportError("Module not found")

                    with pytest.raises(SystemExit):
                        get_celery_app("nonexistent:app")

    def test_cli_with_config_file(self, runner, tmp_path):
        """Test CLI with configuration file."""
        import json

        config_file = tmp_path / "config.json"
        config_data = {
            "app_name": "test-app",
            "target_queue": "test-queue",
            "redis": {"host": "config-redis", "port": 6380},
        }
        config_file.write_text(json.dumps(config_data))

        with patch("celery_throttle.cli.main.get_queue_manager") as mock_get_manager:
            mock_manager = Mock()
            mock_manager.list_queues.return_value = []
            mock_get_manager.return_value = mock_manager

            result = runner.invoke(cli, ["--config", str(config_file), "queue", "list"])

            # Should execute without error
            assert result.exit_code == 0

    def test_queue_prefix_option(self, runner):
        """Test --queue-prefix option."""
        with patch("celery_throttle.cli.main.get_queue_manager") as mock_get_manager:
            mock_manager = Mock()
            mock_manager.list_queues.return_value = []
            mock_get_manager.return_value = mock_manager

            result = runner.invoke(
                cli, ["--queue-prefix", "custom_prefix", "queue", "list"]
            )

            assert result.exit_code == 0
