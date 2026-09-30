"""Tests for capsule git repository helper functions."""

import os
import shlex
import subprocess
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from analysis_pipeline_utils.git import (
    _initialize_codeocean_client,
    _run_git_command,
    get_capsule_version_ignoring_patches,
    get_commits_since_release,
)


# Test _initialize_codeocean_client function
@patch.dict(
    os.environ,
    {"CODEOCEAN_DOMAIN": "test-domain", "CODEOCEAN_API_TOKEN": "test-token"},
)
def test_initialize_codeocean_client_success():
    """Tests initializing code ocean client"""
    with patch("analysis_pipeline_utils.git.CodeOcean") as mock_co:
        _initialize_codeocean_client()
        mock_co.assert_called_once_with(
            domain="https://test-domain", token="test-token"
        )


def test_initialize_codeocean_client_missing_env():
    """Tests initializing code ocean client env"""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            _initialize_codeocean_client()


# Test _run_git_command function
@patch("subprocess.run")
def test_run_git_command_success(mock_run):
    """Tests run git command"""
    mock_run.return_value = Mock(returncode=0, stdout="test output\n")

    result = _run_git_command(["git", "test-command"])
    assert result == "test output"
    mock_run.assert_called_once()


@patch("subprocess.run")
def test_run_git_command_password_sets_askpass(mock_run):
    """The password is supplied via GIT_ASKPASS, never embedded in the
    command itself, and is kept out of the askpass command line too
    (passed via env instead)."""
    mock_run.return_value = Mock(returncode=0, stdout="test output\n")

    result = _run_git_command(["git", "ls-remote", "url"], password="mypass")

    assert result == "test output"
    command, kwargs = mock_run.call_args
    assert "mypass" not in command
    assert "GIT_ASKPASS" in kwargs["env"]
    assert "mypass" not in kwargs["env"]["GIT_ASKPASS"]
    assert kwargs["env"]["GIT_ASKPASS_PASSWORD"] == "mypass"


def test_run_git_command_askpass_prints_password():
    """The generated GIT_ASKPASS command answers git's password prompt
    correctly, exercised the same way git itself would invoke it: as a
    subprocess with the prompt text as its final argument."""
    captured_env = {}

    def fake_run(command, **kwargs):
        captured_env.update(kwargs["env"])
        return Mock(returncode=0, stdout="")

    with patch("subprocess.run", side_effect=fake_run):
        _run_git_command(["git", "ls-remote", "url"], password="mypass")

    askpass_cmd = shlex.split(captured_env["GIT_ASKPASS"])
    env = {**os.environ, "GIT_ASKPASS_PASSWORD": captured_env["GIT_ASKPASS_PASSWORD"]}

    result = subprocess.run(
        askpass_cmd + ["Password for 'https://myuser@example.com': "],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.stdout.strip() == "mypass"



# Test release version lookup (see issue #48)
@patch("analysis_pipeline_utils.git._get_git_remote")
@patch("analysis_pipeline_utils.git._run_git_command")
def test_get_commits_since_release_converts_timestamp(mock_git, mock_url):
    """Unix timestamps from the Code Ocean API are converted for git."""
    mock_url.return_value = ("https://example.com/capsule-1.git", "user:token")
    mock_git.return_value = "abc123\ndef456"

    result = get_commits_since_release(Mock(slug="1"), release_time=1768017516)

    assert result == ["abc123", "def456"]
    log_command = mock_git.call_args_list[-1].args[0]
    since = next(a for a in log_command if a.startswith("--since="))
    assert since == f"--since={datetime.fromtimestamp(1768017516).isoformat()}"


@patch("analysis_pipeline_utils.git._get_git_remote")
@patch("analysis_pipeline_utils.git._run_git_command")
def test_get_commits_since_release_clone_args(mock_git, mock_url):
    """The clone omits --branch for HEAD and never passes --shallow-since."""
    mock_url.return_value = ("https://example.com/capsule-1.git", "user:token")
    mock_git.return_value = ""

    get_commits_since_release(Mock(slug="1"), release_time=1768017516)
    clone_command = mock_git.call_args_list[0].args[0]
    # 'HEAD' is not a branch name and would fail with "Remote branch not found"
    assert "--branch" not in clone_command
    # --shallow-since aborts the clone when no commits match the cutoff
    assert "--shallow-since" not in clone_command

    mock_git.reset_mock()
    get_commits_since_release(Mock(slug="1"), release_time=1768017516, branch="main")
    clone_command = mock_git.call_args_list[0].args[0]
    assert clone_command[clone_command.index("--branch") + 1] == "main"


@patch("analysis_pipeline_utils.git.get_latest_release")
@patch("analysis_pipeline_utils.git.get_commits_since_release")
def test_get_capsule_version_no_commits_since_release(mock_commits, mock_release):
    """No commits since release means the release version is used."""
    mock_release.return_value = {
        "major_version": 2,
        "minor_version": 0,
        "release_time": 1768017516,
    }
    mock_commits.return_value = []

    assert get_capsule_version_ignoring_patches(Mock()) == "2.0"


@patch("analysis_pipeline_utils.git.get_latest_release")
@patch("analysis_pipeline_utils.git.get_commits_since_release")
def test_get_capsule_version_only_patch_commits(mock_commits, mock_release):
    """Commits listed as patches do not invalidate the release version."""
    mock_release.return_value = {
        "major_version": 2,
        "minor_version": 1,
        "release_time": 1768017516,
    }
    mock_commits.return_value = ["aaa", "bbb"]

    assert get_capsule_version_ignoring_patches(Mock(), patch_list=["aaa", "bbb"]) == "2.1"
    assert get_capsule_version_ignoring_patches(Mock(), patch_list=["aaa"]) is None


@patch("analysis_pipeline_utils.git.get_latest_release")
def test_get_capsule_version_degrades_on_error(mock_release):
    """A failed lookup returns None rather than killing the run."""
    mock_release.side_effect = RuntimeError("git exploded")

    assert get_capsule_version_ignoring_patches(Mock()) is None
