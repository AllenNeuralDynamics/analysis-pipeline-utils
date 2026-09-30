"""Utility functions for interacting with capsule git repositories."""

import logging
import os
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import quote

from codeocean import CodeOcean
from codeocean.capsule import Capsule

from .settings import PipelineEnvSettings, get_settings


def _initialize_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client using environment variables.

    Returns:
        CodeOcean: Initialized Code Ocean client
    """
    settings = get_settings(PipelineEnvSettings)
    return CodeOcean(
        domain=f"https://{settings.codeocean_domain}",
        token=settings.codeocean_api_token.get_secret_value(),
    )


def _run_git_command(command: List[str], password: Optional[str] = None) -> str:
    """Run a git command safely and return its output.

    Args:
        command: List of command arguments
        password: Optional password to authenticate with git over HTTPS
            (the username, if any, is expected to already be embedded in
            the remote URL within `command` - see `_get_git_remote` -
            since it isn't considered sensitive). Supplied to git via
            `GIT_ASKPASS`, pointing at a short inline Python script (no
            on-disk file) that prints it back for git's password prompt,
            rather than embedding it in `command` itself (which is
            otherwise visible to any user on the host via
            `ps`/`/proc/<pid>/cmdline`, and would also be echoed back in a
            `CalledProcessError`'s message on failure). The password itself
            is passed via the subprocess environment, not the script text
            or argv, to keep it out of that same process listing too.

    Returns:
        str: Command output or default value if command fails
    """
    env = None
    if password:
        askpass_script = "import os; print(os.environ['GIT_ASKPASS_PASSWORD'])"
        env = {
            **os.environ,
            "GIT_ASKPASS": f"{sys.executable} -c {shlex.quote(askpass_script)}",
            "GIT_ASKPASS_PASSWORD": password,
        }
    result = subprocess.run(
        command, capture_output=True, text=True, check=True, env=env
    )
    return result.stdout.strip()


def _get_git_remote(capsule_slug: str) -> Tuple[str, str]:
    """Get the git remote URL and password for the specified capsule.

    Args:
        capsule_slug: Slug of the capsule

    Returns:
        Tuple of (remote URL with the username embedded, password to pass
        to `_run_git_command`)
    """
    settings = get_settings(PipelineEnvSettings)
    username = quote(settings.codeocean_email or "")
    host = f"{username}@{settings.codeocean_domain}" if username else settings.codeocean_domain
    url = f"https://{host}/capsule-{capsule_slug}.git"
    return url, settings.codeocean_api_token.get_secret_value()


def get_capsule_commit_hash(capsule: Capsule, branch=None) -> str:
    """Get the git version for a specific capsule from the remote repository.

    Args:
        capsule: Capsule object
        branch: Branch name, or None (default) for the remote's HEAD

    Returns:
        str: Commit hash of the HEAD of the capsule's git repository
    """
    git_remote_url, password = _get_git_remote(capsule.slug)
    git_commit_hash = _run_git_command(
        ["git", "ls-remote", git_remote_url, branch or "HEAD"], password=password
    )
    if not git_commit_hash:
        raise ValueError(f"Could not retrieve git commit hash for capsule {capsule}")
    return git_commit_hash.split()[0]  # Return the commit hash part


def get_latest_release(capsule: Capsule) -> Optional[dict]:
    """Get the latest release information for a specific capsule.
    Args:
        capsule: Capsule object
    Returns:
        dict: Latest release information, or None if no releases found
    """
    if not capsule.release_capsule:
        return None
    client = _initialize_codeocean_client()
    release_capsule = client.capsules.get_capsule(capsule.release_capsule)
    versions = release_capsule.versions
    latest = versions[-1]
    return latest


def _as_git_date(release_time: Union[int, str]) -> str:
    """Normalize a Code Ocean release time to a date git can parse.

    The Code Ocean API returns release_time as a unix timestamp (int), while
    callers may also pass an already-formatted string.

    Args:
        release_time: Unix timestamp or date string

    Returns:
        str: ISO 8601 timestamp
    """
    if isinstance(release_time, str):
        return release_time
    return datetime.fromtimestamp(release_time).isoformat()


def get_commits_since_release(
    capsule: Capsule, release_time: Union[int, str], branch=None
) -> List[str]:
    """Get commits since the release time from the capsule's git repository.

    Args:
        capsule: Capsule object
        release_time: Unix timestamp or ISO 8601 timestamp to filter commits since
        branch: Branch name, or None for the remote's default branch

    Returns:
        List of commit hashes since the release time
    """
    git_remote_url, password = _get_git_remote(capsule.slug)
    since = _as_git_date(release_time)

    # Create a temporary directory for the bare clone
    with tempfile.TemporaryDirectory() as tmpdir:
        # Bare clone (only commits, no working tree). Note we deliberately do
        # not pass --shallow-since here: git aborts the clone with "error
        # processing shallow info" when the cutoff selects no commits, which is
        # exactly the no-commits-since-release case this function exists to
        # detect. Filtering with git log below handles that case correctly.
        clone_command = ["git", "clone", "--bare", "--single-branch"]
        # 'HEAD' is not a branch name; omitting --branch follows the remote default
        if branch:
            clone_command += ["--branch", branch]
        clone_command += [git_remote_url, tmpdir]
        _run_git_command(clone_command, password=password)

        # Run git log in the bare repository
        result = _run_git_command(
            [
                "git",
                "--git-dir",
                tmpdir,
                "log",
                f"--since={since}",
                "--pretty=format:%H",
            ],
            password=password,
        )

        return result.splitlines()


def get_capsule_version_ignoring_patches(
    capsule: Capsule, branch=None, patch_list: list[str] | None = None
) -> Optional[str]:
    """Get the capsule version from the latest release, ignoring patch commits.
    Args:
        capsule: Capsule object
        branch: Branch name, or None (default) for the remote's default branch
        patch_list: Optional list of patch commit hashes to ignore
    Returns:
        str: Version of the capsule based on the latest release, or None
        if there are non-patch commits since release
    """
    try:
        release = get_latest_release(capsule)
        if release is not None:
            commits = get_commits_since_release(
                capsule, release_time=release["release_time"], branch=branch
            )
            if not set(commits).difference(set(patch_list or [])):
                return f"{release['major_version']}.{release['minor_version']}"
    except Exception:
        # Version lookup is best-effort metadata; callers fall back to the
        # commit hash. Never fail an analysis run over it.
        logging.warning(
            f"Could not resolve release version for capsule {capsule.id}, "
            "falling back to commit hash.",
            exc_info=True,
        )
    return None
