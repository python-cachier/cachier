# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2024, Jirka Borovec <***@gmail.com>

import os
import subprocess
from contextlib import suppress
from subprocess import DEVNULL

_PATH_HERE = os.path.dirname(__file__)
_PATH_ROOT = os.path.dirname(_PATH_HERE)
_PATH_VERSION = os.path.join(_PATH_HERE, "version.info")
_RELEASING_PROCESS = os.getenv("RELEASING_PROCESS", "0") == "1"

with open(_PATH_VERSION) as fopen:
    __version__ = fopen.read().strip()


def _get_git_sha():
    out = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],  # noqa: S603 S607
        stderr=DEVNULL,
    )
    sha = out.decode("utf-8").strip()
    # SHA short
    return sha[:7]


if not _RELEASING_PROCESS:
    sha_short = ""
    if os.path.isdir(os.path.join(_PATH_ROOT, ".git")):
        with suppress(Exception):
            sha_short = _get_git_sha()
            # print(f"Version enriched with git commit hash: {__version__}.")
        # except Exception:
        #     print("Failed to get the git commit hash,"
        #           f" falling back to base version {__version__}.")
    __version__ += f".dev+{sha_short}" if sha_short else ".dev"


__all__ = ["__version__"]
