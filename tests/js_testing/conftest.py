JEST_PATH = "npm test -- "
import os

import pytest


@pytest.fixture
def run_npm_test():
    def _run_npm_test(name: str, client_url: str = ""):
        import subprocess

        cmd = f"{JEST_PATH} . -t {name} --verbose"
        env = os.environ.copy()
        env["ZNSOCKET_URL"] = client_url.replace("http://", "ws://")
        run = subprocess.run(cmd, check=False, shell=True, env=env, capture_output=True)
        # assert that the test was actually run f'✓ {name}' in run.stdout
        assert f"{name} (" in run.stderr.decode("utf-8")
        # if there is an error raise it
        if run.returncode != 0:
            print(run.stdout.decode("utf-8"))
            raise AssertionError(run.stderr.decode("utf-8"))

    return _run_npm_test
