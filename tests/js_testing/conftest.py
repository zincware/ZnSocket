import os
import subprocess

import pytest


@pytest.fixture
def run_npm_test():
    def _run_bun_test(name: str, client_url: str = ""):
        # time.sleep(1)  # Ensure the server is ready
        env = os.environ.copy()
        env["ZNSOCKET_URL"] = client_url.replace("http://", "ws://")

        # Run only the specific test by name
        cmd = f"bun test --test-name-pattern '{name}'"
        run = subprocess.run(cmd, check=False, shell=True, env=env, capture_output=True)

        output = run.stdout.decode("utf-8")
        error = run.stderr.decode("utf-8")

        # Verify the test name appears in the output (Bun uses test names in output)
        if name not in output and name not in error:
            raise AssertionError(
                f"Test '{name}' was not found in output:\n{output}\n{error}"
            )

        # Raise if the test failed
        if run.returncode != 0:
            print(output)
            raise AssertionError(error)
        print(f"Test '{name}' passed successfully.")

    return _run_bun_test
