Development
===========

To run python tests, use

.. code-block:: bash

    docker run --rm -p 6379:6379 redis
    uv run pytest tests

To run JS/TS tests, use

.. code-block:: bash

    docker run --rm -p 4748:4748 pythonf/znsocket
    bun test --test-name-pattern native_
