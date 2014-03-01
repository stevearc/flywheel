Developing
==========
To get started developing flywheel, run the following command:

.. code-block:: bash

    wget https://raw.github.com/mathcamp/devbox/0.1.0/devbox/unbox.py && \
    python unbox.py git@github.com:mathcamp/flywheel

This will clone the repository and install the package into a virtualenv

Running Tests
-------------
The command to run tests is ``python setup.py nosetests``, or ``tox``. Most of
these tests require `DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 6/7 runtime, so make sure you have that
installed.
