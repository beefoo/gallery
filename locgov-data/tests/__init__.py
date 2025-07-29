# tests/__init__.py
import unittest
import sys


# This only runs when unittest discovers this package
def load_tests(loader, standard_tests, pattern):
    # Discover all tests starting in the current directory
    suite = loader.discover(start_dir=".")

    # Open the file to write test results
    with open("test_results.txt", "w") as f:
        print("=======================================SAVING!")
        runner = unittest.TextTestRunner(stream=f, verbosity=2)
        result = runner.run(suite)

    # Stop the default runner from running
    sys.exit(0)
