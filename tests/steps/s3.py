"""
Steps for interacting with S3.
"""

from behave import given
from modules import minio


@given('a working s3')
def step_wait_for_s3_alive(context):
    """
    Ensure that S3 is ready to accept incoming requests.
    """
    minio.initialize(context)
