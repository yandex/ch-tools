"""
Steps for interacting with S3.
"""

from behave import Context, given, then, when
from hamcrest import assert_that, equal_to, greater_than
from modules import minio, s3
from modules.steps import get_step_data


@given("a working S3")
def step_wait_for_s3_alive(context: Context) -> None:
    """
    Ensure that S3 is ready to accept incoming requests.
    """
    minio.initialize(context)


@then("S3 contains {count:d} objects")
def step_s3_contains_files(context: Context, count: int) -> None:
    s3_client = s3.S3Client(context)
    objects = s3_client.list_objects("")
    assert_that(
        len(objects),
        equal_to(count),
        f"Objects count = {len(objects)}, expected {count}, objects {objects}",
    )


@then("S3 contains greater than {count:d} objects")
def step_s3_contains_greater_than_files(context: Context, count: int) -> None:
    s3_client = s3.S3Client(context)
    objects = s3_client.list_objects("")
    assert_that(
        len(objects),
        greater_than(count),
        f"Objects count = {len(objects)}, expected greater than {count}, objects {objects}",
    )


@then("S3 bucket {bucket} contains {count:d} objects")
def step_cloud_storage_bucket_contains_files(
    context: Context, bucket: str, count: int
) -> None:
    s3_client = s3.S3Client(context, bucket)
    objects = s3_client.list_objects("")
    assert_that(
        len(objects),
        equal_to(count),
        f"Objects count = {len(objects)}, expected {count}, objects {objects}",
    )


@when("we put object in S3")
def step_put_file_in_s3(context: Context) -> None:
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    s3_client.upload_data(conf["data"], conf["path"])
    assert s3_client.path_exists(conf["path"])


@when("we put {count:d} objects in S3")
def step_put_file_count_in_s3(context: Context, count: int) -> None:
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    for i in range(count):
        path = f"{conf['path'].format(i)}"
        s3_client.upload_data(conf["data"], path)
        assert s3_client.path_exists(path)


@when("we delete object in S3")
def stop_delete_file_in_S3(context: Context) -> None:
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    s3_client.delete_data(conf["path"])
    assert not s3_client.path_exists(conf["path"])


@then("Path does not exist in S3")
def step_create_file_in_s3(context: Context) -> None:
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    assert not s3_client.path_exists(conf["path"])
