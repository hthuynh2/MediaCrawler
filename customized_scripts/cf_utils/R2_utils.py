import boto3
import os
from dotenv import load_dotenv
import tempfile

load_dotenv()

# Access the variables
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

# Initialize Cloudflare R2 S3 client
s3_client = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY
)


def get_presigned_upload_url(object_key, content_type=None):
    try:
        if content_type:
            presigned_url = s3_client.generate_presigned_url(
                "put_object",
                Params={"Bucket": R2_BUCKET_NAME, "Key": object_key, "ContentType": content_type},
                ExpiresIn=3600  # 1 hour  expiration
            )
        else:
            presigned_url = s3_client.generate_presigned_url(
                "put_object",
                Params={"Bucket": R2_BUCKET_NAME, "Key": object_key},
                ExpiresIn=3600  # 1 hour expiration
            )
        return {"url": presigned_url, "objectKey": object_key}
    except Exception as e:
        raise "Failed to get presigned upload url " + str(e)


def get_presigned_download_url(object_key, expires_in=3600):
    print(object_key)
    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET_NAME, "Key": object_key},
            ExpiresIn=expires_in  # 1 hour expiration
        )
        return {"url": presigned_url, "object_key": object_key}
    except Exception as e:
        raise "Failed to get presigned download url " + str(e)


def get_presigned_download_url_safe_public_file(object_key):
    print(object_key)
    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET_NAME, "Key": object_key},
            ExpiresIn=24 * 3600  # 1 day expiration
        )
        return {"url": presigned_url}
    except Exception as e:
        raise "Failed to get presigned download url " + str(e)


def download_from_r2(object_key):
    """Downloads a file from Cloudflare R2 to a temporary file."""
    suffix = object_key.split(".")[-1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{suffix}") as temp_file:
        temp_path = temp_file.name
        # temp_path = temp_file.name + "."+ object_key.split(".")[-1]  # Get temp file path

        # Download the file into the temp file
        s3_client.download_file(R2_BUCKET_NAME, object_key, temp_path)
        print(f"File downloaded: {object_key} → {temp_path}")

        return temp_path



def upload_to_r2(object_key, filepath, metadata, max_num_retry=1):
    """
    Uploads data directly to Cloudflare R2 storage.

    Args:
        object_key (str): The key (path/filename) to store the object under in R2
        filepath (str): The path to the file to upload
        metadata (dict): Metadata to attach to the uploaded object

    Returns:
        dict: Response containing the upload result information
    """
    if max_num_retry < 1:
        return {
            "success": False,
            "error": "max_num_retry must be at least 1",
        }

    for retry_count in range(max_num_retry):
        try:
            with open(filepath, "rb") as f:
                response = s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=object_key,
                    Body=f,
                    Metadata=metadata,
                )

            # Check if the upload was successful
            if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                return {
                    "success": True,
                    "object_key": object_key,
                    "etag": response.get('ETag'),
                    "version_id": response.get('VersionId')
                }
            else:
                return {
                    "success": False,
                    "error": "Upload failed with response: " + str(response)
                }

        except Exception as e:
            if retry_count == max_num_retry - 1:
                # Handle any exceptions during upload
                error_message = f"Failed to upload to R2: {str(e)}"
                print(error_message)  # Log the error
                return {
                    "success": False,
                    "error": error_message
                }


def check_if_file_exist_in_R2(objectkey):
    """
    Check if a file exists in the Cloudflare R2 bucket.

    Args:
        filename (str): The key (path) of the file in the bucket

    Returns:
        bool: True if the file exists, False otherwise
    """
    try:
        s3_client.head_object(Bucket=R2_BUCKET_NAME, Key=objectkey)
        return True
    except Exception as e:
        return False


def check_file_exist_and_get_presign_download_url( object_key, expires_in=3600):
    if not check_if_file_exist_in_R2(object_key):
        return None
    res = get_presigned_download_url(object_key, expires_in=expires_in)
    return res


# ========
# Batch presign
def _presign_helper(object_key: str, *, expires_in: int):
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": R2_BUCKET_NAME, "Key": object_key},
        ExpiresIn=expires_in,
    )
    return {"url": url, "object_key": object_key}
