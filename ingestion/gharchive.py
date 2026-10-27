"""Land GH Archive hourly files in a Unity Catalog volume (and archive them to S3).

Standard library only, so it runs unchanged on your laptop and in AWS Lambda
(no layers or packaging needed). boto3 is available in the Lambda runtime and
is imported lazily.

CLI:   python gharchive.py --start 2024-01-01T00 --hours 6
Lambda: handler = gharchive.handler  (EventBridge schedule, hourly)
"""
import argparse
import os
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

GH_URL = "https://data.gharchive.org/{key}.json.gz"
LAG_HOURS = 3  # GH Archive publishes files a little after the hour ends


def hour_key(dt: datetime) -> str:
    """GH Archive file key, e.g. 2024-01-01-5 (the hour is NOT zero-padded)."""
    return f"{dt:%Y-%m-%d}-{dt.hour}"


def hour_range(start: datetime, hours: int):
    return [start + timedelta(hours=i) for i in range(hours)]


def download(key: str, dest_dir: str) -> str:
    path = os.path.join(dest_dir, f"{key}.json.gz")
    req = urllib.request.Request(GH_URL.format(key=key), headers={"User-Agent": "gh-lakehouse"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(path, "wb") as out:
        while chunk := resp.read(1024 * 1024):
            out.write(chunk)
    return path


def upload_to_volume(host: str, token: str, volume_dir: str, local_path: str) -> str:
    """PUT a file into a UC volume via the Databricks Files API.

    Returns "uploaded" or "exists". Never overwrites, so re-runs are safe.
    """
    name = os.path.basename(local_path)
    url = f"{host.rstrip('/')}/api/2.0/fs/files{volume_dir.rstrip('/')}/{name}?overwrite=false"
    size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        req = urllib.request.Request(
            url,
            data=f,
            method="PUT",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            },
        )
        try:
            urllib.request.urlopen(req, timeout=600).read()
            return "uploaded"
        except urllib.error.HTTPError as e:
            if e.code in (409, 412):  # already there
                return "exists"
            raise


def archive_to_s3(bucket: str, local_path: str, prefix: str = "raw/gharchive") -> str:
    import boto3  # lazy: only needed when S3 archiving is on

    key = f"{prefix}/{os.path.basename(local_path)}"
    boto3.client("s3").upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def ingest(hours, host, token, volume_dir, s3_bucket=None):
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for dt in hours:
            key = hour_key(dt)
            try:
                path = download(key, tmp)
            except urllib.error.HTTPError as e:
                results.append((key, f"download failed: HTTP {e.code}"))
                continue
            status = upload_to_volume(host, token, volume_dir, path)
            if s3_bucket:
                archive_to_s3(s3_bucket, path)
            results.append((key, status))
            os.remove(path)
    return results


# ------------------------------------------------------------------ Lambda
def _token_from_ssm(param_name: str) -> str:
    import boto3

    return boto3.client("ssm").get_parameter(Name=param_name, WithDecryption=True)["Parameter"]["Value"]


def handler(event, context):
    """Ingest one hour: now - LAG_HOURS, or event["hour"] = '2024-01-01T05'."""
    if event and event.get("hour"):
        target = datetime.strptime(event["hour"], "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    else:
        target = (datetime.now(timezone.utc) - timedelta(hours=LAG_HOURS)).replace(
            minute=0, second=0, microsecond=0
        )
    token = _token_from_ssm(os.environ["DATABRICKS_TOKEN_PARAM"])
    results = ingest(
        [target],
        os.environ["DATABRICKS_HOST"],
        token,
        os.environ["VOLUME_DIR"],
        os.environ.get("S3_BUCKET"),
    )
    print(results)
    return {"results": results}


# --------------------------------------------------------------------- CLI
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="UTC hour, e.g. 2024-01-01T00")
    p.add_argument("--hours", type=int, default=1)
    p.add_argument("--volume-dir", default=os.environ.get("VOLUME_DIR"),
                   help="e.g. /Volumes/workspace/gh_lakehouse/raw/landing")
    p.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    a = p.parse_args()
    start = datetime.strptime(a.start, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    host, token = os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"]
    for key, status in ingest(hour_range(start, a.hours), host, token, a.volume_dir, a.s3_bucket):
        print(key, status)


if __name__ == "__main__":
    main()
