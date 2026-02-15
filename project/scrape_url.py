"""Scrape ListenBrainz incremental dumps and stream listens tarballs to S3.

Usage (example):
	python scrape_url.py 2428 --bucket my-bucket \
		--prefix listenbrainz/incremental/ --region us-east-1
		
- 2428 should be the dump ID from the last successfully uploaded dump to s3.
- objects that are already in S3 are skipped. 
"""

import argparse
import os
import re
from urllib.parse import urljoin, urlparse

import boto3
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from dotenv import load_dotenv


FOLDER_RE = re.compile(r"listenbrainz-dump-(\d+)-.+-incremental/")


def upload_listenbrainz_dump_to_s3(
	url: str,
	bucket: str,
	prefix: str = "listenbrainz/",
	s3_key: str | None = None,
	region: str | None = None,
	timeout: int = 60,
):
	"""
	Stream a remote ListenBrainz dump file (HTTP/HTTPS) directly into S3.

	- No full download to local disk
	- Uses multipart upload via boto3.upload_fileobj for large files
	"""

	if s3_key is None:
		path = urlparse(url).path
		filename = os.path.basename(path.rstrip("/"))
		if not filename:
			raise ValueError("Could not infer filename from URL. Provide s3_key explicitly.")
		s3_key = f"{prefix.rstrip('/')}/{filename}" if prefix else filename

	session = boto3.session.Session(region_name=region or os.getenv("AWS_DEFAULT_REGION"))
	s3 = session.client("s3")

	with requests.get(url, stream=True, timeout=timeout) as r:
		r.raise_for_status()

		size = r.headers.get("Content-Length")
		if size:
			print(f"Remote file size: {int(size):,} bytes")

		print(f"Uploading to s3://{bucket}/{s3_key} ...")

		s3.upload_fileobj(
			Fileobj=r.raw,
			Bucket=bucket,
			Key=s3_key,
			ExtraArgs={"ContentType": r.headers.get("Content-Type", "application/octet-stream")},
		)

	print("Done.")
	return f"s3://{bucket}/{s3_key}"


def fetch_html(url: str, timeout: int) -> str:
	resp = requests.get(url, timeout=timeout)
	resp.raise_for_status()
	return resp.text


def list_newer_folders(base_url: str, pivot_id: int, timeout: int):
	html = fetch_html(base_url, timeout)
	soup = BeautifulSoup(html, "html.parser")

	folders: list[tuple[int, str]] = []
	for a in soup.find_all("a"):
		href = a.get("href", "")
		match = FOLDER_RE.match(href)
		if not match:
			continue
		dump_id = int(match.group(1))
		if dump_id > pivot_id:
			folders.append((dump_id, urljoin(base_url, href)))

	folders.sort(key=lambda x: x[0])
	return folders


def find_listens_dump_url(folder_url: str, dump_id: int, timeout: int) -> str:
	html = fetch_html(folder_url, timeout)
	soup = BeautifulSoup(html, "html.parser")
	file_re = re.compile(rf"listenbrainz-listens-dump-{dump_id}-.*-incremental\.tar\.zst")

	for a in soup.find_all("a"):
		href = a.get("href", "")
		if file_re.match(href):
			return urljoin(folder_url, href)

	raise ValueError(f"No listens dump found in {folder_url}")


def s3_object_exists(bucket: str, key: str, region: str | None = None) -> bool:
	session = boto3.session.Session(region_name=region or os.getenv("AWS_DEFAULT_REGION"))
	s3 = session.client("s3")
	try:
		s3.head_object(Bucket=bucket, Key=key)
		return True
	except ClientError as exc:  # noqa: BLE001
		if exc.response["Error"].get("Code") in {"404", "NoSuchKey", "NotFound"}:
			return False
		raise


def process_dumps(
	pivot_id: int,
	bucket: str,
	prefix: str,
	base_url: str,
	region: str | None,
	timeout: int,
	dry_run: bool,
):
	folders = list_newer_folders(base_url, pivot_id, timeout)
	if not folders:
		print("No folders found after pivot.")
		return

	for dump_id, folder_url in folders:
		print(f"Processing dump {dump_id} at {folder_url}")
		listens_url = find_listens_dump_url(folder_url, dump_id, timeout)
		filename = os.path.basename(urlparse(listens_url).path)
		key = f"{prefix.rstrip('/')}/{filename}" if prefix else filename

		if s3_object_exists(bucket, key, region):
			print(f"Skipping existing s3://{bucket}/{key}")
			continue

		if dry_run:
			print(f"[dry-run] Would upload {listens_url} -> s3://{bucket}/{key}")
			continue

		upload_listenbrainz_dump_to_s3(
			url=listens_url,
			bucket=bucket,
			prefix=prefix,
			s3_key=key,
			region=region,
			timeout=timeout,
		)


def main():
	load_dotenv()

	parser = argparse.ArgumentParser(description="Scrape ListenBrainz incremental dumps to S3")
	parser.add_argument("pivot_id", type=int, help="Only process dumps with id greater than this")
	parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
	parser.add_argument("--prefix", default="listenbrainz/incremental/", help="S3 prefix for uploads")
	parser.add_argument(
		"--base-url",
		default="https://ftp.musicbrainz.org/pub/musicbrainz/listenbrainz/incremental/",
		help="Base URL of the ListenBrainz incremental listing",
	)
	parser.add_argument("--region", default=None, help="AWS region (overrides AWS_DEFAULT_REGION)")
	parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds")
	parser.add_argument("--dry-run", action="store_true", help="List actions without uploading")

	args = parser.parse_args()

	process_dumps(
		pivot_id=args.pivot_id,
		bucket=args.bucket,
		prefix=args.prefix,
		base_url=args.base_url,
		region=args.region,
		timeout=args.timeout,
		dry_run=args.dry_run,
	)


if __name__ == "__main__":
	main()
