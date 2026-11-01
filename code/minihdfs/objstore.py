import os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
"""Kho đối tượng mô phỏng Google Cloud Storage: MinIO thật (giao thức S3) + chèn độ trễ
theo mô hình của Durner và cộng sự (PVLDB 2023): t(s) = L0 + s * d.

Ngữ nghĩa hệ thống tệp trên kho đối tượng (giống các connector gs:// và s3a://):
  - không có thư mục thật, thư mục là tiền tố của khóa (có đối tượng đánh dấu "dir/")
  - đổi tên = chép (copy) rồi xóa từng đối tượng
  - liệt kê theo trang 1000 khóa / yêu cầu
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

MiB = 1024 * 1024
_SESSION = boto3.session.Session()     # dùng chung để tiến trình con tạo client nhanh (đã nạp sẵn mô tả API)
TRASH = os.path.join(ROOT, "data/minio/.minio.sys/tmp/.trash")


def purge_trash():
    """MinIO chuyển dữ liệu đã xóa vào thùng rác nội bộ và dọn dần; dọn ngay để tránh đầy đĩa."""
    import shutil, os
    if os.path.isdir(TRASH):
        for n in os.listdir(TRASH):
            shutil.rmtree(os.path.join(TRASH, n), ignore_errors=True)


class ObjStore:
    def __init__(self, bucket, L0=0.0, d=0.0, threads=16, part=16 * MiB,
                 endpoint="http://127.0.0.1:9000"):
        self.bucket = bucket
        self.L0, self.d = L0, d
        self.part = part
        self.threads = threads
        self.s3 = _SESSION.client("s3", endpoint_url=endpoint, aws_access_key_id="admin",
                               aws_secret_access_key="admin12345", region_name="us-east-1",
                               config=Config(max_pool_connections=64, retries={"max_attempts": 3}))
        self.lock = threading.Lock()
        self.req = {}

    def ensure_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except Exception:
            self.s3.create_bucket(Bucket=self.bucket)

    # ------------------------------------------------------------ mô hình độ trễ
    def _call(self, kind, fn, nbytes=0):
        t0 = time.monotonic()
        r = fn()
        target = self.L0 + (nbytes / MiB) * self.d
        el = time.monotonic() - t0
        if el < target:
            time.sleep(target - el)
        with self.lock:
            self.req[kind] = self.req.get(kind, 0) + 1
        return r

    @staticmethod
    def k(path):
        return path.lstrip("/")

    # ------------------------------------------------------------ thao tác
    def mkdirs(self, path):
        key = self.k(path).rstrip("/") + "/"
        self._call("PUT", lambda: self.s3.put_object(Bucket=self.bucket, Key=key, Body=b""))
        return True

    def write(self, path, data, **_):
        key = self.k(path)
        mv = memoryview(data)
        if len(mv) <= self.part:
            self._call("PUT", lambda: self.s3.put_object(Bucket=self.bucket, Key=key, Body=bytes(mv)),
                       len(mv))
            return
        up = self._call("POST", lambda: self.s3.create_multipart_upload(Bucket=self.bucket, Key=key))
        uid = up["UploadId"]
        parts = []
        for i, off in enumerate(range(0, len(mv), self.part), start=1):   # tải lên tuần tự từng phần
            chunk = bytes(mv[off:off + self.part])
            r = self._call("PUT", lambda: self.s3.upload_part(Bucket=self.bucket, Key=key, PartNumber=i,
                                                              UploadId=uid, Body=chunk), len(chunk))
            parts.append({"PartNumber": i, "ETag": r["ETag"]})
        self._call("POST", lambda: self.s3.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=uid, MultipartUpload={"Parts": parts}))

    def read(self, path):
        key = self.k(path)

        def get():
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        t0 = time.monotonic()
        data = get()
        target = self.L0 + (len(data) / MiB) * self.d
        el = time.monotonic() - t0
        if el < target:
            time.sleep(target - el)
        with self.lock:
            self.req["GET"] = self.req.get("GET", 0) + 1
        return data

    def read_range(self, path, off, length):
        key = self.k(path)
        return self._call("GET", lambda: self.s3.get_object(
            Bucket=self.bucket, Key=key, Range=f"bytes={off}-{off + length - 1}")["Body"].read(), length)

    def list_keys(self, prefix):
        """Liệt kê đệ quy mọi đối tượng dưới tiền tố, 1000 khóa / yêu cầu."""
        pre = self.k(prefix).rstrip("/") + "/"
        keys, token = [], None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": pre, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            r = self._call("LIST", lambda: self.s3.list_objects_v2(**kw))
            keys.extend((o["Key"], o["Size"]) for o in r.get("Contents", []))
            if not r.get("IsTruncated"):
                return keys
            token = r["NextContinuationToken"]

    def ls_r(self, prefix):
        return [(k, s) for k, s in self.list_keys(prefix) if not k.endswith("/")]

    def count(self, prefix):
        keys = self.ls_r(prefix)
        return {"files": len(keys), "bytes": sum(s for _, s in keys)}

    def exists(self, path):
        key = self.k(path)
        try:
            self._call("HEAD", lambda: self.s3.head_object(Bucket=self.bucket, Key=key))
            return True
        except Exception:
            return bool(self.list_keys(path))

    def rename(self, src, dst):
        """Đổi tên trên kho đối tượng: với mỗi đối tượng, chép sang khóa mới rồi xóa khóa cũ."""
        s, dd = self.k(src).rstrip("/"), self.k(dst).rstrip("/")
        keys = [k for k, _ in self.list_keys(src)]
        if not keys:                       # là một tệp đơn lẻ
            keys = [s]
            mapping = [(s, dd)]
        else:
            mapping = [(k, dd + k[len(s):]) for k in keys]

        def mv(pair):
            a, b = pair
            self._call("COPY", lambda: self.s3.copy_object(Bucket=self.bucket, Key=b,
                                                           CopySource={"Bucket": self.bucket, "Key": a}))
            self._call("DELETE", lambda: self.s3.delete_object(Bucket=self.bucket, Key=a))
        if len(mapping) == 1:
            mv(mapping[0])
        else:
            with ThreadPoolExecutor(self.threads) as ex:
                list(ex.map(mv, mapping))
        return True

    def delete(self, path, recursive=True):
        keys = [k for k, _ in self.list_keys(path)] or [self.k(path)]
        for i in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[i:i + 1000]]
            self._call("DELETE", lambda: self.s3.delete_objects(Bucket=self.bucket,
                                                                Delete={"Objects": batch, "Quiet": True}))
        return True

    def raw_bytes(self, prefix=""):
        return sum(s for _, s in self.list_keys(prefix)) if prefix else None
