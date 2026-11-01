# Mã nguồn mô phỏng - Phần ứng dụng bài toán (HDFS lai Google Cloud Storage)

Thư mục này chứa toàn bộ mã dùng để chạy các thí nghiệm trong mục 6 của báo cáo.

## Thành phần

| Tệp | Nội dung |
|---|---|
| `minihdfs/namenode.py` | NameNode: namespace trong RAM, ánh xạ tệp → block → DataNode, Rack Awareness, heartbeat, nhân bản lại, xóa bản sao thừa, khôi phục EC |
| `minihdfs/datanode.py` | DataNode: lưu block trên đĩa, pipeline ghi có ACK và CRC32, đọc trực tiếp, giới hạn băng thông đĩa |
| `minihdfs/client.py` | Client: ghi theo pipeline, đọc từ node gần nhất, EC XOR-2-1 (cell 1 MiB), đọc khi thiếu block |
| `minihdfs/balancer.py` | Balancer với ngưỡng 10% |
| `minihdfs/objstore.py` | Kho đối tượng: MinIO (S3) + độ trễ theo mô hình Durner `t = 30 ms + s × 20 ms/MiB`; đổi tên = chép + xóa |
| `minihdfs/fsadapter.py` | Giao diện chung `hdfs://` và `gs://` |
| `minihdfs/cluster.py` | Khởi động / dừng cụm (mỗi node một tiến trình) |
| `exp1_terasort.py` | TN1: TeraGen → TeraSort → TeraValidate |
| `exp23_smallfiles.py`, `exp3_jobs.py` | TN2 (đổi tên, liệt kê) và TN3 (tệp nhỏ, WordCount) |
| `exp4_ec.py` | TN4: nhân bản 2, 3 và EC XOR-2-1 |
| `exp5_balancer.py` | TN5: mở rộng cụm 2 → 3 worker, Balancer |
| `analyze.py` | Tính trung vị và tỉ số từ các tệp CSV |
| `results/` | Số liệu thô (CSV, JSON, log) |

## Cách chạy lại

Yêu cầu: Python 3.11 trở lên, `numpy`, `boto3`, MinIO server (có trên conda-forge: `minio-server`), khuyến nghị dùng jemalloc.

```bash
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=admin12345 minio server ./data/minio --address 127.0.0.1:9000 &
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
python3 exp1_terasort.py 3
python3 exp23_smallfiles.py          # TN2 + dựng dữ liệu TN3
E23_PHASE_A=1 python3 exp23_smallfiles.py && python3 exp3_jobs.py   # TN3
python3 exp4_ec.py
python3 exp5_balancer.py
python3 analyze.py
```

Lưu ý: đây là môi trường mô phỏng trên một máy (2 vCPU, 8 GB RAM), không thay thế số đo trên cụm Dataproc thật. Các đường dẫn tuyệt đối `/home/claude/sim/...` trong mã cần sửa lại theo máy của bạn.
