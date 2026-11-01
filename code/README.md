# Mã nguồn mô phỏng: Hệ thống Tệp tin Phân tán Hadoop và Google Cloud Storage

Bộ mã này phục vụ mục 3.7 của báo cáo (IT4931, Đại học Bách khoa Hà Nội). Toàn bộ thực nghiệm chạy trên một máy duy nhất bằng bộ mô phỏng HDFS (`minihdfs/`) và MinIO làm object store. Đây **không phải** cụm Hadoop thật hay Google Cloud thật, nên số liệu chỉ dùng để so tỉ lệ và chiều hướng, không dùng để ước lượng thời gian trên cloud thật.

## Cấu trúc

```
code/
├── minihdfs/            Bộ mô phỏng HDFS
│   ├── namenode.py      NameNode: namespace, block map
│   ├── datanode.py      DataNode: giới hạn băng thông đĩa, dead_timeout
│   ├── client.py        Client HDFS: ghi, đọc, nhân bản, EC XOR-2-1
│   ├── balancer.py      Balancer: đưa độ lệch dung lượng về ngưỡng
│   ├── objstore.py      Object store (MinIO): độ trễ L0 + s*d, rename = COPY + DELETE
│   ├── fsadapter.py     Giao diện chung cho HDFS và object store
│   ├── cluster.py       Dựng và dừng cụm mô phỏng
│   └── common.py        Hàm dùng chung
├── exp1_terasort.py     TN1: TeraGen, TeraSort, TeraValidate (HDFS, gs_local, gs_cloud)
├── exp23_smallfiles.py  TN2: thao tác metadata; TN3: tệp nhỏ và WordCount
├── exp4_ec.py           TN4: nhân bản 2, 3 và EC XOR-2-1
├── exp5_balancer.py     TN5: mở rộng 2 -> 3 worker và balancer
├── analyze.py           Tổng hợp trung vị từ results/ thành sum_e*.json
├── killcluster.sh       Dừng mọi tiến trình cụm mô phỏng còn sót
├── results/             Kết quả CSV, JSON (sinh ra khi chạy)
├── logs/                Log từng thí nghiệm (sinh ra khi chạy)
└── data/                Dữ liệu tạm của cụm và MinIO (sinh ra khi chạy, không đẩy lên git)
```

Mọi đường dẫn được tính từ vị trí file, nên có thể chạy từ bất kỳ thư mục nào.

## Yêu cầu

- Linux (đã thử trên Ubuntu 24.04)
- Python 3.13
- MinIO server, chạy trên `http://127.0.0.1:9000`, tài khoản `admin` / `admin12345` (khai báo trong `minihdfs/objstore.py`). Đây là tài khoản thử nghiệm chỉ dùng trên máy cục bộ.
- Khoảng 23 GiB RAM và ổ SSD. Thời gian chạy đầy đủ phụ thuộc vào máy.

## Chuẩn bị

```bash
cd code
python3 -m venv .venv
source .venv/bin/activate
pip install numpy boto3
chmod +x killcluster.sh
```

Khởi động MinIO trong một terminal riêng và để nó chạy trong suốt quá trình thí nghiệm. Ví dụ, với dữ liệu lưu trong `code/data/minio`:

```bash
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=admin12345 \
  minio server data/minio --address 127.0.0.1:9000 --console-address 127.0.0.1:9001
```

Nếu không có MinIO, TN1 và TN2 sẽ báo `ConnectionRefusedError` tới `127.0.0.1:9000`.

## Chạy

Chạy tuần tự, **không chạy song song** các thí nghiệm: chúng dùng chung cổng, `killcluster.sh` và thư mục rác của MinIO, nên chạy song song sẽ làm lỗi lẫn nhau.

```bash
source .venv/bin/activate
python3 exp1_terasort.py 3          # TN1, 3 lần chạy
python3 exp23_smallfiles.py         # TN2 và TN3
python3 exp4_ec.py                  # TN4
python3 exp5_balancer.py            # TN5
python3 analyze.py                  # tổng hợp kết quả
```

Có thể điều chỉnh bằng biến môi trường, ví dụ:

```bash
E1_SMALL=1 python3 exp1_terasort.py 1    # bản thử nhỏ của TN1 (3 map, 200.000 bản ghi mỗi map)
E3_N=1000 E3_RUNS=1 python3 exp23_smallfiles.py
E5_RUNS=1 python3 exp5_balancer.py
```

Chạy nền và ghi log:

```bash
nohup sh -c 'python3 exp1_terasort.py 3 > logs/exp1.log 2>&1; python3 exp23_smallfiles.py > logs/exp23.log 2>&1; python3 exp4_ec.py > logs/exp4.log 2>&1; python3 exp5_balancer.py > logs/exp5.log 2>&1; python3 analyze.py > logs/analyze.log 2>&1' &
```

Kiểm tra tiến trình và lỗi:

```bash
pgrep -fa exp || echo "đã xong"
grep -l Traceback logs/*.log       # không in gì là không có lỗi
```

Nếu một lần chạy bị dừng giữa chừng, chạy `sh killcluster.sh` trước khi chạy lại. Nếu tên cụm bị dùng lại khi còn dữ liệu cũ, xóa `data/hdfs/` trước khi chạy lại (`rm -rf data/hdfs/*`).

## File kết quả

| File | Nội dung |
|---|---|
| `results/e1_terasort.csv`, `e1_requests.json` | TN1: thời gian từng pha, số yêu cầu tới object store |
| `results/e2_metadata.csv` | TN2: đếm, liệt kê, đổi tên, đổi tên ngược |
| `results/e3_smallfiles.csv`, `e3_log.json` | TN3: số đối tượng NameNode, thời gian WordCount |
| `results/e4_ec.csv` | TN4: dung lượng, ghi, đọc, đọc khi mất node, phục hồi |
| `results/e5_balancer.csv`, `e5_balancer.json` | TN5: thời gian quét, balancer, độ lệch dung lượng |
| `results/sum_e*.json` | Trung vị từ `analyze.py` |

Cột `s0`–`s3` trong `e5_balancer.csv` là bốn snapshot, lần lượt là: `s0` 2 worker, `s1` sau khi thêm w3, `s2` sau balancer, `s3` sau khi ghi thêm 512 MiB. Mỗi snapshot có `stdev` (độ lệch chuẩn) và `spread` (chênh lệch max − min) của mức dùng đĩa (%) giữa các DataNode.

## Kết quả chính (trung vị 3 lần)

| Thí nghiệm | Chỉ số | Kết quả |
|---|---|---|
| TN1 | Sắp xếp: HDFS / gs_local / gs_cloud | 8,39 s / 10,34 s / 23,45 s |
| TN2 | Đổi tên 10.000 tệp: HDFS / gs_cloud | 0,0003 s / 44,48 s |
| TN3 | WordCount, số đối tượng NameNode: tệp rời / gộp | 301,8 s, 20.002 đối tượng / 16,1 s, 10 đối tượng |
| TN4 | Hệ số dung lượng: nhân bản 2 / 3 / EC XOR-2-1 | 2,0× / 3,0× / 1,5× |
| TN4 | Phục hồi: nhân bản 2 / EC | 10,54 s / 18,98 s |
| TN5 | Thời gian quét: 2 worker / sau balancer | 19,74 s / 14,70 s |
| TN5 | Chênh lệch dung lượng: sau thêm w3 / sau balancer | 50,0 / 3,12 điểm % |

Bảng đầy đủ và cách diễn giải nằm ở mục 3.7 của báo cáo.

## Ghi chú và giới hạn

- Dữ liệu được thu nhỏ: TN1 dùng khoảng 1 GB (10.737.416 bản ghi), không phải 1 TB.
- Băng thông đĩa, độ trễ và dung lượng là tham số mô phỏng, cấu hình trong từng script. Số tuyệt đối không dùng để ước lượng thời gian trên cloud thật.
- Thí nghiệm đĩa mạng và SSD trong kế hoạch ban đầu không được thực hiện.
- H6 (chi phí tiền) không được đo bằng mô phỏng.
- EC đọc bình thường nhanh hơn nhân bản trong mô phỏng (8,8 s so với 14,2 s). Đây có thể là đặc điểm của mô phỏng khi đọc song song từ nhiều node, không nên coi là kết luận chung.
- TN3 chạy đầy đủ trong `exp23_smallfiles.py`. Thư mục `legacy/` chứa bản `exp3_jobs.py` cũ, không cần chạy để tái lập kết quả.