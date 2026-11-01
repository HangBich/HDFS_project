# Hệ thống Tệp tin Phân tán Hadoop: mô phỏng HDFS và Google Cloud Storage

Đề tài IT4931 (Lưu trữ và xử lý dữ liệu lớn), Đại học Bách khoa Hà Nội.

Nhóm thực hiện: Nguyễn Thị Bích Hằng (20261098M), Lê Thị Hồng Nhung (20241136M), Vũ Tùng Dương (20261229M). Giảng viên hướng dẫn: TS. Tạ Duy Hoàng.

## Nội dung

- `report/`: báo cáo LaTeX (Overleaf) gồm lý thuyết HDFS, ứng dụng và kết quả thực nghiệm.
- `code/`: bộ mô phỏng HDFS và object store cùng năm thí nghiệm. Xem hướng dẫn chi tiết tại [`code/README.md`](code/README.md).

## Kết quả chính

Các thí nghiệm chạy trên một máy duy nhất bằng bộ mô phỏng, không phải cụm Hadoop hay Google Cloud thật. Số liệu chỉ dùng để so tỉ lệ và chiều hướng.

| Chủ đề | Kết quả |
|---|---|
| Sắp xếp 1 GB: HDFS / object store | 8,39 s / 23,45 s |
| Đổi tên 10.000 tệp: HDFS / object store | 0,0003 s / 44,48 s |
| Số đối tượng NameNode: tệp rời / tệp gộp | 20.002 / 10 |
| Hệ số dung lượng: nhân bản 3 / EC XOR-2-1 | 3,0× / 1,5× |
| Thời gian quét: trước / sau balancer | 19,74 s / 14,70 s |

Chi tiết và cách diễn giải nằm trong mục 3.7 của báo cáo.