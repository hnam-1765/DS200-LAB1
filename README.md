# DS200 Lab1 - Hadoop Movie Analysis

Thư mục được tổ chức

- `src/main/java/lab1/`: Mã nguồn java chính
- `pom.xml`: Cấu hình để build hadoop
- `results/`: Các file kết quả
- `Images/`: Ảnh chụp màn hình
- `python_bonus/`: Phần mở rông python
- `tools/generate_results.py`:Tạo kết quả `results/`.

## Ma nguon Hadoop chinh

- `Exercise1AverageRatingJob.java`: tinh diem trung binh va tong so luot danh gia cho moi phim, co xu ly `cleanup()` de tim phim diem cao nhat trong nhom phim co it nhat 5 ratings.
- `Exercise2GenreAnalysisJob.java`: tach nhieu the loai va tinh diem trung binh theo the loai.
- `Exercise3GenderAnalysisJob.java`: join ratings voi users theo `UserID`, sau do tinh trung binh theo gioi tinh cho tung phim.
- `Exercise4AgeGroupAnalysisJob.java`: nhom tuoi thanh `0-18`, `18-35`, `35-50`, `50+` va tinh trung binh theo nhom tuoi.
- `MovieLookup.java`, `UserLookup.java`: helper doc `movies.txt` va `users.txt` qua Distributed Cache.

## Build va chay Hadoop

Build jar:

```bash
cd /home/namhoai/WorkSpace/Hoctap/DS200-LAB1
mvn clean package
```

Chạy bài 1:

```bash
hadoop jar target/movie-hadoop-lab-1.0-SNAPSHOT.jar \
  lab1.Exercise1AverageRatingJob \
  movies.txt ratings_1.txt ratings_2.txt output_ex1
```

Chạy bài 3:

```bash
hadoop jar target/movie-hadoop-lab-1.0-SNAPSHOT.jar \
  lab1.Exercise3GenderAnalysisJob \
  movies.txt users.txt ratings_1.txt ratings_2.txt output_ex3
```

## File ket qua va anh

- File kết quả `results/`.
- Ảnh chụp màn hình `Images/`.

## Ghi chu

-KHông có bộ nào trên 5 lượt đánh giá nên bài 1 không đủ điều kiện
- Phan `python_bonus/` là phần mở rộng. Phần chính là  `src/main/java/lab1/`.
