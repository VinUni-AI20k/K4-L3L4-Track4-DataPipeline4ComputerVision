# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Đức Đông |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602367 |
| GitHub | nguyenducdong22 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-06570 (MSSV 2A202602367: lấy phần chữ số 2202602367, 2202602367 mod 26 = 17, phần tử thứ 17 của `list-trips`)  |

## Kết quả cuối

> Lưu ý về môi trường: máy tôi không chạy được Docker Desktop theo yêu cầu, nên tôi chạy toàn bộ trên **Google Colab** bằng notebook `colab/Day09_DataPipeline_Colab.ipynb`. Notebook dựng lại đúng các dịch vụ của `docker-compose.yml` (MinIO, Postgres 16, Airflow 2.10.5, Metabase, venv của lab) và chạy **nguyên bản** mã của `pipeline/`, `dbt/`, `dags/`. Vì Colab đã dùng cổng 8080 và 9000, Airflow chạy ở cổng 8081 và MinIO ở 9100/9101. `tests/`, `pipeline/verify.py` và `docker-compose.yml` không bị sửa.

- `dataset_version` của bạn: `64e7c23ddec6`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 731 / 137 / 151 (tổng 1019 khung giữ lại từ 1246 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt giữa nguồn A và B ở bronze (2 dòng đầu mỗi bảng): (1) **tên cột**: A là `ts_utc, speed_mps, heading_deg, lat, lon, acc_x, gas, steer`; B là `timestamp_ms, spd_kmh, hdg, latitude, longitude, ax, throttle_pct, steering`. (2) **đơn vị tốc độ**: A là m/s (1.213), B là km/h (55.11). (3) **kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`). (4) **bàn đạp**: A là tỷ lệ 0 đến 1 (`gas` 0.028), B là phần trăm (`throttle_pct` 2.0). (5) **đồng hồ thiết bị**: B lệch 1500 ms so với video (`telemetry_clock_offset_ms` = 1500 trong `bronze_video_files`), A lệch 0.
Pipeline giữ mỗi nguồn một bảng và để mọi cột ở dạng chữ vì bronze là bản ghi nguyên gốc, chỉ thêm: nếu ép kiểu ngay thì ô trống, giá trị lỗi hoặc đơn vị lạ sẽ bị mất hay làm sập lúc nạp, và không thể xử lý lại khi luật thay đổi. Số dòng khớp CSV (3021 và 3234), nạp lần hai báo `already present` (nhận diện bằng sha256), bảng chỉ có 1 commit.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging B (`stg_telemetry_b`) đổi tên cột về schema của A, đổi km/h sang m/s, đổi epoch ms sang timestamp UTC và **trừ độ lệch 1500 ms** khai báo trong `sources.json`. Bằng chứng: thời điểm bắt đầu của B là `2025-10-08 14:30:00.009 UTC`, khớp giờ bắt đầu video `14:30:00`; tốc độ tối đa sau chuẩn hoá là 71.0 km/h (A) và 100.0 km/h (B), hợp lý.
Bảng quarantine (số dòng):

| drive | duplicate | speed_missing | speed_out_of_range |
|---|---|---|---|
| drive_a | 20 | 30 | 3 |
| drive_b | 20 | 30 | 3 |

Sạch + cách ly = nhận về: 2968 + 53 = 3021 (A), 3181 + 53 = 3234 (B). Dòng lỗi vào `silver_telemetry_rejects` (106 dòng) kèm lý do thay vì bị xoá để có thể kiểm toán, đếm tỷ lệ lỗi theo nguồn, báo lỗi cho đội cảm biến và xử lý lại nếu luật đổi; xoá im lặng thì không ai biết đã mất dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống giữa hai mẫu quá 1000 ms) là `warn` vì đó là sự cố cảm biến có thật nhưng đã biết và chấp nhận được: dữ liệu vẫn dùng được, chỉ cần báo (bước 2 cho PASS=6 WARN=1). `in_range` (0 đến 70 m/s) là `error` vì giá trị vô lý làm hỏng mọi thứ phía sau nên phải dừng pipeline.
Thí nghiệm `break-gate` (`break_gate.json`): nới luật từ 70 lên 1000 m/s thì test `in_range_silver_telemetry_speed_mps__70__0` **thất bại với 6 dòng** (`gate_failed: true`, `PASS=5 WARN=1 ERROR=1`). Đó chính là 6 dòng tốc độ vô lý (3 của mỗi drive) mà quarantine đã loại; khi luật bị nới chúng lọt vào silver và cổng bắt được. Sau khi khôi phục: `PASS=6 WARN=1 ERROR=0`. Cổng chất lượng là lưới an toàn thứ hai: bắt cả lỗi do người sửa luật sai, và ngăn dữ liệu xấu đi tiếp xuống gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` (trip-00003, 60 khung): bảng `silver.frames` có 1246 dòng, dấu vân tay `6c7480dfde7e` trước và **1246 dòng, cùng dấu vân tay** sau hai lần chạy lại: không đổi. Kiểu ghi `append` sai: 120 dòng cho một trip chỉ có 60 khung, tức **nhân đôi (2x)**.
Delta `overwrite` theo predicate `trip_id` là một commit nguyên tử: người đọc thấy hoặc bản cũ hoặc bản mới, không có trạng thái nửa vời. Cách xoá rồi ghi lại là hai bước: nếu sập giữa chừng thì trip đã mất dữ liệu cũ mà chưa có dữ liệu mới. Với overwrite, lần sập thì commit không xảy ra và bản cũ còn nguyên, chạy lại là xong.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

Khung **đen** (độ sáng dưới 12) là lỗi video, không có thông tin, nên bị **bỏ** khỏi gold (A có 6, B có 4). Khung **tối** (đường hầm, bóng râm) vẫn có nội dung, chỉ khó nhìn, nên chỉ **gắn cờ nhưng giữ lại** (`toi_giu_lai`: A 4, B 61). Tôi đồng ý: bỏ khung tối sẽ làm mô hình không học được điều kiện thiếu sáng ngoài đời thật; khung tối giữ lại để người dùng tự lọc nếu cần.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi giữ ngưỡng **2** (pipeline đang dùng): ngưỡng 0 chỉ bắt khung giống hệt bit, bỏ sót các khung gần như trùng do nhiễu nén; ngưỡng 6 loại 363/602 khung của A và 244/644 của B, nhiều khả năng loại cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ xe: A chạy chậm (trung bình 42 km/h ở train, lại có đoạn camera đứng hình 6 giây) nên nhiều khung trùng hơn hẳn B (67 km/h): ở cùng ngưỡng 2, A trùng 149 còn B chỉ 53. Xe chạy nhanh thì cảnh đổi nhiều giữa hai khung nên ít trùng.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: với 1019 khung giữ lại và 26 trip, **chia theo khung hình làm cả 26/26 trip bị chia cắt** (26 trip có khung ở cả train và test); **chia theo trip thì 0 trip bị chia cắt**.
Các khung của cùng một trip chỉ cách nhau 0,5 giây, gần như là cùng một cảnh (cùng đường, cùng ánh sáng, cùng xe phía trước). Nếu một nửa nằm ở train, nửa kia ở test thì mô hình chỉ cần nhớ cảnh là "đoán đúng" test: điểm kiểm thử đẹp giả tạo, không phản ánh khả năng tổng quát hoá lên cảnh mới. Chia theo trip buộc test là những cảnh mô hình chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là băm nội dung (của manifest và danh sách khung) nên cùng nội dung thì cùng version, khác nội dung thì khác version, và ai cũng tái tạo được để đối chiếu; ngày giờ hay số đếm thì chạy lại cùng dữ liệu vẫn ra version mới, không chứng minh được gì. Hai lần `export-dataset` liên tiếp của tôi đều ra `64e7c23ddec6` với cùng `manifest_sha256` `3ac10d1c...`.
MSSV được trộn vào `md5(trip_id || MSSV)` để xếp thứ tự trip, nên mỗi MSSV có cách chia split khác dù cùng dữ liệu. Thử đổi MSSV thành 2A202602368: version đổi thành `55e5ca990413`, split thành 707 / 120 / 192 (so với 731 / 137 / 151 của tôi). Đặt lại MSSV của tôi: version trở lại `64e7c23ddec6`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lineage` trong `manifest.json` (version `64e7c23ddec6`) nói dataset được tạo từ: hai video gốc có sha256 `636ba1a9...e9aa` (drive_a) và `bfa62703...dfc` (drive_b); các phiên bản bảng Delta `bronze.raw_telemetry_a/b` = 0, `bronze.trips_raw` = 0, `bronze.video_files` = 1, `silver.frames` = 27, `silver.telemetry` = 0, `silver.frames_flagged` = 0, `silver.frame_telemetry` = 0; luật lọc: bỏ khung `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; chia theo trip, phân tầng theo drive, thứ tự md5.
Ba tháng sau, để tái tạo đúng dataset cần: sha256 video, phiên bản từng bảng (time travel của Delta), luật lọc và ngưỡng trong `dbt_project.yml` và `config.py`, MSSV dùng làm muối chia split, và commit mã của repo.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của tôi là **trip-06570** (`map_index` 17). Với `LAB_FAIL_ONCE=trip-06570`, chạy `day09_video_pipeline` từ lake trống: **1 task chuyển đỏ** (`silver_frames[17]`, log có dòng `simulated crash while extracting trip-06570; run it again`), **7 task `upstream_failed`** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`), 29 task thành công (3 bronze, `trip_commands`, 25 trip còn lại). Tôi clear **chỉ task đỏ** `silver_frames` (và các task hạ nguồn của nó: 8 task được đặt lại, gồm 1 task đỏ và 7 task upstream_failed), nên 25 trip đã xong **không phải chạy lại**; task đỏ chạy lần 2 (`try_number` 2) và DAG chuyển xanh, 37/37 task thành công.
Dữ liệu **không nhân đôi**: `silver.frames` có 1246 dòng và 1246 `frame_id` khác nhau (`select count(*), count(distinct frame_id)`); `verify` vẫn **27/27** và `dataset_version` vẫn `64e7c23ddec6`. Bằng chứng: `report/screenshots/crash_task_log.png`, `crash_graph_red.png`, `crash_graph_green.png`, `report/logs/05_crash_rerun.json`, `report/logs/05_after_rerun_verify.txt`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba thành phần vỡ trước khi lên 500 xe mỗi ngày: (1) **giải mã video bằng FFmpeg trong một task cho mỗi trip chạy tuần tự** (`max_active_tis_per_dagrun=1`, `PARALLELISM=2`, LocalExecutor trên một máy): hiện 26 trip mất vài phút, 500 xe là hàng chục nghìn trip. Thay bằng chạy phân tán (KubernetesExecutor hoặc Ray/Spark), có GPU decode, nhiều worker song song theo partition (slide 24). (2) **DuckDB một tiến trình + dbt `threads: 1` trên một file `.duckdb`**: một người ghi, không mở rộng được. Thay bằng Spark/Trino/Flink trên Delta hoặc Iceberg, hoặc warehouse có tính toán tách rời (slide 22, 23). (3) **Airflow một container, DAG nạp một lần cho cả lô, MinIO một nút và nạp ảnh về một thư mục `datasets/<version>/`**: lịch, metadata DB và lưu trữ sẽ nghẽn. Thay bằng Airflow nhiều scheduler hoặc Dagster có sensor theo sự kiện, object storage nhiều nút (S3 thật), ghi dataset dạng shard (WebDataset hoặc Parquet) thay vì hàng triệu file ảnh lẻ (slide 12).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm hai câu SQL trên `marts.mart_trip_summary` vào dashboard. Câu 1, **tỷ lệ khung bị loại theo trip** (`drop_pct = 100 * (frames - frames_kept) / frames`, sắp giảm dần): trip-04622 (drive_a) bị loại cao nhất, **56,7%** (34/60 khung), rồi trip-03904 50,0% và trip-02086 46,7%; trip của bài tập sập, trip-06570, loại 28,9% (13/45). Những trip tốc độ thấp ở drive A (ví dụ trip-08888 trung bình 6,5 km/h loại 40%) bị loại nhiều vì xe gần như đứng yên nên dHash trùng; còn 4 trip (trip-01037, trip-04324, trip-07304, trip-09209) loại 0%. Câu 2: khung giữ lại theo ánh sáng và split: toàn bộ là `Day` (test 151, train 731, val 137), tức dataset này không có cảnh đêm.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Bất ngờ nhất là chia theo khung hình làm **cả 26/26 trip** bị rò rỉ, chứ không chỉ vài trip. Chưa rõ hết: vì sao `silver.frames` lên version 27 sau khi chạy lại task sập (mỗi lần overwrite một partition là một commit) mà `dataset_version` vẫn không đổi; tôi hiểu là version chỉ băm nội dung dữ liệu chứ không băm số commit, nhưng chưa tự kiểm chứng. Lỗi pipeline chưa bắt được: dataset hoàn toàn ban ngày (`Day`, `Clouds`), không có cảnh đêm hay mưa nên mô hình huấn luyện sẽ lệch; và khung đứng hình ở A chỉ bắt được 11/12 (check `frozen camera` đạt 11/12, một khung đứng hình lọt qua). Tôi cũng không dùng Docker: tôi dựng lại stack trên Colab, nên cổng và đường dẫn khác bản Docker (8081, 9100).
