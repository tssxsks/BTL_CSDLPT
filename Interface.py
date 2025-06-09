#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import psycopg2
import psycopg2.extras

# Tên hai bảng metadata dùng để lưu thông tin phân vùng
RANGE_META = "range_meta"
ROBIN_META = "rrobin_meta"


def get_connection(
    user="postgres", password="1234", dbname="ratingsdb", host="localhost"
):
    """
    Tạo và trả về một connection tới PostgreSQL.

    Tham số:
    - user: tên người dùng PostgreSQL
    - password: mật khẩu
    - dbname: tên database
    - host: máy chủ chứa DB (mặc định localhost)

    Thiết lập autocommit = False để tự kiểm soát transaction.
    """
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    conn.autocommit = False
    return conn


def create_metadata(meta_table, meta_values, conflict_updates, conn):
    """
    Khởi tạo bảng metadata (nếu chưa tồn tại) và ghi thông tin metadata mới.

    - meta_table: tên bảng metadata (RANGE_META hoặc ROBIN_META)
    - meta_values: dict các cột và giá trị để INSERT
    - conflict_updates: dict các cột và giá trị để UPDATE nếu xảy ra conflict
    - conn: connection tới PostgreSQL
    """
    cur = conn.cursor()
    try:
        # 1) Tạo bảng metadata nếu chưa có, tuỳ theo loại RANGE hay ROUND-ROBIN
        if meta_table == RANGE_META:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {RANGE_META} (
                    table_name      TEXT PRIMARY KEY,
                    partition_count INT NOT NULL,
                    min_val         FLOAT NOT NULL,
                    max_val         FLOAT NOT NULL
                );
            """)
        elif meta_table == ROBIN_META:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ROBIN_META} (
                    table_name       TEXT PRIMARY KEY,
                    partition_count  INT NOT NULL,
                    last_rr_index    BIGINT NOT NULL
                );
            """)

        # 2) Chuẩn bị câu lệnh INSERT ... ON CONFLICT để ghi hoặc cập nhật metadata
        columns = list(meta_values.keys())
        values = list(meta_values.values())
        col_str = ', '.join(columns)
        placeholder = ', '.join(['%s'] * len(values))
        # Xây dựng phần SET cho ON CONFLICT
        conflict_set = ', '.join(f"{k} = EXCLUDED.{k}" for k in conflict_updates.keys())

        # 3) Thực thi INSERT với ON CONFLICT
        cur.execute(
            f"""
            INSERT INTO {meta_table} ({col_str})
            VALUES ({placeholder})
            ON CONFLICT (table_name) DO UPDATE SET {conflict_set};
            """,
            values,
        )

    finally:
        cur.close()


def loadratings(table_name, filepath, conn):
    """
    Đọc file rating (định dạng user::movie::rating) và tải vào bảng mới.

    - table_name: tên bảng sẽ lưu dữ liệu rating
    - filepath: đường dẫn tới file nguồn
    - conn: connection tới PostgreSQL
    """
    cur = conn.cursor()
    try:
        # Kiểm tra file tồn tại
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File không tồn tại: {filepath}")

        # Xoá bảng nếu đã tồn tại, rồi tạo lại với 3 cột
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid  INT,
                movieid INT,
                rating  FLOAT
            );
        """)

        # Đọc file, chuyển "::" thành tab để COPY nhanh
        buf = io.StringIO()
        with open(filepath, "r") as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) >= 3:
                    buf.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        buf.seek(0)

        # Dùng COPY để insert hàng loạt
        cur.copy_expert(
            f"COPY {table_name} (userid, movieid, rating) "
            "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t');",
            buf,
        )

        conn.commit()  # Hoàn tất transaction
    except Exception as e:
        conn.rollback()  # Quay lại nếu lỗi
        raise RuntimeError(f"Lỗi load ratings: {e}")
    finally:
        cur.close()


def rangepartition(table_name, num_partitions, conn):
    """
    Phân vùng bảng theo giá trị rating (Range Partitioning).

    - table_name: bảng gốc chứa ratings
    - num_partitions: số phân vùng cần tạo (>=1)
    - conn: connection tới PostgreSQL

    Tạo các bảng con range_part0, range_part1, ..., range_part{N-1}, 
    chia đều khoảng [0.0, 5.0] theo num_partitions, rồi chèn dữ liệu vào.
    """
    cur = conn.cursor()
    try:
        if num_partitions < 1:
            raise ValueError("Số phân vùng phải >=1")

        # 1) Tạo (hoặc xoá rồi tạo lại) bảng con cho mỗi phân vùng
        for i in range(num_partitions):
            cur.execute(f"DROP TABLE IF EXISTS range_part{i} CASCADE;")
            cur.execute(f"""
                CREATE TABLE range_part{i} (
                    userid  INT,
                    movieid INT,
                    rating  FLOAT
                );
            """)

        # 2) Xác định khoảng giá trị và độ rộng mỗi phân vùng
        min_rating, max_rating = 0.0, 5.0
        delta = (max_rating - min_rating) / num_partitions

        # 3) Chèn dữ liệu vào từng bảng con theo điều kiện rating
        for i in range(num_partitions):
            low = min_rating + i * delta
            # Đảm bảo phần trên của partition cuối = max_rating
            high = (min_rating + (i + 1) * delta) if i < num_partitions - 1 else max_rating

            # với partition 0: điều kiện rating >= low
            # các partition khác: rating > low
            cond = (
                f"rating >= {low} AND rating <= {high}"
                if i == 0
                else f"rating > {low} AND rating <= {high}"
            )

            cur.execute(f"""
                INSERT INTO range_part{i}
                SELECT userid, movieid, rating
                  FROM {table_name}
                 WHERE {cond};
            """)

        # 4) Lưu metadata về range partition (số partitions, min, max) vào bảng RANGE_META
        create_metadata(
            meta_table=RANGE_META,
            meta_values={
                "table_name": table_name,
                "partition_count": num_partitions,
                "min_val": min_rating,
                "max_val": max_rating,
            },
            conflict_updates={
                "partition_count": num_partitions,
                "min_val": min_rating,
                "max_val": max_rating,
            },
            conn=conn,
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def roundrobinpartition(table_name, num_partitions, conn):
    """
    Phân vùng bảng theo Round-Robin.

    - table_name: bảng gốc chứa ratings
    - num_partitions: số phân vùng cần tạo (>=1)
    - conn: connection tới PostgreSQL

    Tạo các bảng rrobin_part0..rrobin_part{N-1}, rồi lần lượt gán các dòng
    theo chỉ số luân phiên.
    """
    cur = conn.cursor()
    sel_cur = conn.cursor()
    try:
        if num_partitions < 1:
            raise ValueError("Số phân vùng phải >= 1")

        # 1) Tạo (xoá rồi tạo lại) bảng con cho mỗi partition
        for i in range(num_partitions):
            cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE;")
            cur.execute(f"""
                CREATE TABLE rrobin_part{i} (
                    userid  INT,
                    movieid INT,
                    rating  FLOAT
                );
            """)

        # 2) Lấy toàn bộ dữ liệu từ bảng gốc
        sel_cur.execute(f"SELECT userid, movieid, rating FROM {table_name};")

        # Dùng buckets list để gom hàng chờ insert theo batch
        buckets = [[] for _ in range(num_partitions)]
        batch_size = 10000  # lấy mỗi lần 10k rows
        row_index = 0       # đánh dấu thứ tự row để tính partition

        # 3) Lặp fetch và gom theo bucket
        while True:
            rows = sel_cur.fetchmany(batch_size)
            if not rows:
                break

            for userid, movieid, rating in rows:
                part_idx = row_index % num_partitions
                buckets[part_idx].append((userid, movieid, rating))
                row_index += 1

            # Khi bucket đạt 1000 record, flush xuống DB
            for i, bucket in enumerate(buckets):
                if len(bucket) >= 1000:
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO rrobin_part{i} (userid, movieid, rating) VALUES %s",
                        bucket,
                        page_size=1000,
                    )
                    buckets[i] = []

        sel_cur.close()

        # 4) Flush lại những bucket còn thừa (<1000)
        for i, bucket in enumerate(buckets):
            if bucket:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO rrobin_part{i} (userid, movieid, rating) VALUES %s",
                    bucket,
                    page_size=1000,
                )

        # 5) Tính last_rr_index (chỉ số của bản ghi cuối cùng)
        last_rr_index = row_index - 1 if row_index > 0 else -1

        # 6) Lưu metadata vào bảng ROBIN_META
        create_metadata(
            meta_table=ROBIN_META,
            meta_values={
                "table_name": table_name,
                "partition_count": num_partitions,
                "last_rr_index": last_rr_index,
            },
            conflict_updates={
                "partition_count": num_partitions,
                "last_rr_index": last_rr_index,
            },
            conn=conn,
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def rangeinsert(table_name, userid, movieid, rating, conn):
    """
    Chèn thêm một bản ghi vào partition range và cập nhật bảng gốc.

    - table_name: tên bảng gốc
    - userid, movieid, rating: giá trị cần insert
    - conn: connection tới PostgreSQL

    Lấy thông tin min/max và số partition từ RANGE_META, xác định phân vùng
    phù hợp rồi insert vào cả bảng gốc và bảng con.
    """
    cur = conn.cursor()
    try:
        # 1) Kiểm tra rating hợp lệ
        if not (0.0 <= rating <= 5.0):
            raise ValueError(f"Rating phải trong khoảng [0.0, 5.0], nhận được: {rating}")

        # 2) Insert vào bảng gốc
        cur.execute(
            f"""
            INSERT INTO {table_name}(userid, movieid, rating)
            VALUES (%s, %s, %s);
            """,
            (userid, movieid, rating),
        )

        # 3) Lấy metadata để tính partition
        cur.execute(
            f"""
            SELECT partition_count, min_val, max_val
              FROM {RANGE_META}
             WHERE table_name = %s;
            """,
            (table_name,),
        )
        meta = cur.fetchone()
        if not meta:
            raise RuntimeError("Phải gọi rangepartition trước khi rangeinsert")
        N, min_rating, max_rating = meta

        delta = (max_rating - min_rating) / N
        idx = N - 1  # default gán vào cuối cùng nếu không khớp

        # 4) Xác định partition dựa trên rating
        for i in range(N):
            low = min_rating + i * delta
            high = (min_rating + (i + 1) * delta) if i < N - 1 else max_rating
            if (i == 0 and low <= rating <= high) or (i != 0 and low < rating <= high):
                idx = i
                break

        # 5) Insert vào bảng con tương ứng
        cur.execute(
            f"""
            INSERT INTO range_part{idx}(userid, movieid, rating)
            VALUES (%s, %s, %s);
            """,
            (userid, movieid, rating),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def roundrobininsert(table_name, userid, movieid, rating, conn):
    """
    Chèn thêm một bản ghi vào partition round-robin và cập nhật metadata.

    - table_name: tên bảng gốc
    - userid, movieid, rating: giá trị cần insert
    - conn: connection tới PostgreSQL

    Dựa vào last_rr_index trong ROBIN_META để chọn partition kế tiếp,
    rồi update lại last_rr_index.
    """
    cur = conn.cursor()
    try:
        # 1) Kiểm tra rating hợp lệ
        if not (0.0 <= rating <= 5.0):
            raise ValueError(f"Rating phải trong khoảng [0.0, 5.0], nhận được: {rating}")

        # 2) Insert vào bảng gốc
        cur.execute(
            f"""
            INSERT INTO {table_name}(userid, movieid, rating)
            VALUES (%s, %s, %s);
            """,
            (userid, movieid, rating),
        )

        # 3) Lấy metadata để biết số partition và last index
        cur.execute(
            f"""
            SELECT partition_count, last_rr_index
              FROM {ROBIN_META}
             WHERE table_name = %s;
            """,
            (table_name,),
        )
        meta = cur.fetchone()
        if not meta:
            raise RuntimeError("Phải gọi roundrobinpartition trước khi gọi roundrobininsert")
        num_partitions, last_idx = meta

        # 4) Tính partition kế tiếp
        new_last_idx = last_idx + 1
        part_idx = new_last_idx % num_partitions

        # 5) Insert vào bảng con
        cur.execute(
            f"""
            INSERT INTO rrobin_part{part_idx}(userid, movieid, rating)
            VALUES (%s, %s, %s);
            """,
            (userid, movieid, rating),
        )

        # 6) Cập nhật last_rr_index trong ROBIN_META
        cur.execute(
            f"""
            UPDATE {ROBIN_META}
               SET last_rr_index = %s
             WHERE table_name = %s;
            """,
            (new_last_idx, table_name),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def count_partitions(prefix, conn):
    """
    Đếm số bảng con bắt đầu bằng prefix.

    - prefix: tiền tố tên bảng (ví dụ "range_part", "rrobin_part")
    - conn: connection tới PostgreSQL

    Sử dụng pg_stat_user_tables để đếm các bảng người dùng.
    Trả về số lượng bảng.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM pg_stat_user_tables
             WHERE relname LIKE %s;
            """,
            (prefix + "%",),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()
