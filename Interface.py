#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import psycopg2
import psycopg2.extras

# Tên các bảng metadata
RANGE_META = "range_meta"
ROBIN_META = "rrobin_meta"


def get_connection(
    user="postgres", password="1234", dbname="ratingsdb", host="localhost"
):
    """
    Mở kết nối tới PostgreSQL và trả về connection object.
    """
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    conn.autocommit = False
    return conn


def loadratings(table_name, filepath, conn):
    """
    1) Xóa (nếu có) rồi tạo lại bảng chính.
    2) Đọc file ratings (format userID::movieID::rating::timestamp)
       rồi dùng COPY để load nhanh vào PostgreSQL.
    """
    cur = conn.cursor()
    try:
        # Kiểm tra file tồn tại
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File không tồn tại: {filepath}")

        # 1. Drop & create
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                userid  INT,
                movieid INT,
                rating  FLOAT
            );
        """
        )

        # 2. Chuẩn bị buffer TSV
        buf = io.StringIO()
        with open(filepath, "r") as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) >= 3:
                    # chỉ lấy 3 trường đầu, bỏ timestamp
                    buf.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        buf.seek(0)

        # 3. COPY từ STDIN
        cur.copy_expert(
            f"COPY {table_name} (userid, movieid, rating) "
            "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t');",
            buf,
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Lỗi load ratings: {e}")
    finally:
        cur.close()


def rangepartition(table_name, num_partitions, conn):
    """
    Phân mảnh theo Range: chia đều [0.0,5.0] thành num_partitions khoảng,
    chèn dữ liệu vào các bảng range_part0, range_part1, ...
    """
    cur = conn.cursor()
    try:
        if num_partitions < 1:
            raise ValueError("Số phân vùng phải >=1")

        # Tạo bảng metadata nếu chưa có
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RANGE_META} (
                table_name     TEXT PRIMARY KEY,
                partition_count INT NOT NULL,
                min_val        FLOAT NOT NULL,
                max_val        FLOAT NOT NULL
            );
        """
        )

        # Xóa cũ và tạo lại các bảng con
        for i in range(num_partitions):
            cur.execute(f"DROP TABLE IF EXISTS range_part{i} CASCADE;")
            cur.execute(
                f"""
                CREATE TABLE range_part{i} (
                    userid  INT,
                    movieid INT,
                    rating  FLOAT
                );
            """
            )

        # Tính độ rộng mỗi khoảng
        min_rating, max_rating = 0.0, 5.0
        delta = (max_rating - min_rating) / num_partitions

        # Chèn dữ liệu vào từng partition
        for i in range(num_partitions):
            low = min_rating + i * delta
            high = (
                (min_rating + (i + 1) * delta) if i < num_partitions - 1 else max_rating
            )

            if i == 0:
                # bao gồm low và high
                cond = f"rating >= {low} AND rating <= {high}"
            else:
                # không bao gồm low, chỉ <= high
                cond = f"rating > {low} AND rating <= {high}"

            cur.execute(
                f"""
                INSERT INTO range_part{i}
                SELECT userid, movieid, rating
                  FROM {table_name}
                 WHERE {cond};
            """
            )

        # Cập nhật metadata
        cur.execute(
            f"""
            INSERT INTO {RANGE_META}(table_name, partition_count, min_val, max_val)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE
              SET partition_count = EXCLUDED.partition_count,
                  min_val         = EXCLUDED.min_val,
                  max_val         = EXCLUDED.max_val;
        """,
            (table_name, num_partitions, min_rating, max_rating),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def roundrobinpartition(table_name, num_partitions, conn):
    """
    Phân mảnh Round Robin: chia tuần tự các bản ghi gốc thành N bảng phân vùng.
    """
    cur = conn.cursor()
    sel_cur = conn.cursor()  # Tạo cursor riêng cho SELECT để tránh lỗi khi fetchmany

    try:
        if num_partitions < 1:
            raise ValueError("Số phân vùng phải >= 1")

        # Tạo bảng metadata nếu chưa có
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ROBIN_META} (
                table_name       TEXT PRIMARY KEY,
                partition_count  INT NOT NULL,
                last_rr_index    BIGINT NOT NULL
            );
        """
        )

        # Xóa & tạo lại các bảng con
        for i in range(num_partitions):
            cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE;")
            cur.execute(
                f"""
                CREATE TABLE rrobin_part{i} (
                    userid  INT,
                    movieid INT,
                    rating  FLOAT
                );
            """
            )

        # Lấy dữ liệu gốc
        sel_cur.execute(
            f"""
            SELECT userid, movieid, rating FROM {table_name};
        """
        )

        buckets = [[] for _ in range(num_partitions)]
        batch_size = 10000
        row_index = 0

        while True:
            rows = sel_cur.fetchmany(batch_size)
            if not rows:
                break

            for userid, movieid, rating in rows:
                part_idx = row_index % num_partitions
                buckets[part_idx].append((userid, movieid, rating))
                row_index += 1

            # Batch insert nếu bucket đủ lớn
            for i, bucket in enumerate(buckets):
                if len(bucket) >= 1000:
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO rrobin_part{i} (userid, movieid, rating) VALUES %s",
                        bucket,
                        page_size=1000,
                    )
                    buckets[i] = []

        sel_cur.close()  # Đóng con trỏ SELECT sau khi hoàn thành

        # Chèn nốt phần còn lại
        for i, bucket in enumerate(buckets):
            if bucket:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO rrobin_part{i} (userid, movieid, rating) VALUES %s",
                    bucket,
                    page_size=1000,
                )

        # Cập nhật last_rr_index = tổng số bản ghi đã phân phối - 1
        last_rr_index = row_index - 1 if row_index > 0 else -1
        # Cập nhật metadata
        cur.execute(
            f"""
            INSERT INTO {ROBIN_META}(table_name, partition_count, last_rr_index)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE
              SET partition_count = EXCLUDED.partition_count,
                  last_rr_index   = EXCLUDED.last_rr_index;
        """,
            (table_name, num_partitions, last_rr_index),
        )
        # Nếu last_rr_index = -1, index insert mới sẽ là 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def rangeinsert(table_name, userid, movieid, rating, conn):
    """
    Chèn bản ghi mới vào bảng chính và bảng range_part tương ứng.
    """
    cur = conn.cursor()
    try:
        # Validate input
        if not (0.0 <= rating <= 5.0):
            raise ValueError(
                f"Rating phải trong khoảng [0.0, 5.0], nhận được: {rating}"
            )

        # 1) Insert vào bảng chính
        cur.execute(
            f"""
            INSERT INTO {table_name}(userid, movieid, rating)
            VALUES (%s, %s, %s);
        """,
            (userid, movieid, rating),
        )

        # 2) Lấy metadata
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

        # 3) Tính partition index - sử dụng logic giống với rangepartition
        delta = (max_rating - min_rating) / N

        # Tìm partition phù hợp bằng cách kiểm tra từng khoảng
        idx = N - 1  # default là partition cuối

        for i in range(N):
            low = min_rating + i * delta
            high = (min_rating + (i + 1) * delta) if i < N - 1 else max_rating

            if i == 0:
                # Partition đầu: bao gồm cả low và high
                if low <= rating <= high:
                    idx = i
                    break
            else:
                # Các partition khác: không bao gồm low, chỉ <= high
                if low < rating <= high:
                    idx = i
                    break

        # 4) Chèn vào range_part{idx}
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
    Chèn bản ghi mới vào bảng chính và bảng phân mảnh Round Robin tiếp theo.
    """
    cur = conn.cursor()
    try:
        if not (0.0 <= rating <= 5.0):
            raise ValueError(
                f"Rating phải trong khoảng [0.0, 5.0], nhận được: {rating}"
            )

        # Insert vào bảng chính
        cur.execute(
            f"""
            INSERT INTO {table_name}(userid, movieid, rating)
            VALUES (%s, %s, %s);
        """,
            (userid, movieid, rating),
        )

        # Lấy metadata
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
            raise RuntimeError(
                "Phải gọi roundrobinpartition trước khi gọi roundrobininsert"
            )
        num_partitions, last_idx = meta

        # Tính chỉ số bảng kế tiếp
        new_last_idx = last_idx + 1
        part_idx = new_last_idx % num_partitions

        # Insert vào bảng phân vùng
        cur.execute(
            f"""
            INSERT INTO rrobin_part{part_idx}(userid, movieid, rating)
            VALUES (%s, %s, %s);
        """,
            (userid, movieid, rating),
        )

        # Cập nhật lại chỉ số cuối cùng
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
    Trả về số bảng có tên bắt đầu bằng `prefix`.
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
        cnt = cur.fetchone()[0]
        return cnt
    finally:
        cur.close()
