import psycopg2

def getopenconnection(user='postgres', password='12345@A3', dbname='ratingsdb'):
    return psycopg2.connect(dbname=dbname, user=user, host='localhost', password=password)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load dữ liệu từ file ratingsfilepath vào bảng ratingstablename (UserID int, MovieID int, Rating float)
    """
    con = openconnection
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            UserID INT,
            MovieID INT,
            Rating FLOAT
        );
    """)

    with open(ratingsfilepath, 'r') as f:
        batch = []
        for line in f:
            parts = line.strip().split("::")
            if len(parts) >= 3:
                userid = int(parts[0])
                movieid = int(parts[1])
                rating = float(parts[2])
                batch.append((userid, movieid, rating))
            if len(batch) == 10000:
                args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in batch)
                cur.execute(f"INSERT INTO {ratingstablename} (UserID, MovieID, Rating) VALUES " + args_str)
                batch.clear()
        if batch:
            args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in batch)
            cur.execute(f"INSERT INTO {ratingstablename} (UserID, MovieID, Rating) VALUES " + args_str)

    con.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo các bảng phân vùng range_part{i} dựa trên phân vùng range theo Rating chia đều [0-5]
    """
    con = openconnection
    cur = con.cursor()

    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS range_part{i};")

    delta = 5.0 / numberofpartitions

    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = f"range_part{i}"
        cur.execute(f"""
            CREATE TABLE {table_name} (
                UserID INT,
                MovieID INT,
                Rating FLOAT
            );
        """)
        if i == 0:
            cur.execute(f"""
                INSERT INTO {table_name} (UserID, MovieID, Rating)
                SELECT UserID, MovieID, Rating FROM {ratingstablename}
                WHERE Rating >= %s AND Rating <= %s;
            """, (minRange, maxRange))
        else:
            cur.execute(f"""
                INSERT INTO {table_name} (UserID, MovieID, Rating)
                SELECT UserID, MovieID, Rating FROM {ratingstablename}
                WHERE Rating > %s AND Rating <= %s;
            """, (minRange, maxRange))

    con.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo các bảng rrobin_part{i} phân vùng theo Round Robin
    """
    con = openconnection
    cur = con.cursor()

    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i};")

    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE rrobin_part{i} (
                UserID INT,
                MovieID INT,
                Rating FLOAT
            );
        """)

    cur.execute(f"""
        WITH numbered AS (
            SELECT UserID, MovieID, Rating,
            ROW_NUMBER() OVER () - 1 AS rownum_zero_based
            FROM {ratingstablename}
        )
        SELECT * FROM numbered;
    """)
    rows = cur.fetchall()

    partitions = {i: [] for i in range(numberofpartitions)}
    for row in rows:
        idx = row[3] % numberofpartitions
        partitions[idx].append(row[:3])

    for i in range(numberofpartitions):
        batch = partitions[i]
        if batch:
            args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in batch)
            cur.execute(f"INSERT INTO rrobin_part{i} (UserID, MovieID, Rating) VALUES " + args_str)

    con.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert dữ liệu mới vào bảng chính và phân vùng Round Robin
    """
    con = openconnection
    cur = con.cursor()

    cur.execute(f"INSERT INTO {ratingstablename} (UserID, MovieID, Rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'rrobin_part%';")
    numberofpartitions = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]

    index = (total_rows - 1) % numberofpartitions

    table_name = f"rrobin_part{index}"
    cur.execute(f"INSERT INTO {table_name} (UserID, MovieID, Rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    con.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert dữ liệu mới vào bảng chính và phân vùng Range dựa trên giá trị rating
    """
    con = openconnection
    cur = con.cursor()

    cur.execute(f"INSERT INTO {ratingstablename} (UserID, MovieID, Rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'range_part%';")
    numberofpartitions = cur.fetchone()[0]

    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating == 5.0:
        index = numberofpartitions - 1
    elif rating % delta == 0 and index != 0:
        index -= 1
    if index >= numberofpartitions:
        index = numberofpartitions - 1

    table_name = f"range_part{index}"
    cur.execute(f"INSERT INTO {table_name} (UserID, MovieID, Rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    con.commit()
    cur.close()
