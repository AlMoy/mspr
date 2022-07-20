import psycopg2


def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            database='d8ea37h21hefua',
            user='xkljbahadhhkwg',
            password='6748cce6ed48cb3cac45e5751fc9505175da798baa4bc126695095205b1fb2bb',
            host='ec2-3-248-121-12.eu-west-1.compute.amazonaws.com',
            port='5432'
        )
        print('PostgresSQL Database connection successful')
    except psycopg2.Error as err:
        print(f'PostgresSQL Database connection failed : {err}')

    return conn


def insert(conn, json, now):
    cur = conn.cursor()
    for line in json:
        keys = ""
        values = ""
        for key, item in line.items():
            if keys == "":
                keys += key
            else:
                keys += ", " + key

            get_type = type(item)
            item = str(item).replace("'", "\"")
            if get_type is str or get_type is dict or get_type is list:
                item = "'{value}'".replace("{value}", item)
            else:
                item = item

            if values == "":
                values += item
            else:
                values += ", " + item
        keys += ", now"
        values += ", '" + str(now) + "'"
        sql = "INSERT INTO data({keys}) VALUES ({values})".replace("{keys}", keys).replace("{values}", values)
        cur.execute(sql)
    conn.commit()
