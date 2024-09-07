import psycopg2
# saytda verifikatsiyadan utolmadim shunga codni shu yirga yozdim


conn = psycopg2.connect(
    database='n50',
    user='postgres',
    host='localhost',
    password='112',
    port=5432
)       
cur = conn.cursor()
# ------------------------------------------1

enum_type = '''create type Y_N AS ENUM ('Y', 'N')'''
# cur.execute(enum_type)
# conn.commit()

table_products = '''
CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    low_fats Y_N not null,
    recyclable Y_N not null
);'''

# cur.execute(table_products)
# conn.commit()


products_data = '''
INSERT INTO products (product_id, low_fats, recyclable)
VALUES 
    (1, 'Y', 'N'),
    (2, 'N', 'Y'),
    (3, 'Y', 'Y'), 
    (4, 'N', 'N'),
    (5, 'N', 'Y'),
    (6, 'Y', 'Y')
ON CONFLICT (product_id) DO NOTHING;''' 

# cur.execute(products_data)
# conn.commit()

select_products = '''
SELECT product_id FROM products 
WHERE low_fats = 'Y' AND recyclable = 'Y';'''

# cur.execute(select_products)
# result = cur.fetchall()

# for row in result:
#     print(f"Product ID: {row[0]}")

#------------------------------------------------2
customer_table = '''create table customer 
                        (id serial primary key, 
                        name varchar(100) not null,
                        referee_id int);'''

# cur.execute(customer_table)
# conn.commit()

customer_data = '''insert into customer(name, referee_id)
                    values('tom', null),
                    ('alex', 1),
                    ('jone', 2),
                    ('Mark', null),
                    ('Bill', 2);'''

# cur.execute(customer_data)
# conn.commit()

select_query = '''select name from customer where referee_id != 2 or referee_id is null;'''

# cur.execute(select_query)
# result = cur.fetchall()

# for row in result:
#     print(row)

# -------------------------------------------3
table_world = '''create table world(name varchar(100),
                                    continent varchar(100),
                                    area  int,
                                    population int,
                                    gdp bigint);'''

# cur.execute(table_world)
# conn.commit()                                   

world_data = '''insert into world(name, continent, area, population, gdp)
                    values('Afghanistan', 'Asia' , 652230, 25500100, 20343000000),
                            ('Albania', 'Europe' , 28748, 2831741, 12960000000),
                            ('Algeria', 'Africa', 2381741, 37100000, 188681000000),
                            ('Andorra', 'Europe', 468, 78115, 3712000000),
                            ('Angola', 'Africa', 1246700, 20609294, 100990000000 );'''
# cur.execute(world_data)
# conn.commit()

select_query = '''select name, population, area from world where area > 3000000 or population > 25000000;'''

# cur.execute(select_query)
# result = cur.fetchall()
# for row in result:
#     print(row)


# -------------------------------------10
activity_type = '''create type activity as ENUM ('start', 'end')'''
# cur.execute(activity_type)
# conn.commit()

table_activity = '''create table activity_table(
                                            machine_id int,
                                            process_id int,
                                            activity_type activity not null,
                                            timestamp float);'''
# cur.execute(table_activity)
# conn.commit()

activity_data = '''insert into activity_table(machine_id, process_id, activity_type, timestamp)
 values(0    , 0          , 'start'         , 0.712),     
 (0          , 0          , 'end'           , 1.520),     
 (0          , 1          , 'start'         , 3.140),     
 (0          , 1          , 'end'           , 4.120),     
 (1          , 0          , 'start'         , 0.550),     
 (1          , 0          , 'end'           , 1.550),     
 (1          , 1          , 'start'         , 0.430),     
 (1          , 1          , 'end'           , 1.420),     
 (2          , 0          , 'start'         , 4.100),   
 (2          , 0          , 'end'           , 4.512),    
 (2          , 1          , 'start'         , 2.500),     
 (2          , 1          , 'end'           , 5.000 );'''

# cur.execute(activity_data)
# conn.commit()

select_activity = '''select a.machine_id, round(cast(avg(b.timestamp - a.timestamp) as numeric), 3) as processing_time 
from activity_table a 
join activity_table b
on a.machine_id = b.machine_id and a.process_id = b.process_id and a.activity_type = 'start' and b.activity_type = 'end'
group by a.machine_id;'''
# cur.execute(select_activity)
# result = cur.fetchall()

# for row in result:
#     print(row)



cur.close()
conn.close()








