import psycopg2

conn = psycopg2.connect(
            database='n50',
            user='postgres',
            host='localhost',
            password='112',
            port=5432
        )
cur = conn.cursor()

def create_tables():
    company_query = '''
        CREATE TABLE IF NOT EXISTS company(
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            address VARCHAR(100)
        );'''

    department_query = '''
        CREATE TABLE IF NOT EXISTS department(
            id SERIAL PRIMARY KEY,
            dep_name VARCHAR(100)
        );'''

    employees_query = '''
        CREATE TABLE IF NOT EXISTS employees(
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(50),
            gender VARCHAR(50),
            companyID INT,
            FOREIGN KEY (companyID) REFERENCES company (id) ON DELETE CASCADE,
            departmentID INT,
            FOREIGN KEY (departmentID) REFERENCES department (id) ON DELETE CASCADE
        );'''

    cur.execute(company_query)
    cur.execute(department_query)
    cur.execute(employees_query)
    conn.commit()      

def insert_departments():
    insert_data = '''
        INSERT INTO department (dep_name) 
        VALUES 
            ('Research and Development'),
            ('Support'),
            ('Product Management'),
            ('Services'),
            ('Business Development'),
            ('Legal'),
            ('Sales'),
            ('Training'),
            ('Engineering'),
            ('Marketing');'''
    cur.execute(insert_data)
    conn.commit()

def insert_companies():
    insert_data = '''
        INSERT INTO company (name, address) 
        VALUES 
            ('Brainverse', 'Đưc Trọng'),
            ('Demizz', 'Kunsan'),
            ('Browsetype', 'Pantijan No 2'),
            ('Livepath', 'Mabilang'),
            ('Leenti', 'Yushu'),
            ('Vidoo', 'Huoxian'),
            ('Shuffletag', 'Zhongxiao'),
            ('Divape', 'Fuzhai'),
            ('Camimbo', 'Balucawi'),
            ('DabZ', 'Gifu-shi');'''
    cur.execute(insert_data)
    conn.commit()

def insert_employees():
    insert_data = '''
        INSERT INTO employees (name, email, gender, companyID, departmentID) 
        VALUES 
            ('Nathanil', 'nskynner0@phpbb.com', 'Male', 10, 7),
            ('Sean', 'sscase1@vinaora.com', 'Male', 2, 2),
            ('Koressa', 'kperri2@mayoclinic.com', 'Female', 4, 8),
            ('Muffin', 'manster3@netvibes.com', 'Male', 3, 6),
            ('Nestor', 'ngiacovelli4@sbwire.com', 'Male', 6, NULL),
            ('Neale', 'nalu5@walmart.com', 'Male', 7, 5),
            ('Sammy', NULL, 'Female', 10, NULL),
            ('Florenza', NULL, 'Female', NULL, 6),
            ('Kort', 'khabben1e@indiegogo.com', 'Male', 4, 4),
            ('Danya', 'dgilleon1f@amazon.co.uk', 'Female', 1, 7);
        '''
    cur.execute(insert_data)
    conn.commit()

def fetch_stats():
    cur.execute('SELECT COUNT(*) FROM employees;')
    employees_count = cur.fetchone()[0]
    print(f'Total Employees: {employees_count}')

    cur.execute('SELECT gender, COUNT(*) FROM employees GROUP BY gender;')
    gender_count = cur.fetchall()
    print('Count by Gender:')
    for row in gender_count:
        print(f'{row[0]}: {row[1]}')

    cur.execute('''
            SELECT company.name, COUNT(employees.id) 
            FROM employees 
            JOIN company ON employees.companyID = company.id 
            GROUP BY company.name;
        ''')
    company_employee_count = cur.fetchall()
    print('Count by Company:')
    for row in company_employee_count:
        print(f'{row[0]}: {row[1]}')

    cur.execute('''
            SELECT department.dep_name, COUNT(employees.id) 
            FROM employees 
            JOIN department ON employees.departmentID = department.id 
            GROUP BY department.dep_name;
        ''')
    department_employee_count = cur.fetchall()
    print('Count by Department:')
    for row in department_employee_count:
        print(f'{row[0]}: {row[1]}')

    cur.execute('SELECT name FROM employees WHERE departmentID IS NULL;')
    no_department_employees = cur.fetchall()
    print('Employees without a department:')
    for row in no_department_employees:
        print(row[0])

    cur.execute('''
            SELECT employees.name 
            FROM employees 
            JOIN department ON employees.departmentID = department.id 
            WHERE employees.gender = 'Male' 
            AND department.dep_name = 'Sales';
        ''')
    sales_male_employees = cur.fetchall()
    print('Male employees in Sales:')
    for row in sales_male_employees:
        print(row[0])   


while True:
    choice = input('create tables => 1\nadd data departments => 2\nadd data companies=> 3\nadd data employees=> 4\nfetch stats => 5\nexit => q\n....: ')
    if choice == '1':
        create_tables()
        print('Created tables.')
    elif choice == '2':
        insert_departments()
        print('Added data to departments.')
    elif choice == '3':
        insert_companies()
        print('Added data to companies.')
    elif choice == '4':
        insert_employees()
        print('Added data to employees.')
    elif choice == '5':
        fetch_stats()
    elif choice == 'q':
        break
