from faker import Faker
from textwrap import dedent

fake = Faker()

print(dedent(f'''
        INSERT INTO 
            USERS (birthday, username, email, password, address, role, active) 
        VALUES'''))

for _ in range(1500):
    profile = fake.simple_profile()
    print(f"    ('{profile['birthdate']}', '{profile['username']}', '{profile['mail']}', '{fake.password()}', '{profile['address'].replace(chr(0xa), ' ')}', 1, true),")