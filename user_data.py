from faker import Faker
from random import choice
from sqlalchemy import create_engine, Column, Integer, String, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import EmailStr
import os

# Define the Role Enum and UserSchema
class Role(Enum):
    NORMAL_USER = "NORMAL_USER"
    COLLECTOR = "COLLECTOR"

Base = declarative_base()

class UserSchema(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True)
    password = Column(String, nullable=False)
    phone_number = Column(String, nullable=False)
    role = Column(Enum(Role), nullable=False)

# Initialize Faker
faker = Faker()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://username:password@localhost/waste_management")

# Create a database engine
engine = create_engine(DATABASE_URL)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Function to generate and insert fake user data
def generate_user_data(num_records: int):
    for _ in range(num_records):
        first_name = faker.first_name()
        last_name = faker.last_name()
        email = faker.email()
        password = faker.password(length=8)
        phone_number = faker.phone_number()
        role = choice([Role.NORMAL_USER, Role.COLLECTOR])

        user = UserSchema(first_name=first_name, last_name=last_name, email=email, password=password, phone_number=phone_number, role=role)
        session.add(user)

    # Commit the transaction
    session.commit()

# Generate 500,000 fake user records
num_records = 500000
generate_user_data(num_records)

# Close the session
session.close()

print(f"Successfully inserted {num_records} user records into PostgreSQL.")
