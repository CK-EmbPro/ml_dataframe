from faker import Faker
from random import choice, uniform
from datetime import date
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, Enum, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Define the WasteCategory Enum and WasteSchema
class WasteCategory(Enum):
    BIODEGRADABLE = "BIODEGRADABLE"
    NON_BIODEGRADABLE = "NON_BIODEGRADABLE"

Base = declarative_base()

class WasteSchema(Base):
    __tablename__ = 'waste'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(Enum(WasteCategory), nullable=False)
    weight = Column(Float, nullable=False)
    date_collected = Column(Date, nullable=False)
    description = Column(String, nullable=False)
    is_collected = Column(Boolean, nullable=False)

# Initialize Faker
faker = Faker()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://username:password@localhost/waste_management")

# Create a database engine
engine = create_engine(DATABASE_URL)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Function to generate and insert fake waste data
def generate_waste_data(num_records: int):
    for _ in range(num_records):
        category = choice([WasteCategory.BIODEGRADABLE, WasteCategory.NON_BIODEGRADABLE])
        weight = round(uniform(0.1, 100.0), 2)  # Random weight between 0.1 and 100
        date_collected = faker.date_this_decade()  # Random date within this decade
        description = faker.sentence(nb_words=6)
        is_collected = choice([True, False])  # Random collection status

        waste = WasteSchema(category=category, weight=weight, date_collected=date_collected, description=description, is_collected=is_collected)
        session.add(waste)

    # Commit the transaction
    session.commit()

# Generate 500,000 fake waste records
num_records = 500000
generate_waste_data(num_records)

# Close the session
session.close()

print(f"Successfully inserted {num_records} waste records into PostgreSQL.")
