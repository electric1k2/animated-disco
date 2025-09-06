#!/usr/bin/env python3
"""Database initialization script"""

from sqlalchemy import create_engine
from models import Base
from config import DATABASE_URL

def init_database():
    """Initialize database tables"""
    engine = create_engine(DATABASE_URL, echo=True)
    
    # Create all tables
    Base.metadata.create_all(engine)
    print("Database tables created successfully!")

if __name__ == "__main__":
    init_database()