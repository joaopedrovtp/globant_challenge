from sqlalchemy import Integer, String, Float, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from database import Base


class Department(Base):
    __tablename__ = 'departments'
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String, unique=True)

class Job(Base):
    __tablename__ = 'jobs'
    id = mapped_column(Integer, primary_key=True)
    title = mapped_column(String, unique=True)

class HiredEmployees(Base):
    __tablename__ = 'hired_employees'
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String)
    datetime  = mapped_column(String)
    department_id = mapped_column(Integer)
    job_id = mapped_column(Integer)
    # department_id = mapped_column(Integer, ForeignKey("departments.id"))
    # job_id = mapped_column(Integer, ForeignKey("jobs.id"))
