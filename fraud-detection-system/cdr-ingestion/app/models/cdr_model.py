"""SQLAlchemy ORM model for CDR data"""
from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class CDRData(Base):
    """CDR record database model"""
    
    __tablename__ = "cdr_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(50), nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    risk_score = Column(Float, nullable=False)
    city = Column(String(100), nullable=False)
    mobile_number = Column(String(20), nullable=False)
    sim_serial_number = Column(String(50), nullable=False, unique=True)
    sim_status = Column(String(20), nullable=False)  # active, inactive, suspended
    imei = Column(String(50), nullable=False, unique=True)
    fraud_flag = Column(Boolean, default=False)
    fraud_type = Column(String(50), nullable=True)  # Type of fraud if detected
    transaction_id = Column(String(100), nullable=False, unique=True, index=True)
    time_stamp = Column(DateTime, nullable=False)
    type = Column(String(50), nullable=False)  # Transaction type
    amount_eur = Column(Float, nullable=False)
    processed_timestamp = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<CDRData(customer_id={self.customer_id}, transaction_id={self.transaction_id}, mobile={self.mobile_number})>"
    
    def to_dict(self):
        """Convert model to dictionary"""
        return {
            "id": self.id,
            "customer_id": self.customer_id,
            "full_name": self.full_name,
            "risk_score": self.risk_score,
            "city": self.city,
            "mobile_number": self.mobile_number,
            "sim_serial_number": self.sim_serial_number,
            "sim_status": self.sim_status,
            "imei": self.imei,
            "fraud_flag": self.fraud_flag,
            "fraud_type": self.fraud_type,
            "transaction_id": self.transaction_id,
            "time_stamp": self.time_stamp.isoformat() if self.time_stamp else None,
            "type": self.type,
            "amount_eur": self.amount_eur,
            "processed_timestamp": self.processed_timestamp.isoformat() if self.processed_timestamp else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
