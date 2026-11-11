"""Database service for CDR storage"""
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from app.config import settings
from app.models.cdr_model import Base, CDRData

logger = logging.getLogger(__name__)


class DatabaseService:
    """Handles database operations for CDR records"""
    
    def __init__(self):
        """Initialize database connection"""
        self.engine = None
        self.SessionLocal = None
    
    def init_db(self) -> bool:
        """
        Initialize database connection and create tables
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.engine = create_engine(
                settings.database_url,
                echo=settings.debug,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            Base.metadata.create_all(bind=self.engine)
            logger.info(f"Database initialized at {settings.postgres_host}:{settings.postgres_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            return False
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = None
        try:
            if self.SessionLocal is None:
                logger.error("Database not initialized")
                session = None
            else:
                session = self.SessionLocal()
            
            yield session
            
            if session:
                session.commit()
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            if session:
                session.close()
    
    def save_cdr(self, cdr_data: Dict[str, Any]) -> bool:
        """Save CDR record to database"""
        try:
            with self.get_session() as session:
                if session is None:
                    logger.error("Cannot save CDR: database session not initialized")
                    return False
                
                cdr = CDRData(
                    customer_id=cdr_data.get("customer_id"),
                    full_name=cdr_data.get("full_name"),
                    risk_score=cdr_data.get("risk_score", 0),
                    city=cdr_data.get("city"),
                    mobile_number=cdr_data.get("mobile_number"),
                    sim_serial_number=cdr_data.get("sim_serial_number"),
                    sim_status=cdr_data.get("sim_status"),
                    imei=cdr_data.get("imei"),
                    fraud_flag=cdr_data.get("fraud_flag", False),
                    fraud_type=cdr_data.get("fraud_type"),
                    transaction_id=cdr_data.get("transaction_id"),
                    time_stamp=cdr_data.get("time_stamp"),
                    type=cdr_data.get("type"),
                    amount_eur=cdr_data.get("amount_eur", 0),
                    processed_timestamp=cdr_data.get("processed_timestamp"),
                )
                
                session.add(cdr)
                logger.info(f"CDR {cdr_data.get('transaction_id')} saved to database")
                return True
                
        except IntegrityError:
            logger.warning(f"CDR {cdr_data.get('transaction_id')} already exists")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error saving CDR: {str(e)}")
            return False
    
    def get_cdr_by_id(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve CDR record by transaction ID"""
        try:
            with self.get_session() as session:
                if session is None:
                    logger.error("Cannot fetch CDR: database session not initialized")
                    return None
                cdr = session.query(CDRData).filter(CDRData.transaction_id == transaction_id).first()
                if cdr:
                    # Detach from session before returning
                    session.expunge(cdr)
                    return cdr.to_dict()
                return None
        except Exception as e:
            logger.error(f"Error fetching CDR {transaction_id}: {str(e)}")
            return None
    
    def get_cdr_count(self) -> int:
        """Get total count of CDR records"""
        try:
            with self.get_session() as session:
                if session is None:
                    return 0
                return session.query(CDRData).count()
        except Exception as e:
            logger.error(f"Error getting CDR count: {str(e)}")
            return 0
    
    def get_recent_cdrs(self, limit: int = 10) -> list:
        """Get recent CDR records"""
        try:
            with self.get_session() as session:
                if session is None:
                    logger.error("Cannot fetch recent CDRs: database session not initialized")
                    return []
                cdrs = session.query(CDRData).order_by(
                    CDRData.created_at.desc()
                ).limit(limit).all()
                # Convert to dict list before session closes
                result = [cdr.to_dict() for cdr in cdrs]
                # Expunge all objects from session
                for cdr in cdrs:
                    session.expunge(cdr)
                return result
        except Exception as e:
            logger.error(f"Error fetching recent CDRs: {str(e)}")
            return []
    
    def close(self):
        """Close database connection"""
        try:
            if self.engine:
                self.engine.dispose()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}")
