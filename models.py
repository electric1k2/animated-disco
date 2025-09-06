from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, Enum, BigInteger, FLOAT
from sqlalchemy.types import DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

Base = declarative_base()

class NumberStatus(enum.Enum):
    AVAILABLE = "available"
    RESERVED = "reserved"
    USED = "used"
    DELETED = "deleted"

class ReservationStatus(enum.Enum):
    WAITING_CODE = "waiting_code"
    COMPLETED = "completed"
    EXPIRED = "expired"
    CANCELED = "canceled"

class TransactionType(enum.Enum):
    ADD = "add"
    DEDUCT = "deduct"
    PURCHASE = "purchase"
    REWARD = "reward"

class ProviderMode(enum.Enum):
    POLL = "poll"
    WEBHOOK = "webhook"

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, nullable=False)
    username = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    balance = Column(DECIMAL(12, 2), default=0)
    joined_at = Column(DateTime, default=func.now())
    is_admin = Column(Boolean, default=False)
    is_banned = Column(Boolean, default=False)
    last_reward_at = Column(DateTime)
    language_code = Column(String, default='ar')  # Default to Arabic
    
    # Relationships
    reservations = relationship("Reservation", back_populates="user")
    transactions = relationship("Transaction", back_populates="user")

class Service(Base):
    __tablename__ = 'services'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    emoji = Column(String, default="📱")
    description = Column(Text)
    default_price = Column(DECIMAL(12, 2), nullable=False)
    active = Column(Boolean, default=True)
    
    # Relationships
    numbers = relationship("Number", back_populates="service")
    reservations = relationship("Reservation", back_populates="service")

class Country(Base):
    __tablename__ = 'countries'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    code = Column(String, unique=True, nullable=False)  # e.g., SA, EG, AE
    flag = Column(String, default="🏳️")
    active = Column(Boolean, default=True)

class ServiceCountry(Base):
    __tablename__ = 'service_countries'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    country_name = Column(String, nullable=False)
    country_code = Column(String, nullable=False)  # e.g., +20
    flag = Column(String, default="🇪🇬")
    active = Column(Boolean, default=True)
    
    # Relationships
    service = relationship("Service")
    country = relationship("Country")

class Number(Base):
    __tablename__ = 'numbers'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    country_code = Column(String, nullable=False)
    phone_number = Column(String, nullable=False)
    status = Column(Enum(NumberStatus), default=NumberStatus.AVAILABLE)
    reserved_by_user_id = Column(Integer, ForeignKey('users.id'))
    reserved_at = Column(DateTime)
    expires_at = Column(DateTime)
    code_received_at = Column(DateTime)
    price_override = Column(DECIMAL(12, 2))
    usage_count = Column(Integer, default=0)  # Count how many times this number was used
    
    # Relationships
    service = relationship("Service", back_populates="numbers")
    reserved_by = relationship("User")
    reservations = relationship("Reservation", back_populates="number")

class Provider(Base):
    __tablename__ = 'providers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    base_url = Column(String, nullable=False)
    api_key = Column(String, nullable=False)
    mode = Column(Enum(ProviderMode), default=ProviderMode.POLL)
    poll_interval_sec = Column(Integer, default=5)
    active = Column(Boolean, default=True)

class ServiceProviderMap(Base):
    __tablename__ = 'service_provider_map'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    provider_id = Column(Integer, ForeignKey('providers.id'), nullable=False)
    regex_pattern = Column(String, default=r'\b\d{5,6}\b')
    
    # Relationships
    service = relationship("Service")
    provider = relationship("Provider")

class Reservation(Base):
    __tablename__ = 'reservations'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    number_id = Column(Integer, ForeignKey('numbers.id'), nullable=False)
    status = Column(Enum(ReservationStatus), default=ReservationStatus.WAITING_CODE)
    created_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime)
    expired_at = Column(DateTime)
    code_value = Column(String)
    
    # Relationships
    user = relationship("User", back_populates="reservations")
    service = relationship("Service", back_populates="reservations")
    number = relationship("Number", back_populates="reservations")

class Transaction(Base):
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    type = Column(Enum(TransactionType), nullable=False)
    amount = Column(DECIMAL(12, 2), nullable=False)
    reason = Column(String)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User", back_populates="transactions")

class Channel(Base):
    __tablename__ = 'channels'
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    username_or_link = Column(String, nullable=False)
    required = Column(Boolean, default=True)
    active = Column(Boolean, default=True)
    reward_amount = Column(DECIMAL(12, 2), default=5.0)

class Group(Base):
    __tablename__ = 'groups'
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    username_or_link = Column(String, nullable=False)
    group_id = Column(String, nullable=False)  # Telegram group ID
    required = Column(Boolean, default=True)
    active = Column(Boolean, default=True)
    reward_amount = Column(DECIMAL(12, 2), default=5.0)

class UserChannelReward(Base):
    __tablename__ = 'user_channel_rewards'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    channel_id = Column(Integer, ForeignKey('channels.id'), nullable=False)
    last_award_at = Column(DateTime)
    times_awarded = Column(Integer, default=0)
    
    # Relationships
    user = relationship("User")
    channel = relationship("Channel")

class UserGroupReward(Base):
    __tablename__ = 'user_group_rewards'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.id'), nullable=False)
    last_award_at = Column(DateTime)
    times_awarded = Column(Integer, default=0)
    
    # Relationships
    user = relationship("User")
    group = relationship("Group")

class SecurityMode(enum.Enum):
    TOKEN_ONLY = "token_only"
    ADMIN_ONLY = "admin_only"
    HMAC = "hmac"

class MessageStatus(enum.Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    REJECTED = "rejected"
    ORPHAN = "orphan"

class ServiceGroup(Base):
    __tablename__ = 'service_groups'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    group_chat_id = Column(String, nullable=False)  # Can be negative for groups
    group_title = Column(String)
    group_username = Column(String)
    secret_token = Column(String)
    regex_pattern = Column(String, nullable=False, default=r'\b\d{4,6}\b')
    security_mode = Column(Enum(SecurityMode), default=SecurityMode.TOKEN_ONLY)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    service = relationship("Service")

class ProviderMessage(Base):
    __tablename__ = 'provider_messages'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'))
    group_chat_id = Column(String, nullable=False)
    sender_id = Column(String, nullable=False)
    message_text = Column(Text)
    raw_payload = Column(Text)  # JSON payload
    received_at = Column(DateTime, default=func.now())
    status = Column(Enum(MessageStatus), default=MessageStatus.PENDING)
    processed_at = Column(DateTime)
    
    # Relationships
    service = relationship("Service")

class BlockedMessage(Base):
    __tablename__ = 'blocked_messages'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'))
    group_chat_id = Column(String, nullable=False)
    sender_id = Column(String, nullable=False)
    message_text = Column(Text)
    reason = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())

class StatsMessage(Base):
    __tablename__ = 'stats_messages'
    
    id = Column(Integer, primary_key=True)
    group_chat_id = Column(String, nullable=False)
    message_id = Column(Integer, nullable=False)
    total_reservations = Column(Integer, default=0)
    successful_reservations = Column(Integer, default=0)
    success_rate = Column(FLOAT, default=0.0)
    last_updated = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

class AdminAuditLink(Base):
    __tablename__ = 'admin_audit_links'
    
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'))
    admin_id = Column(String, nullable=False)
    chat_id = Column(String, nullable=False)
    group_title = Column(String)
    group_username = Column(String)
    raw_message = Column(Text)  # JSON
    result = Column(String)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    service = relationship("Service")

class UserDataChannel(Base):
    __tablename__ = 'user_data_channels'
    
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, nullable=False)  # Telegram channel ID
    channel_username = Column(String)
    channel_title = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())

class ForcedSubscription(Base):
    __tablename__ = 'forced_subscriptions'
    
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, nullable=False)  # Telegram channel ID to subscribe to
    channel_username = Column(String)
    channel_title = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())

class UserSubscriptionStatus(Base):
    __tablename__ = 'user_subscription_status'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    subscription_id = Column(Integer, ForeignKey('forced_subscriptions.id'), nullable=False)
    is_subscribed = Column(Boolean, default=False)
    checked_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User")
    subscription = relationship("ForcedSubscription")
