import asyncio
import logging
import re
import json
import csv
import io
import hmac
import hashlib
import time
import threading
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError

from models import (
    Base, User, Service, Country, ServiceCountry, Number, Provider, ServiceProviderMap,
    Reservation, Transaction, Channel, UserChannelReward, Group, UserGroupReward,
    ProviderMessage, ServiceGroup, BlockedMessage, AdminAuditLink,
    UserDataChannel, ForcedSubscription, UserSubscriptionStatus,
    NumberStatus, ReservationStatus, TransactionType, ProviderMode,
    SecurityMode, MessageStatus
)
from config import (
    BOT_TOKEN, ADMIN_ID, ADMIN_USERNAME, ADMIN_PASSWORD, DATABASE_URL, RESERVATION_TIMEOUT_MIN,
    POLL_INTERVAL_SEC, DEFAULT_REWARD_AMOUNT, PAGE_SIZE, PROVIDER_API_TIMEOUT,
    HMAC_SECRET
)
from translations import translator, t
from commands import set_bot_commands, get_text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = scoped_session(sessionmaker(bind=engine))

# Bot setup
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Global variables for session management
admin_sessions = {}  # {user_id: datetime}
maintenance_mode = False

def load_maintenance_mode():
    """Load maintenance mode from database or config"""
    try:
        db = get_db()
        # Check if a setting exists for maintenance mode in database
        # For now, use environment variable with fallback to False
        import os
        return os.getenv("MAINTENANCE_MODE", "false").lower() == "true"
    except:
        return False

def save_maintenance_mode(enabled: bool):
    """Save maintenance mode state"""
    try:
        # Log the maintenance mode change for audit trail
        logger.info(f"Maintenance mode {'enabled' if enabled else 'disabled'} by admin")
        # Could extend this to save to database for persistence
        return True
    except Exception as e:
        logger.error(f"Error saving maintenance mode: {e}")
        return False

# Auto cleanup configuration
auto_cleanup_enabled = True
cleanup_interval_hours = 6  # Run cleanup every 6 hours
message_retention_days = 3  # Keep messages for 3 days
orphan_message_retention_hours = 24  # Keep orphan messages for 24 hours

# FSM States
class UserStates(StatesGroup):
    waiting_for_service = State()
    waiting_for_country = State()
    waiting_for_number_action = State()
    waiting_for_last_digits = State()

class AdminStates(StatesGroup):
    waiting_for_password = State()
    waiting_for_current_password = State()
    waiting_for_new_password = State()
    waiting_for_service_name = State()
    waiting_for_service_emoji = State()
    waiting_for_service_price = State()
    waiting_for_service_description = State()
    waiting_for_service_regex = State()
    waiting_for_service_group_id = State()
    waiting_for_service_secret_token = State()
    waiting_for_service_security_mode = State()
    waiting_for_country_name = State()
    waiting_for_country_code = State()
    waiting_for_country_flag = State()
    waiting_for_number_manual = State()
    waiting_for_numbers_file = State()
    waiting_for_provider_name = State()
    waiting_for_provider_url = State()
    waiting_for_provider_key = State()
    waiting_for_channel_title = State()
    waiting_for_channel_username = State()
    waiting_for_channel_reward = State()
    waiting_for_numbers_input = State()
    waiting_for_user_id_balance = State()
    waiting_for_balance_amount = State()
    waiting_for_broadcast_message = State()
    # Edit service states
    waiting_for_edit_service_name = State()
    waiting_for_edit_service_emoji = State()
    waiting_for_edit_service_price = State()
    waiting_for_edit_service_description = State()
    # User data channel and forced subscription states
    waiting_for_channel_id = State()
    waiting_for_subscription_channel_id = State()
    # Password change state (for main admin only)
    waiting_for_new_password = State()

# Utility functions
def get_db():
    """Get database session"""
    return SessionLocal()

# Helper function to get user language
def get_user_language(user_id: str) -> str:
    """Get user's preferred language"""
    db = get_db()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if user and user.language_code is not None:
            return str(user.language_code)
        return 'ar'
    finally:
        db.close()

# Helper function to update user language
def update_user_language(user_id: str, lang_code: str) -> bool:
    """Update user's preferred language"""
    db = get_db()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if user:
            user.language_code = lang_code
            db.commit()
            return True
        return False
    except Exception as e:
        logger.error(f"Error updating user language: {e}")
        db.rollback()
        return False
    finally:
        db.close()

async def get_or_create_user(telegram_id: str, username: Optional[str] = None, first_name: Optional[str] = None, last_name: Optional[str] = None) -> tuple[User, bool]:
    """Get existing user or create new one. Returns (user, is_new_user)"""
    db = get_db()
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        is_new_user = False
        if not user:
            user = User(
                telegram_id=telegram_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                is_admin=(int(telegram_id) == ADMIN_ID),
                language_code=None  # No language set for new users
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            is_new_user = True
        return user, is_new_user
    finally:
        db.close()

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id == ADMIN_ID or user_id in admin_sessions

def is_admin_session_valid(user_id: int) -> bool:
    """Check if admin session is still valid"""
    if user_id == ADMIN_ID:
        return True
    if user_id in admin_sessions:
        # Session valid for 1 hour
        return (datetime.now() - admin_sessions[user_id]).seconds < 3600
    return False

def normalize_phone_number(phone: str) -> str:
    """Normalize phone number to international format with enhanced validation"""
    if not phone:
        return ""
    
    # Remove all non-digit characters except +
    phone = re.sub(r'[^\d\+]', '', phone)
    
    # Remove multiple + signs, keep only the first one
    if phone.count('+') > 1:
        phone = '+' + phone.replace('+', '')
    
    # Ensure starts with +
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Basic validation: must have at least country code + number (minimum 8 digits total)
    # Remove + for digit counting
    digits_only = phone[1:] if phone.startswith('+') else phone
    if not digits_only.isdigit() or len(digits_only) < 7:
        return ""
    
    # Check for common invalid patterns
    if digits_only.startswith('00'):  # Remove international dialing prefix
        digits_only = digits_only[2:]
        phone = '+' + digits_only
    
    return phone

def extract_last_digits(phone_number: str, digits: int = 3) -> str:
    """Extract last N digits from phone number"""
    # Remove all non-digit characters
    clean_number = re.sub(r'\D', '', phone_number)
    return clean_number[-digits:] if len(clean_number) >= digits else clean_number

def extract_last_three_digits_from_masked_number(message_text: str) -> Optional[str]:
    """Extract last 2-3 digits from masked phone numbers in group messages
    
    Enhanced to handle formats like:
    - to: +2011236**872
    - to: 20 11236•••72  
    - to: +201122•••407
    - •••\***872
    - •••72
    - **407
    """
    try:
        # Enhanced patterns to handle various masked formats
        patterns = [
            # High priority: "to:" prefix patterns
            r'to:\s*\+?[\d\s]*[\*•]{1,}(\d{2,3})(?:\s|$|[^\d])',  # to: +201122•••407 or to: +201122••72
            r'to:\s*(\d{2,3})(?:\s|$)',  # to: 872 or to: 72 (standalone digits)
            
            # Medium priority: masked patterns without "to:"
            r'[•]{3,}\\?\*{0,}(\d{2,3})(?:\s|$|[^\d])',  # •••\***872 or •••\**72
            r'[\*•]{2,}(\d{2,3})(?:\s|$|[^\d])',  # ••872 or **72
            
            # Low priority: any 2-3 digits at word boundaries
            r'\b(\d{2,3})(?:\s|$|[^\d])'  # Any 2-3 digits
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, message_text, re.IGNORECASE)
            if matches:
                # Return the last match (most likely to be phone number ending)
                last_digits = matches[-1]
                if len(last_digits) >= 2 and last_digits.isdigit():
                    logger.info(f"Extracted last digits '{last_digits}' using pattern from masked message: {message_text}")
                    return last_digits
        
        # Additional fallback: look for any sequence of 2-3 digits after symbols
        fallback_matches = re.findall(r'[•\*\\]+([0-9]{2,3})', message_text)
        if fallback_matches:
            digits = fallback_matches[-1]  # Take the last match
            logger.info(f"Fallback extraction of digits '{digits}' from: {message_text}")
            return digits
        
        # If no pattern matched, try to find any 2-3 consecutive digits
        all_digit_groups = re.findall(r'\d{2,3}', message_text)
        if all_digit_groups:
            last_digits = all_digit_groups[-1]  # Take the last group
            logger.info(f"Extracted last digits '{last_digits}' from last digit group in: {message_text}")
            return last_digits
        
        logger.warning(f"Could not extract last digits from: {message_text}")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting last digits from masked number: {e}")
        return None

async def find_reservation_by_last_digits(last_digits: str, service_id: int) -> Optional[Reservation]:
    """Find active reservation by matching last 2-3 digits of phone number"""
    db = get_db()
    try:
        # Get all active reservations for this service
        active_reservations = db.query(Reservation).join(Number).filter(
            Reservation.service_id == service_id,
            Reservation.status == ReservationStatus.WAITING_CODE,
            Reservation.expired_at > datetime.now()
        ).all()
        
        digits_count = len(last_digits)
        
        # Check each reservation's phone number last digits
        for reservation in active_reservations:
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if number:
                # Extract same number of digits as the search term
                number_last_digits = extract_last_digits(str(number.phone_number), digits_count)
                if number_last_digits == last_digits:
                    logger.info(f"Found reservation by last {digits_count} digits '{last_digits}': {str(number.phone_number)}")
                    return reservation
        
        return None
    except Exception as e:
        logger.error(f"Error finding reservation by last digits: {e}")
        return None
    finally:
        db.close()

async def search_in_orphan_messages(last_digits: str, service_id: int) -> Optional[tuple]:
    """Search for codes in orphan messages using last digits"""
    db = get_db()
    try:
        # Search in blocked/orphan messages for messages containing these last digits
        orphan_messages = db.query(ProviderMessage).filter(
            ProviderMessage.service_id == service_id,
            ProviderMessage.status == MessageStatus.ORPHAN,
            ProviderMessage.received_at >= datetime.now() - timedelta(hours=2)  # Last 2 hours
        ).order_by(ProviderMessage.received_at.desc()).limit(50).all()
        
        # Get regex pattern for this service
        service_provider_map = db.query(ServiceProviderMap).filter(
            ServiceProviderMap.service_id == service_id
        ).first()
        regex_pattern = str(service_provider_map.regex_pattern) if service_provider_map else r'\b\d{5,6}\b'
        
        for msg in orphan_messages:
            # Check if message contains our last digits
            if last_digits in str(msg.message_text):
                # Try to extract full number and code
                numbers_in_message = re.findall(r'\b\d{10,15}\b', str(msg.message_text))
                for full_number in numbers_in_message:
                    if extract_last_digits(full_number) == last_digits:
                        # Extract code from this message
                        codes = re.findall(regex_pattern, str(msg.message_text))
                        if codes:
                            logger.info(f"Found orphan message with number ending {last_digits}: {full_number}, code: {codes[0]}")
                            return (full_number, codes[0], msg.message_text)
        
        return None
    except Exception as e:
        logger.error(f"Error searching orphan messages: {e}")
        return None
    finally:
        db.close()

async def search_code_in_groups(phone_number: str, service_id: int) -> Optional[str]:
    """Search for code in recent group messages for the given phone number"""
    db = get_db()
    try:
        # Find service groups for this service
        service_groups = db.query(ServiceGroup).filter(
            ServiceGroup.service_id == service_id,
            ServiceGroup.active == True
        ).all()
        
        if not service_groups:
            logger.warning(f"No active groups found for service_id {service_id}")
            return None
        
        # Get regex pattern for this service
        service_provider_map = db.query(ServiceProviderMap).filter(
            ServiceProviderMap.service_id == service_id
        ).first()
        regex_pattern = str(service_provider_map.regex_pattern) if service_provider_map else r'\b\d{5,6}\b'
        
        # Search in recent messages for this phone number
        for group in service_groups:
            logger.info(f"Searching for code in group {group.group_chat_id} for number {phone_number}")
            
            # Look for recent messages containing this phone number
            recent_messages = db.query(ProviderMessage).filter(
                ProviderMessage.service_id == service_id,
                ProviderMessage.message_text.contains(phone_number),
                ProviderMessage.received_at >= datetime.now() - timedelta(hours=1)  # Last hour only
            ).order_by(ProviderMessage.received_at.desc()).limit(10).all()
            
            for msg in recent_messages:
                # Try to extract code from message
                number, code = extract_number_and_code(str(msg.message_text), regex_pattern)
                if number == phone_number and code:
                    logger.info(f"Found code {code} for number {phone_number} in message: {msg.message_text}")
                    return code
        
        logger.info(f"No code found for number {phone_number} in any group messages")
        return None
        
    except Exception as e:
        logger.error(f"Error searching for code in groups: {e}")
        return None
    finally:
        db.close()

async def auto_search_for_code(reservation_id: int):
    """Auto search for code - starts after 15 seconds then every 5 seconds"""
    # Wait 15 seconds before first search
    await asyncio.sleep(5)
    
    max_attempts = 50  # Max 5 minutes (50 * 5 seconds + 15 initial)
    attempts = 0
    
    while attempts < max_attempts:
        db = get_db()
        try:
            # Check if reservation is still valid
            reservation = db.query(Reservation).filter(
                Reservation.id == reservation_id,
                Reservation.status == ReservationStatus.WAITING_CODE
            ).first()
            
            if not reservation:
                logger.info(f"Reservation {reservation_id} no longer valid, stopping auto search")
                return
            
            # Get number for this reservation
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if not number:
                logger.warning(f"Number not found for reservation {reservation_id}")
                return
            
            logger.info(f"Auto searching for code attempt {attempts + 1} for number {str(number.phone_number)}")
            
            # Search for code
            code = await search_code_in_groups(str(number.phone_number), number.service_id)
            
            if code:
                logger.info(f"Auto search found code {code} for reservation {reservation_id}")
                
                # Complete the reservation
                success = await complete_reservation_atomic(reservation_id, code)
                
                if success:
                    # Send code to user
                    service = db.query(Service).filter(Service.id == number.service_id).first()
                    
                    await bot.send_message(
                        reservation.user_id,
                        f"✅ تم استلام كود التحقق!\n\n"
                        f"📱 الرقم: `{str(number.phone_number)}`\n"
                        f"🏷 الخدمة: {str(service.emoji) if service else ''} {str(service.name) if service else ''}\n"
                        f"🔢 الكود: ```{code}```\n"
                        f"💰 تم الخصم: {str(service.default_price) if service else '0'} وحدة\n\n"
                        f"✅ تمت العملية بنجاح",
                        parse_mode="Markdown"
                    )
                    return
                
        except Exception as e:
            logger.error(f"Error in auto search for reservation {reservation_id}: {e}")
        finally:
            db.close()
        
        attempts += 1
        await asyncio.sleep(2)  # Wait 2 seconds between attempts
    
    logger.info(f"Auto search completed for reservation {reservation_id} after {max_attempts} attempts")

def detect_country_code(phone: str) -> str:
    """Detect country code from phone number"""
    phone = normalize_phone_number(phone)
    
    # Common country codes mapping
    country_codes = {
        '+1': '+1',      # USA/Canada
        '+7': '+7',      # Russia/Kazakhstan  
        '+20': '+20',    # Egypt
        '+33': '+33',    # France
        '+34': '+34',    # Spain
        '+39': '+39',    # Italy
        '+44': '+44',    # UK
        '+49': '+49',    # Germany
        '+52': '+52',    # Mexico
        '+55': '+55',    # Brazil
        '+60': '+60',    # Malaysia
        '+61': '+61',    # Australia
        '+62': '+62',    # Indonesia
        '+63': '+63',    # Philippines
        '+64': '+64',    # New Zealand
        '+65': '+65',    # Singapore
        '+66': '+66',    # Thailand
        '+81': '+81',    # Japan
        '+82': '+82',    # South Korea
        '+84': '+84',    # Vietnam
        '+86': '+86',    # China
        '+90': '+90',    # Turkey
        '+91': '+91',    # India
        '+92': '+92',    # Pakistan
        '+93': '+93',    # Afghanistan
        '+94': '+94',    # Sri Lanka
        '+95': '+95',    # Myanmar
        '+98': '+98',    # Iran
        '+212': '+212',  # Morocco
        '+213': '+213',  # Algeria
        '+216': '+216',  # Tunisia
        '+218': '+218',  # Libya
        '+220': '+220',  # Gambia
        '+221': '+221',  # Senegal
        '+222': '+222',  # Mauritania
        '+223': '+223',  # Mali
        '+224': '+224',  # Guinea
        '+225': '+225',  # Ivory Coast
        '+226': '+226',  # Burkina Faso
        '+227': '+227',  # Niger
        '+228': '+228',  # Togo
        '+229': '+229',  # Benin
        '+230': '+230',  # Mauritius
        '+231': '+231',  # Liberia
        '+232': '+232',  # Sierra Leone
        '+233': '+233',  # Ghana
        '+234': '+234',  # Nigeria
        '+235': '+235',  # Chad
        '+236': '+236',  # Central African Republic
        '+237': '+237',  # Cameroon
        '+238': '+238',  # Cape Verde
        '+239': '+239',  # Sao Tome and Principe
        '+240': '+240',  # Equatorial Guinea
        '+241': '+241',  # Gabon
        '+242': '+242',  # Republic of the Congo
        '+243': '+243',  # Democratic Republic of the Congo
        '+244': '+244',  # Angola
        '+245': '+245',  # Guinea-Bissau
        '+246': '+246',  # British Indian Ocean Territory
        '+248': '+248',  # Seychelles
        '+249': '+249',  # Sudan
        '+250': '+250',  # Rwanda
        '+251': '+251',  # Ethiopia
        '+252': '+252',  # Somalia
        '+253': '+253',  # Djibouti
        '+254': '+254',  # Kenya
        '+255': '+255',  # Tanzania
        '+256': '+256',  # Uganda
        '+257': '+257',  # Burundi
        '+258': '+258',  # Mozambique
        '+260': '+260',  # Zambia
        '+261': '+261',  # Madagascar
        '+262': '+262',  # Reunion
        '+263': '+263',  # Zimbabwe
        '+264': '+264',  # Namibia
        '+265': '+265',  # Malawi
        '+266': '+266',  # Lesotho
        '+267': '+267',  # Botswana
        '+268': '+268',  # Swaziland
        '+269': '+269',  # Comoros
        '+290': '+290',  # Saint Helena
        '+291': '+291',  # Eritrea
        '+297': '+297',  # Aruba
        '+298': '+298',  # Faroe Islands
        '+299': '+299',  # Greenland
        '+350': '+350',  # Gibraltar
        '+351': '+351',  # Portugal
        '+352': '+352',  # Luxembourg
        '+353': '+353',  # Ireland
        '+354': '+354',  # Iceland
        '+355': '+355',  # Albania
        '+356': '+356',  # Malta
        '+357': '+357',  # Cyprus
        '+358': '+358',  # Finland
        '+359': '+359',  # Bulgaria
        '+370': '+370',  # Lithuania
        '+371': '+371',  # Latvia
        '+372': '+372',  # Estonia
        '+373': '+373',  # Moldova
        '+374': '+374',  # Armenia
        '+375': '+375',  # Belarus
        '+376': '+376',  # Andorra
        '+377': '+377',  # Monaco
        '+378': '+378',  # San Marino
        '+380': '+380',  # Ukraine
        '+381': '+381',  # Serbia
        '+382': '+382',  # Montenegro
        '+383': '+383',  # Kosovo
        '+385': '+385',  # Croatia
        '+386': '+386',  # Slovenia
        '+387': '+387',  # Bosnia and Herzegovina
        '+389': '+389',  # North Macedonia
        '+420': '+420',  # Czech Republic
        '+421': '+421',  # Slovakia
        '+423': '+423',  # Liechtenstein
        '+500': '+500',  # Falkland Islands
        '+501': '+501',  # Belize
        '+502': '+502',  # Guatemala
        '+503': '+503',  # El Salvador
        '+504': '+504',  # Honduras
        '+505': '+505',  # Nicaragua
        '+506': '+506',  # Costa Rica
        '+507': '+507',  # Panama
        '+508': '+508',  # Saint Pierre and Miquelon
        '+509': '+509',  # Haiti
        '+590': '+590',  # Guadeloupe
        '+591': '+591',  # Bolivia
        '+592': '+592',  # Guyana
        '+593': '+593',  # Ecuador
        '+594': '+594',  # French Guiana
        '+595': '+595',  # Paraguay
        '+596': '+596',  # Martinique
        '+597': '+597',  # Suriname
        '+598': '+598',  # Uruguay
        '+599': '+599',  # Netherlands Antilles
        '+670': '+670',  # East Timor
        '+672': '+672',  # Antarctica
        '+673': '+673',  # Brunei
        '+674': '+674',  # Nauru
        '+675': '+675',  # Papua New Guinea
        '+676': '+676',  # Tonga
        '+677': '+677',  # Solomon Islands
        '+678': '+678',  # Vanuatu
        '+679': '+679',  # Fiji
        '+680': '+680',  # Palau
        '+681': '+681',  # Wallis and Futuna
        '+682': '+682',  # Cook Islands
        '+683': '+683',  # Niue
        '+684': '+684',  # American Samoa
        '+685': '+685',  # Samoa
        '+686': '+686',  # Kiribati
        '+687': '+687',  # New Caledonia
        '+688': '+688',  # Tuvalu
        '+689': '+689',  # French Polynesia
        '+690': '+690',  # Tokelau
        '+691': '+691',  # Micronesia
        '+692': '+692',  # Marshall Islands
        '+850': '+850',  # North Korea
        '+852': '+852',  # Hong Kong
        '+853': '+853',  # Macau
        '+855': '+855',  # Cambodia
        '+856': '+856',  # Laos
        '+880': '+880',  # Bangladesh
        '+886': '+886',  # Taiwan
        '+960': '+960',  # Maldives
        '+961': '+961',  # Lebanon
        '+962': '+962',  # Jordan
        '+963': '+963',  # Syria
        '+964': '+964',  # Iraq
        '+965': '+965',  # Kuwait
        '+966': '+966',  # Saudi Arabia
        '+967': '+967',  # Yemen
        '+968': '+968',  # Oman
        '+970': '+970',  # Palestine
        '+971': '+971',  # United Arab Emirates
        '+972': '+972',  # Israel
        '+973': '+973',  # Bahrain
        '+974': '+974',  # Qatar
        '+975': '+975',  # Bhutan
        '+976': '+976',  # Mongolia
        '+977': '+977',  # Nepal
        '+992': '+992',  # Tajikistan
        '+993': '+993',  # Turkmenistan
        '+994': '+994',  # Azerbaijan
        '+995': '+995',  # Georgia
        '+996': '+996',  # Kyrgyzstan
        '+998': '+998',  # Uzbekistan
    }
    
    # Check for exact matches (longest first)
    for length in [4, 3, 2]:
        if len(phone) >= length + 1:  # +1 for the '+' sign
            prefix = phone[:length + 1]
            if prefix in country_codes:
                return country_codes[prefix]
    
    # Default fallback
    return '+1'  # Default to US/Canada if no match found

def get_country_name_and_flag(country_code: str) -> tuple[str, str]:
    """Get country name and flag from country code"""
    country_info = {
        '+1': ('الولايات المتحدة', '🇺🇸'),
        '+7': ('روسيا', '🇷🇺'),
        '+20': ('مصر', '🇪🇬'),
        '+33': ('فرنسا', '🇫🇷'),
        '+34': ('إسبانيا', '🇪🇸'),
        '+39': ('إيطاليا', '🇮🇹'),
        '+44': ('المملكة المتحدة', '🇬🇧'),
        '+49': ('ألمانيا', '🇩🇪'),
        '+52': ('المكسيك', '🇲🇽'),
        '+55': ('البرازيل', '🇧🇷'),
        '+60': ('ماليزيا', '🇲🇾'),
        '+61': ('أستراليا', '🇦🇺'),
        '+62': ('إندونيسيا', '🇮🇩'),
        '+63': ('الفلبين', '🇵🇭'),
        '+64': ('نيوزيلندا', '🇳🇿'),
        '+65': ('سنغافورة', '🇸🇬'),
        '+66': ('تايلاند', '🇹🇭'),
        '+81': ('اليابان', '🇯🇵'),
        '+82': ('كوريا الجنوبية', '🇰🇷'),
        '+84': ('فيتنام', '🇻🇳'),
        '+86': ('الصين', '🇨🇳'),
        '+90': ('تركيا', '🇹🇷'),
        '+91': ('الهند', '🇮🇳'),
        '+92': ('باكستان', '🇵🇰'),
        '+93': ('أفغانستان', '🇦🇫'),
        '+94': ('سريلانكا', '🇱🇰'),
        '+95': ('ميانمار', '🇲🇲'),
        '+98': ('إيران', '🇮🇷'),
        '+212': ('المغرب', '🇲🇦'),
        '+213': ('الجزائر', '🇩🇿'),
        '+216': ('تونس', '🇹🇳'),
        '+218': ('ليبيا', '🇱🇾'),
        '+220': ('غامبيا', '🇬🇲'),
        '+221': ('السنغال', '🇸🇳'),
        '+222': ('موريتانيا', '🇲🇷'),
        '+223': ('مالي', '🇲🇱'),
        '+224': ('غينيا', '🇬🇳'),
        '+225': ('ساحل العاج', '🇨🇮'),
        '+226': ('بوركينا فاسو', '🇧🇫'),
        '+227': ('النيجر', '🇳🇪'),
        '+228': ('توغو', '🇹🇬'),
        '+229': ('بنين', '🇧🇯'),
        '+230': ('موريشيوس', '🇲🇺'),
        '+231': ('ليبيريا', '🇱🇷'),
        '+232': ('سيراليون', '🇸🇱'),
        '+233': ('غانا', '🇬🇭'),
        '+234': ('نيجيريا', '🇳🇬'),
        '+235': ('تشاد', '🇹🇩'),
        '+236': ('جمهورية أفريقيا الوسطى', '🇨🇫'),
        '+237': ('الكاميرون', '🇨🇲'),
        '+238': ('الرأس الأخضر', '🇨🇻'),
        '+239': ('ساو تومي وبرينسيبي', '🇸🇹'),
        '+240': ('غينيا الاستوائية', '🇬🇶'),
        '+241': ('الغابون', '🇬🇦'),
        '+242': ('جمهورية الكونغو', '🇨🇬'),
        '+243': ('جمهورية الكونغو الديمقراطية', '🇨🇩'),
        '+244': ('أنغولا', '🇦🇴'),
        '+245': ('غينيا بيساو', '🇬🇼'),
        '+248': ('سيشل', '🇸🇨'),
        '+249': ('السودان', '🇸🇩'),
        '+250': ('رواندا', '🇷🇼'),
        '+251': ('إثيوبيا', '🇪🇹'),
        '+252': ('الصومال', '🇸🇴'),
        '+253': ('جيبوتي', '🇩🇯'),
        '+254': ('كينيا', '🇰🇪'),
        '+255': ('تنزانيا', '🇹🇿'),
        '+256': ('أوغندا', '🇺🇬'),
        '+257': ('بوروندي', '🇧🇮'),
        '+258': ('موزمبيق', '🇲🇿'),
        '+260': ('زامبيا', '🇿🇲'),
        '+261': ('مدغشقر', '🇲🇬'),
        '+263': ('زيمبابوي', '🇿🇼'),
        '+264': ('ناميبيا', '🇳🇦'),
        '+265': ('ملاوي', '🇲🇼'),
        '+266': ('ليسوتو', '🇱🇸'),
        '+267': ('بوتسوانا', '🇧🇼'),
        '+268': ('إسواتيني', '🇸🇿'),
        '+269': ('جزر القمر', '🇰🇲'),
        '+351': ('البرتغال', '🇵🇹'),
        '+352': ('لوكسمبورغ', '🇱🇺'),
        '+353': ('أيرلندا', '🇮🇪'),
        '+354': ('أيسلندا', '🇮🇸'),
        '+355': ('ألبانيا', '🇦🇱'),
        '+356': ('مالطا', '🇲🇹'),
        '+357': ('قبرص', '🇨🇾'),
        '+358': ('فنلندا', '🇫🇮'),
        '+359': ('بلغاريا', '🇧🇬'),
        '+370': ('ليتوانيا', '🇱🇹'),
        '+371': ('لاتفيا', '🇱🇻'),
        '+372': ('إستونيا', '🇪🇪'),
        '+373': ('مولدوفا', '🇲🇩'),
        '+374': ('أرمينيا', '🇦🇲'),
        '+375': ('بيلاروس', '🇧🇾'),
        '+376': ('أندورا', '🇦🇩'),
        '+377': ('موناكو', '🇲🇨'),
        '+378': ('سان مارينو', '🇸🇲'),
        '+380': ('أوكرانيا', '🇺🇦'),
        '+381': ('صربيا', '🇷🇸'),
        '+382': ('الجبل الأسود', '🇲🇪'),
        '+383': ('كوسوفو', '🇽🇰'),
        '+385': ('كرواتيا', '🇭🇷'),
        '+386': ('سلوفينيا', '🇸🇮'),
        '+387': ('البوسنة والهرسك', '🇧🇦'),
        '+389': ('مقدونيا الشمالية', '🇲🇰'),
        '+420': ('التشيك', '🇨🇿'),
        '+421': ('سلوفاكيا', '🇸🇰'),
        '+423': ('ليختنشتاين', '🇱🇮'),
        '+500': ('جزر فوكلاند', '🇫🇰'),
        '+501': ('بليز', '🇧🇿'),
        '+502': ('غواتيمالا', '🇬🇹'),
        '+503': ('السلفادور', '🇸🇻'),
        '+504': ('هندوراس', '🇭🇳'),
        '+505': ('نيكاراغوا', '🇳🇮'),
        '+506': ('كوستاريكا', '🇨🇷'),
        '+507': ('بنما', '🇵🇦'),
        '+509': ('هايتي', '🇭🇹'),
        '+590': ('غوادلوب', '🇬🇵'),
        '+591': ('بوليفيا', '🇧🇴'),
        '+592': ('غيانا', '🇬🇾'),
        '+593': ('الإكوادور', '🇪🇨'),
        '+594': ('غيانا الفرنسية', '🇬🇫'),
        '+595': ('باراغواي', '🇵🇾'),
        '+596': ('مارتينيك', '🇲🇶'),
        '+597': ('سورينام', '🇸🇷'),
        '+598': ('أوروغواي', '🇺🇾'),
        '+670': ('تيمور الشرقية', '🇹🇱'),
        '+673': ('بروناي', '🇧🇳'),
        '+674': ('ناورو', '🇳🇷'),
        '+675': ('بابوا غينيا الجديدة', '🇵🇬'),
        '+676': ('تونغا', '🇹🇴'),
        '+677': ('جزر سليمان', '🇸🇧'),
        '+678': ('فانواتو', '🇻🇺'),
        '+679': ('فيجي', '🇫🇯'),
        '+680': ('بالاو', '🇵🇼'),
        '+681': ('واليس وفوتونا', '🇼🇫'),
        '+682': ('جزر كوك', '🇨🇰'),
        '+683': ('نيوي', '🇳🇺'),
        '+684': ('ساموا الأمريكية', '🇦🇸'),
        '+685': ('ساموا', '🇼🇸'),
        '+686': ('كيريباتي', '🇰🇮'),
        '+687': ('كاليدونيا الجديدة', '🇳🇨'),
        '+688': ('توفالو', '🇹🇻'),
        '+689': ('بولينيزيا الفرنسية', '🇵🇫'),
        '+690': ('توكيلاو', '🇹🇰'),
        '+691': ('ميكرونيزيا', '🇫🇲'),
        '+692': ('جزر مارشال', '🇲🇭'),
        '+850': ('كوريا الشمالية', '🇰🇵'),
        '+852': ('هونغ كونغ', '🇭🇰'),
        '+853': ('ماكاو', '🇲🇴'),
        '+855': ('كمبوديا', '🇰🇭'),
        '+856': ('لاوس', '🇱🇦'),
        '+880': ('بنغلاديش', '🇧🇩'),
        '+886': ('تايوان', '🇹🇼'),
        '+960': ('المالديف', '🇲🇻'),
        '+961': ('لبنان', '🇱🇧'),
        '+962': ('الأردن', '🇯🇴'),
        '+963': ('سوريا', '🇸🇾'),
        '+964': ('العراق', '🇮🇶'),
        '+965': ('الكويت', '🇰🇼'),
        '+966': ('السعودية', '🇸🇦'),
        '+967': ('اليمن', '🇾🇪'),
        '+968': ('عمان', '🇴🇲'),
        '+970': ('فلسطين', '🇵🇸'),
        '+971': ('الإمارات', '🇦🇪'),
        '+972': ('إسرائيل', '🇮🇱'),
        '+973': ('البحرين', '🇧🇭'),
        '+974': ('قطر', '🇶🇦'),
        '+975': ('بوتان', '🇧🇹'),
        '+976': ('منغوليا', '🇲🇳'),
        '+977': ('نيبال', '🇳🇵'),
        '+992': ('طاجيكستان', '🇹🇯'),
        '+993': ('تركمانستان', '🇹🇲'),
        '+994': ('أذربيجان', '🇦🇿'),
        '+995': ('جورجيا', '🇬🇪'),
        '+996': ('قيرغيزستان', '🇰🇬'),
        '+998': ('أوزبكستان', '🇺🇿'),
    }
    
    return country_info.get(country_code, ('دولة غير معروفة', '🌍'))

def ensure_service_country_exists(service_id: int, country_code: str, db_session) -> ServiceCountry:
    """Ensure ServiceCountry entry exists for the given service and country code"""
    # Check if ServiceCountry already exists
    service_country = db_session.query(ServiceCountry).filter(
        ServiceCountry.service_id == service_id,
        ServiceCountry.country_code == country_code
    ).first()
    
    if not service_country:
        # Get country name and flag
        country_name, flag = get_country_name_and_flag(country_code)
        
        # Create new ServiceCountry entry
        service_country = ServiceCountry(
            service_id=service_id,
            country_name=country_name,
            country_code=country_code,
            flag=flag,
            active=True
        )
        db_session.add(service_country)
        db_session.flush()  # Flush to get the ID
        
        logger.info(f"Auto-created ServiceCountry: {country_name} ({country_code}) for service {service_id}")
    
    return service_country

async def notify_admin_low_stock(service_id: int, country_code: str, country_name: str):
    """Notify admin when a country runs out of numbers"""
    try:
        message = (
            f"⚠️ تنبيه نفاد المخزون!\n\n"
            f"🌍 الدولة: {country_name} ({country_code})\n"
            f"📱 الخدمة: {service_id}\n\n"
            f"لا توجد أرقام متاحة لهذه الدولة.\n"
            f"يرجى إضافة أرقام جديدة."
        )
        
        await bot.send_message(ADMIN_ID, message)
        logger.info(f"Sent low stock notification for {country_name} ({country_code})")
    except Exception as e:
        logger.error(f"Failed to send low stock notification: {e}")

async def check_and_notify_empty_countries():
    """Check for countries with no available numbers and notify admin"""
    db = get_db()
    try:
        # Get all service-country combinations with their available number counts
        countries_with_zero = db.query(ServiceCountry).filter(
            ServiceCountry.active == True
        ).all()
        
        for service_country in countries_with_zero:
            available_count = db.query(Number).filter(
                Number.service_id == service_country.service_id,
                Number.country_code == service_country.country_code,
                Number.status == 'AVAILABLE'
            ).count()
            
            if available_count == 0:
                # Check if we already notified recently (within last hour)
                # This is a simple check to avoid spam notifications
                await notify_admin_low_stock(
                    int(service_country.service_id), 
                    str(service_country.country_code), 
                    str(service_country.country_name)
                )
                
    finally:
        db.close()


def format_sms_message(phone_number: str, code: str) -> str:
    """Format SMS message in 'to: code:' format"""
    try:
        normalized_phone = normalize_phone_number(phone_number)
        return f"to: {normalized_phone}\ncode: {code}"
    except Exception as e:
        logger.error(f"Error formatting SMS message: {e}")
        return f"to: {phone_number}\ncode: {code}"

def create_example_sms_message(service_name: str = "Example", phone_number: str = "+1234567890", code: str = "123456") -> str:
    """Create an example SMS message for group demonstration"""
    formatted = format_sms_message(phone_number, code)
    return f"📱 {service_name} SMS:\n{formatted}"

async def send_formatted_sms_to_group(group_chat_id: str, phone_number: str, code: str, service_name: str = "SMS"):
    """Send formatted SMS message to a group"""
    try:
        formatted_message = create_example_sms_message(service_name, phone_number, code)
        await bot.send_message(
            chat_id=group_chat_id,
            text=formatted_message
        )
        return True
    except Exception as e:
        logger.error(f"Error sending formatted SMS to group {group_chat_id}: {e}")
        return False

def test_extract_number_and_code():
    """Test function for number and code extraction"""
    test_cases = [
        "to:+20112763404 code:123456",
        "to: +20112763404 code: 123456", 
        "TO:+20112763404 CODE:123456",
        "message to:+20112763404 code:654321 end",
        "استلمت رمز to:+971501234567 code:789012 للتحقق"
    ]
    
    print("Testing extract_number_and_code function:")
    for i, test_msg in enumerate(test_cases, 1):
        number, code = extract_number_and_code(test_msg, r'\d{6}')
        print(f"Test {i}: '{test_msg}' -> number={number}, code={code}")
    
    return True

def extract_number_and_code(message_text: str, regex_pattern: str) -> tuple[Optional[str], Optional[str]]:
    """Extract phone number and code from message text in format: to:+20112763404 code:123456"""
    try:
        # Extract number from 'to:' format (with or without spaces, with or without +)
        number_match = re.search(r'to:\s*(\+?\d+)', message_text, re.IGNORECASE)
        if number_match:
            raw_number = number_match.group(1)
            # Add + if not present
            if not raw_number.startswith('+'):
                raw_number = '+' + raw_number
            number = normalize_phone_number(raw_number)
        else:
            number = None
        
        # Extract code from 'code:' format (with or without spaces)
        code_match = re.search(r'code:\s*(\d+)', message_text, re.IGNORECASE)
        if code_match:
            code = code_match.group(1)
        else:
            # Fallback to service-specific regex pattern
            code_match = re.search(regex_pattern, message_text)
            code = code_match.group() if code_match else None
        
        # Log for debugging
        if number and code:
            logger.info(f"Successfully extracted from '{message_text}': number={number}, code={code}")
        else:
            logger.warning(f"Failed to extract from '{message_text}': number={number}, code={code}")
        
        return number, code
    except Exception as e:
        logger.error(f"Error extracting number and code from '{message_text}': {e}")
        return None, None

async def is_user_admin_in_chat(user_id: int, chat_id: str) -> bool:
    """Check if user is admin in the chat"""
    try:
        chat_member = await bot.get_chat_member(chat_id, user_id)
        return chat_member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False

async def extract_code_from_message(text: str, service_name: str) -> Optional[str]:
    """Enhanced OTP code extraction with advanced pattern matching and context awareness"""
    db = get_db()
    try:
        service = db.query(Service).filter(Service.name == service_name).first()
        if not service:
            logger.warning(f"Service '{service_name}' not found for code extraction")
            return None
        
        # Get service-specific regex pattern
        mapping = db.query(ServiceProviderMap).filter(ServiceProviderMap.service_id == service.id).first()
        service_pattern = str(mapping.regex_pattern) if mapping else r'\b\d{4,8}\b'
        
        # Enhanced pattern collection with multilingual support
        patterns = [
            # Service-specific pattern first (highest priority)
            service_pattern,
            
            # Direct code patterns (Arabic and English)
            r'(?:code|كود|رمز)\s*:?\s*(\d{4,8})',
            r'(?:verification|تحقق|تأكيد)\s*:?\s*(\d{4,8})',
            r'(?:otp|كلمة مرور)\s*:?\s*(\d{4,8})',
            r'(?:pin|رقم سري)\s*:?\s*(\d{4,8})',
            
            # Service-specific patterns
            f'{service_name.lower()}\\s*:?\\s*(\\d{{4,8}})',
            
            # Context-aware patterns
            r'your\s+(?:code|verification)\s+is\s*:?\s*(\d{4,8})',
            r'enter\s+(?:code|pin)\s*:?\s*(\d{4,8})',
            r'use\s+(?:code|pin)\s*:?\s*(\d{4,8})',
            r'كودك\s+هو\s*:?\s*(\d{4,8})',
            r'رمز\s+التحقق\s*:?\s*(\d{4,8})',
            
            # Fallback patterns for common formats
            r'(\d{4,8})\s*(?:is|هو)\s+(?:your|كودك)',
            r'(?:confirm|تأكيد).*?(\d{4,8})',
            r'(?:security|أمان).*?(\d{4,8})',
            
            # Last resort: isolated numbers
            r'\b(\d{4,8})\b'
        ]
        
        # Try each pattern with scoring system
        candidates = []
        text_lower = text.lower()
        
        for i, pattern in enumerate(patterns):
            try:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    code = match if isinstance(match, str) else match[0] if isinstance(match, tuple) else str(match)
                    
                    # Validate code
                    if code.isdigit() and 4 <= len(code) <= 8:
                        # Calculate confidence score
                        score = 100 - (i * 5)  # Earlier patterns get higher scores
                        
                        # Bonus points for context
                        if any(keyword in text_lower for keyword in ['code', 'verification', 'otp', 'كود', 'رمز', 'تحقق']):
                            score += 20
                        
                        # Bonus for service name in text
                        if service_name.lower() in text_lower:
                            score += 15
                        
                        # Penalty for very common patterns that might be noise
                        if len(code) == 4 and code in ['1234', '0000', '9999']:
                            score -= 30
                        
                        # Penalty if it looks like a phone number
                        if len(code) > 6 and (code.startswith('20') or code.startswith('1')):
                            score -= 25
                        
                        candidates.append((code, score, i))
                        
            except Exception as pattern_error:
                logger.warning(f"Pattern '{pattern}' failed: {pattern_error}")
                continue
        
        # Sort by score and return best candidate
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_code, best_score, pattern_idx = candidates[0]
            
            logger.info(f"✅ Code extracted: '{best_code}' (score: {best_score}, pattern: {pattern_idx}) from: '{text[:100]}...'")
            return best_code
        
        logger.warning(f"❌ No valid code found in message: '{text[:100]}...'")
        return None
        
    except Exception as e:
        logger.error(f"Error in enhanced code extraction: {e}")
        return None
    finally:
        db.close()

# ==== AUTOMATIC MESSAGE CLEANUP SYSTEM ====

def cleanup_dead_messages():
    """Background function to automatically cleanup dead and old messages"""
    try:
        db = get_db()
        current_time = datetime.now()
        
        # Define cleanup criteria
        old_message_cutoff = current_time - timedelta(days=message_retention_days)
        orphan_message_cutoff = current_time - timedelta(hours=orphan_message_retention_hours)
        blocked_message_cutoff = current_time - timedelta(hours=12)  # Clean blocked messages after 12 hours
        
        # Cleanup old provider messages
        deleted_provider = db.query(ProviderMessage).filter(
            ProviderMessage.received_at < old_message_cutoff
        ).delete()
        
        # Cleanup old orphan messages (unmatched messages)
        deleted_orphan = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.ORPHAN,
            ProviderMessage.received_at < orphan_message_cutoff
        ).delete()
        
        # Cleanup old blocked messages
        deleted_blocked = db.query(BlockedMessage).filter(
            BlockedMessage.created_at < blocked_message_cutoff
        ).delete()
        
        # Cleanup old rejected messages
        deleted_rejected = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.REJECTED,
            ProviderMessage.received_at < old_message_cutoff
        ).delete()
        
        # Cleanup processed messages older than retention period
        deleted_processed = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.PROCESSED,
            ProviderMessage.received_at < old_message_cutoff
        ).delete()
        
        db.commit()
        
        total_deleted = deleted_provider + deleted_orphan + deleted_blocked + deleted_rejected + deleted_processed
        
        if total_deleted > 0:
            logger.info(
                f"🗑️ Auto cleanup completed: "
                f"Provider: {deleted_provider}, "
                f"Orphan: {deleted_orphan}, "
                f"Blocked: {deleted_blocked}, "
                f"Rejected: {deleted_rejected}, "
                f"Processed: {deleted_processed}. "
                f"Total: {total_deleted} messages deleted"
            )
        else:
            logger.info("🗑️ Auto cleanup completed: No old messages to clean")
            
    except Exception as e:
        logger.error(f"❌ Error in automatic message cleanup: {e}")
        try:
            db.rollback()
        except:
            pass
    finally:
        try:
            db.close()
        except:
            pass

def cleanup_expired_reservations():
    """Clean up expired reservations and release numbers"""
    try:
        db = get_db()
        current_time = datetime.now()
        
        # Find expired reservations
        expired_reservations = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.WAITING_CODE,
            Reservation.expired_at < current_time
        ).all()
        
        released_count = 0
        for reservation in expired_reservations:
            # Update reservation status
            reservation.status = ReservationStatus.EXPIRED
            
            # Release the number
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if number:
                number.status = 'AVAILABLE'
                number.reserved_by_user_id = None
                number.reserved_at = None
                number.expires_at = None
                released_count += 1
        
        db.commit()
        
        if released_count > 0:
            logger.info(f"📱 Released {released_count} expired number reservations")
            
    except Exception as e:
        logger.error(f"❌ Error cleaning expired reservations: {e}")
        try:
            db.rollback()
        except:
            pass
    finally:
        try:
            db.close()
        except:
            pass

def periodic_cleanup_worker():
    """Background worker that runs cleanup tasks periodically"""
    logger.info(f"🔄 Auto cleanup worker started - Running every {cleanup_interval_hours} hours")
    
    while auto_cleanup_enabled:
        try:
            # Run message cleanup
            cleanup_dead_messages()
            
            # Run reservation cleanup
            cleanup_expired_reservations()
            
            # Wait for next cleanup cycle
            sleep_time = cleanup_interval_hours * 3600  # Convert hours to seconds
            logger.info(f"⏰ Next auto cleanup in {cleanup_interval_hours} hours")
            
            for _ in range(int(sleep_time / 60)):  # Check every minute if cleanup is still enabled
                if not auto_cleanup_enabled:
                    break
                time.sleep(60)
                
        except Exception as e:
            logger.error(f"❌ Error in cleanup worker: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying on error
    
    logger.info("🛑 Auto cleanup worker stopped")

def start_auto_cleanup():
    """Start the automatic cleanup system"""
    global auto_cleanup_enabled
    auto_cleanup_enabled = True
    
    # Start cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup_worker, daemon=True)
    cleanup_thread.start()
    
    logger.info("✅ Automatic message cleanup system started")

def stop_auto_cleanup():
    """Stop the automatic cleanup system"""
    global auto_cleanup_enabled
    auto_cleanup_enabled = False
    logger.info("🛑 Automatic message cleanup system stopped")

async def create_main_keyboard(user_id: str = None) -> InlineKeyboardMarkup:
    """Create main menu keyboard"""
    keyboard = InlineKeyboardBuilder()
    
    # Get user language
    lang_code = 'ar'  # Default to Arabic
    if user_id:
        lang_code = get_user_language(user_id)
    
    # Get active services
    db = get_db()
    try:
        services = db.query(Service).filter(Service.active == True).all()
        
        # Add service buttons (2 per row)
        for i in range(0, len(services), 2):
            row = []
            for j in range(2):
                if i + j < len(services):
                    service = services[i + j]
                    translated_name = await get_text(str(service.name), lang_code)
                    row.append(InlineKeyboardButton(
                        text=f"{str(service.emoji)} {translated_name}",
                        callback_data=f"svc_{service.id}"
                    ))
            keyboard.row(*row)
        
        # Additional buttons with localization
        free_credits_text = t('free_credits', lang_code)
        balance_text = t('my_balance', lang_code)
        
        keyboard.row(
            InlineKeyboardButton(text=free_credits_text, callback_data="free_credits"),
            InlineKeyboardButton(text=balance_text, callback_data="my_balance")
        )
        
        # Add stats button
        keyboard.row(
            InlineKeyboardButton(text="📊 النسب والإحصائيات", callback_data="view_stats")
        )
        
        # Show admin button only for admin
        if user_id and (int(user_id) == ADMIN_ID or is_admin_session_valid(int(user_id))):
            keyboard.row(
                InlineKeyboardButton(text=t('help', lang_code), callback_data="help"),
                InlineKeyboardButton(text=t('admin_panel', lang_code), callback_data="admin")
            )
        else:
            keyboard.row(
                InlineKeyboardButton(text=t('help', lang_code), callback_data="help"),
                InlineKeyboardButton(text=t('settings', lang_code), callback_data="settings")
            )
        
        return keyboard.as_markup()
    finally:
        db.close()

def create_countries_keyboard(service_id: int, user_id: Optional[int] = None, page: int = 0) -> InlineKeyboardMarkup:
    """Create countries selection keyboard for a service"""
    keyboard = InlineKeyboardBuilder()
    
    db = get_db()
    try:
        # First, get all countries for this service and filter those with available numbers
        all_countries = db.query(ServiceCountry).filter(
            ServiceCountry.service_id == service_id,
            ServiceCountry.active == True
        ).all()
        
        # Filter countries to only include those with available numbers for this user
        countries_with_numbers = []
        for country in all_countries:
            if user_id:
                # Find numbers that this user has already used
                used_number_ids = db.query(Reservation.number_id).filter(
                    Reservation.user_id == user_id,
                    Reservation.status == ReservationStatus.COMPLETED
                ).subquery()
                
                # Count available numbers that user hasn't used
                available_count = db.query(Number).filter(
                    Number.service_id == service_id,
                    Number.country_code == country.country_code,
                    Number.status == 'AVAILABLE',
                    ~Number.id.in_(used_number_ids)
                ).count()
            else:
                # If no user_id provided, show all available numbers
                available_count = db.query(Number).filter(
                    Number.service_id == service_id,
                    Number.country_code == country.country_code,
                    Number.status == 'AVAILABLE'
                ).count()
            
            # Only include countries with available numbers
            if available_count > 0:
                countries_with_numbers.append((country, available_count))
        
        # Sort countries by name for consistent display
        countries_with_numbers.sort(key=lambda x: x[0].country_name)
        
        # Apply pagination to filtered results
        total_countries_with_numbers = len(countries_with_numbers)
        start_index = page * PAGE_SIZE
        end_index = start_index + PAGE_SIZE
        page_countries = countries_with_numbers[start_index:end_index]
        
        # Create buttons for countries on current page
        for country, available_count in page_countries:
            keyboard.row(InlineKeyboardButton(
                text=f"{country.flag} {country.country_name} (✅ {available_count})",
                callback_data=f"cty_{service_id}_{country.country_code}"
            ))
        
        # Navigation buttons based on filtered results
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⏮️ السابق", callback_data=f"cty_page_{service_id}_{page-1}"))
        
        if end_index < total_countries_with_numbers:
            nav_buttons.append(InlineKeyboardButton(text="⏭️ التالي", callback_data=f"cty_page_{service_id}_{page+1}"))
        
        if nav_buttons:
            keyboard.row(*nav_buttons)
        
        # Add information about current page
        if total_countries_with_numbers > PAGE_SIZE:
            current_start = start_index + 1
            current_end = min(end_index, total_countries_with_numbers)
            keyboard.row(InlineKeyboardButton(
                text=f"📄 {current_start}-{current_end} من {total_countries_with_numbers}",
                callback_data="no_action"
            ))
        
        keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
        
        return keyboard.as_markup()
    finally:
        db.close()

def create_service_groups_keyboard() -> InlineKeyboardMarkup:
    """Create service groups management keyboard"""
    keyboard = InlineKeyboardBuilder()
    
    db = get_db()
    try:
        service_groups = db.query(ServiceGroup).join(Service).filter(
            ServiceGroup.active == True,
            Service.active == True
        ).all()
        
        for sg in service_groups:
            status = "✅" if sg.active else "❌"
            keyboard.row(InlineKeyboardButton(
                text=f"{status} {sg.service.emoji} {sg.service.name} - Group: {sg.group_chat_id}",
                callback_data=f"edit_service_group_{sg.id}"
            ))
        
        keyboard.row(
            InlineKeyboardButton(text="➕ ربط خدمة بجروب", callback_data="admin_add_service"),
            InlineKeyboardButton(text="📊 إحصائيات الرسائل", callback_data="admin_messages_stats")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        return keyboard.as_markup()
    finally:
        db.close()

def create_number_action_keyboard(reservation_id: int) -> InlineKeyboardMarkup:
    """Create keyboard for number actions"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔄 تغيير الرقم", callback_data=f"change_number_{reservation_id}"),
        InlineKeyboardButton(text="🌍 تغيير الدولة", callback_data=f"change_country_{reservation_id}")
    )
    keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
    return keyboard.as_markup()

def create_admin_keyboard(user_id: int = None) -> InlineKeyboardMarkup:
    """Create admin panel keyboard"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🛠 إدارة الخدمات", callback_data="admin_services"),
        InlineKeyboardButton(text="🌍 إدارة الدول", callback_data="admin_countries")
    )
    keyboard.row(
        InlineKeyboardButton(text="📱 إدارة الأرقام", callback_data="admin_numbers"),
        InlineKeyboardButton(text="🔗 إدارة الجروبات", callback_data="admin_service_groups")
    )
    keyboard.row(
        InlineKeyboardButton(text="👥 إدارة المستخدمين", callback_data="admin_users"),
        InlineKeyboardButton(text="📢 إدارة القنوات", callback_data="admin_channels")
    )
    keyboard.row(
        InlineKeyboardButton(text="💰 شحن رصيد", callback_data="admin_add_balance"),
        InlineKeyboardButton(text="💳 خصم رصيد", callback_data="admin_deduct_balance")
    )
    keyboard.row(
        InlineKeyboardButton(text="📢 رسالة جماعية", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="💬 رسالة خاصة", callback_data="admin_private_message")
    )
    keyboard.row(
        InlineKeyboardButton(text="📦 المخزون", callback_data="admin_inventory"),
        InlineKeyboardButton(text="📊 الإحصائيات", callback_data="admin_stats")
    )
    keyboard.row(
        InlineKeyboardButton(text="⚙️ الإعدادات", callback_data="admin_settings"),
        InlineKeyboardButton(text="🔧 وضع الصيانة", callback_data="admin_maintenance")
    )
    keyboard.row(
        InlineKeyboardButton(text="📋 قناة بيانات المستخدمين", callback_data="admin_user_data_channel"),
        InlineKeyboardButton(text="🔒 الاشتراك الإجباري", callback_data="admin_forced_subscription")
    )
    
    # Add password change button only for main admin (7011309417)
    if user_id == ADMIN_ID:
        keyboard.row(InlineKeyboardButton(text="🔑 تغيير كلمة المرور", callback_data="admin_change_password"))
    
    keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
    return keyboard.as_markup()

async def reserve_number(user_id: int, service_id: int, country_code: str) -> Optional[Reservation]:
    """Reserve a number for user"""
    db = get_db()
    try:
        # Find numbers that this user has already used
        used_number_ids = db.query(Reservation.number_id).filter(
            Reservation.user_id == user_id,
            Reservation.status == ReservationStatus.COMPLETED
        ).subquery()
        
        # Find available number that user hasn't used before
        available_number = db.query(Number).filter(
            Number.service_id == service_id,
            Number.country_code == country_code,
            Number.status == 'AVAILABLE',
            ~Number.id.in_(used_number_ids)  # Exclude numbers already used by this user
        ).first()
        
        if not available_number:
            return None
        
        # Create reservation
        expires_at = datetime.now() + timedelta(minutes=RESERVATION_TIMEOUT_MIN)
        reservation = Reservation(
            user_id=user_id,
            service_id=service_id,
            number_id=available_number.id,
            status=ReservationStatus.WAITING_CODE,
            expired_at=expires_at
        )
        
        # Update number status
        available_number.status = 'RESERVED'
        available_number.reserved_by_user_id = user_id
        available_number.reserved_at = datetime.now()
        available_number.expires_at = expires_at
        
        db.add(reservation)
        db.commit()
        db.refresh(reservation)
        
        return reservation
    finally:
        db.close()

async def complete_reservation_atomic(reservation_id: int, code: str) -> bool:
    """Complete reservation atomically with proper transaction handling"""
    db = get_db()
    try:
        # Begin transaction
        
        # Lock the reservation for update
        reservation = db.query(Reservation).filter(
            Reservation.id == reservation_id
        ).with_for_update().first()
        
        if not reservation or reservation.status != ReservationStatus.WAITING_CODE:
            db.rollback()
            return False
        
        # Lock related records
        user = db.query(User).filter(
            User.id == reservation.user_id
        ).with_for_update().first()
        
        service = db.query(Service).filter(
            Service.id == reservation.service_id
        ).first()
        
        number = db.query(Number).filter(
            Number.id == reservation.number_id
        ).with_for_update().first()
        
        if not user or not service or not number:
            db.rollback()
            return False
        
        # Calculate price
        price = float(number.price_override or service.default_price)
        
        # Check if user has enough balance
        if float(user.balance or 0) < float(price):
            # Mark reservation as failed due to insufficient balance
            reservation.status = ReservationStatus.EXPIRED
            db.commit()
            
            await bot.send_message(
                str(user.telegram_id),
                f"❌ رصيدك غير كافي!\nالسعر المطلوب: {price}\nرصيدك الحالي: {user.balance}"
            )
            return False
        
        # Complete the transaction atomically
        user.balance = float(user.balance or 0) - price
        reservation.status = ReservationStatus.COMPLETED
        reservation.code_value = code
        reservation.completed_at = datetime.now()
        number.status = 'USED'
        number.code_received_at = datetime.now()
        
        # Increment usage count
        number.usage_count = (number.usage_count or 0) + 1
        
        # Count unique users who have completed reservations for this number
        unique_users_count = db.query(Reservation.user_id).filter(
            Reservation.number_id == number.id,
            Reservation.status == ReservationStatus.COMPLETED
        ).distinct().count()
        
        # If number has been used by 3 different users, mark it as deleted
        if unique_users_count >= 3:
            number.status = 'DELETED'
            logger.info(f"Number {number.phone_number} marked as deleted after being used by {unique_users_count} different users")
        
        # Create transaction record
        transaction = Transaction(
            user_id=user.id,
            type=TransactionType.PURCHASE,
            amount=price,
            reason=f"{service.name} {number.phone_number}"
        )
        db.add(transaction)
        
        # Check if this was the last available number for this country/service before committing
        remaining_numbers = db.query(Number).filter(
            Number.service_id == reservation.service_id,
            Number.country_code == number.country_code,
            Number.status == 'AVAILABLE',
            Number.id != number.id  # Exclude the current number being used
        ).count()
        
        # Commit all changes
        db.commit()
        
        # Format message with new style
        sms_formatted = format_sms_message(str(number.phone_number), code)
        
        # Notify user
        await bot.send_message(
            str(user.telegram_id),
            f"🎉 وصل الكود!\n\n"
            f"```\n{sms_formatted}\n```\n\n"
            f"تم خصم {price} من رصيدك\n"
            f"رصيدك الحالي: {user.balance}",
            parse_mode="Markdown"
        )
        
        # Check if we need to notify admin about empty stock
        if remaining_numbers == 0:
            # Get country name for notification
            country_name, _ = get_country_name_and_flag(str(number.country_code))
            await notify_admin_low_stock(int(reservation.service_id), str(number.country_code), country_name)
        
        return True
        
    except Exception as e:
        logger.error(f"Error completing reservation atomically: {e}")
        db.rollback()
        return False
    finally:
        db.close()

async def poll_provider_messages():
    """Poll provider APIs for new messages"""
    while True:
        try:
            db = get_db()
            try:
                # Get active providers
                providers = db.query(Provider).filter(
                    Provider.active == True,
                    Provider.mode == ProviderMode.POLL
                ).all()
                
                for provider in providers:
                    try:
                        await process_provider_messages(provider)
                    except Exception as e:
                        logger.error(f"Error processing provider {provider.name}: {e}")
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error in polling loop: {e}")
        
        await asyncio.sleep(min(POLL_INTERVAL_SEC, 30))

async def process_provider_messages(provider: Provider):
    """Process messages from a specific provider"""
    try:
        timeout = aiohttp.ClientTimeout(total=PROVIDER_API_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {"Authorization": f"Bearer {provider.api_key}"}
            async with session.get(f"{provider.base_url}/messages", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    messages = data.get('messages', [])
                    
                    for msg in messages:
                        await process_single_message(provider, msg)
                        
    except Exception as e:
        logger.error(f"Error fetching messages from {provider.name}: {e}")

async def process_single_message(provider: Provider, message: Dict[str, Any]):
    """Process a single message from provider"""
    db = get_db()
    try:
        to_number = normalize_phone_number(message.get('to', ''))
        text = message.get('text', '')
        service_name = message.get('service', '')
        
        # Try to infer service from text if not provided
        if not service_name:
            # Basic service inference (can be improved)
            if 'whatsapp' in text.lower():
                service_name = 'WhatsApp'
            elif 'telegram' in text.lower():
                service_name = 'Telegram'
            # Add more service inference logic here
        
        # Extract code
        code = await extract_code_from_message(text, service_name)
        if not code:
            return
        
        # Find matching reservation
        service = db.query(Service).filter(Service.name == service_name).first()
        if not service:
            return
        
        number = db.query(Number).filter(
            Number.phone_number == to_number,
            Number.service_id == service.id,
            Number.status == 'RESERVED'
        ).first()
        
        if not number:
            return
        
        reservation = db.query(Reservation).filter(
            Reservation.number_id == number.id,
            Reservation.status == ReservationStatus.WAITING_CODE
        ).first()
        
        if not reservation:
            return
        
        # Store message
        provider_msg = ProviderMessage(
            provider_id=provider.id,
            raw_payload=json.dumps(message)
        )
        db.add(provider_msg)
        db.commit()
        
        # Complete reservation
        await complete_reservation_atomic(reservation.id, code)
        
    finally:
        db.close()

async def check_expired_reservations():
    """Check and expire old reservations"""
    while True:
        try:
            db = get_db()
            try:
                now = datetime.now()
                expired_reservations = db.query(Reservation).filter(
                    Reservation.status == ReservationStatus.WAITING_CODE,
                    Reservation.expired_at < now
                ).all()
                
                for reservation in expired_reservations:
                    # Mark as expired
                    reservation.status = ReservationStatus.EXPIRED
                    
                    # Delete number that was reserved but didn't receive codes
                    number = db.query(Number).filter(Number.id == reservation.number_id).first()
                    if number:
                        # Check if number received any codes during reservation
                        if number.code_received_at is None:
                            # Delete the number if no code was received
                            number.status = 'DELETED'
                            logger.info(f"Number {number.phone_number} deleted after reservation expired without receiving code")
                        else:
                            # Return to available if it did receive a code but wasn't completed
                            number.status = 'AVAILABLE'
                            
                        number.reserved_by_user_id = None
                        number.reserved_at = None
                        number.expires_at = None
                    
                    # Notify user
                    user = db.query(User).filter(User.id == reservation.user_id).first()
                    if user:
                        keyboard = InlineKeyboardBuilder()
                        keyboard.row(InlineKeyboardButton(text="🔄 احجز رقم جديد", callback_data="main_menu"))
                        
                        try:
                            await bot.send_message(
                                user.telegram_id,
                                "⏰ انتهت مهلة انتظار الكود\n"
                                "لم يتم خصم أي رسوم من رصيدك\n"
                                "يمكنك حجز رقم جديد",
                                reply_markup=keyboard.as_markup()
                            )
                        except Exception:
                            # Silently ignore user notification errors
                            pass
                
                db.commit()
                
            finally:
                db.close()
        
        except Exception:
            # Reduced logging to prevent spam
            pass
        
        await asyncio.sleep(30)  # Check every 30 seconds

async def check_user_subscriptions_periodically():
    """Check all users' subscriptions periodically and block those who left"""
    while True:
        try:
            db = get_db()
            try:
                # Get all active forced subscriptions
                forced_subs = db.query(ForcedSubscription).filter(
                    ForcedSubscription.active == True
                ).all()
                
                if not forced_subs:
                    await asyncio.sleep(120)  # Check every 2 minutes if no forced subs
                    continue
                
                # Get all users who are not admins
                users = db.query(User).filter(
                    User.telegram_id != str(ADMIN_ID),
                    User.is_banned == False
                ).all()
                
                for user in users:
                    try:
                        user_id = int(user.telegram_id)
                        is_subscribed = True
                        
                        for sub in forced_subs:
                            try:
                                member = await bot.get_chat_member(sub.channel_id, user_id)
                                if member.status in ['left', 'kicked']:
                                    is_subscribed = False
                                    break
                            except Exception as e:
                                logger.error(f"Error checking subscription for user {user_id} in channel {sub.channel_id}: {e}")
                                continue
                        
                        # If user left required channels, send warning
                        if not is_subscribed:
                            try:
                                subscription_keyboard = await create_subscription_keyboard()
                                await bot.send_message(
                                    user_id,
                                    "⚠️ تم اكتشاف خروجك من إحدى القنوات الإجبارية!\n\n"
                                    "🔒 يجب الاشتراك في جميع القنوات التالية لمواصلة استخدام البوت:\n\n"
                                    "👇 اضغط على الأزرار للاشتراك مرة أخرى:",
                                    reply_markup=subscription_keyboard
                                )
                            except Exception as e:
                                logger.error(f"Error sending subscription warning to user {user_id}: {e}")
                        
                    except Exception as e:
                        logger.error(f"Error processing user {user.telegram_id}: {e}")
                        continue
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error in subscription check task: {e}")
        
        await asyncio.sleep(900)  # Check every 15 minutes

# Subscription and user data channel functions
async def check_user_subscription(user_id: int) -> bool:
    """Check if user is subscribed to all required channels"""
    db = get_db()
    try:
        # Get all active forced subscriptions
        forced_subs = db.query(ForcedSubscription).filter(
            ForcedSubscription.active == True
        ).all()
        
        if not forced_subs:
            return True  # No forced subscriptions
        
        for sub in forced_subs:
            try:
                # Check if user is member of the channel
                member = await bot.get_chat_member(sub.channel_id, user_id)
                if member.status in ['left', 'kicked']:
                    return False
            except Exception as e:
                logger.error(f"Error checking subscription for channel {sub.channel_id}: {e}")
                return False
        
        return True
    finally:
        db.close()

async def send_user_data_to_channel(user: User, reservation: Optional[Reservation] = None):
    """Send user data to the configured user data channel"""
    db = get_db()
    try:
        # Get active user data channel
        channel = db.query(UserDataChannel).filter(
            UserDataChannel.active == True
        ).first()
        
        if not channel:
            return
        
        # Prepare user info
        user_info = f"👤 **معلومات مستخدم جديد**\n\n"
        user_info += f"🆔 **المعرف:** `{user.telegram_id}`\n"
        user_info += f"👤 **الاسم:** {user.first_name or 'غير محدد'}"
        if user.last_name:
            user_info += f" {user.last_name}"
        user_info += "\n"
        
        if user.username:
            user_info += f"📝 **اليوزر:** @{user.username}\n"
        
        user_info += f"💰 **الرصيد:** {user.balance}\n"
        user_info += f"📅 **تاريخ الانضمام:** {user.joined_at.strftime('%Y-%m-%d %H:%M')}\n"
        
        if reservation:
            service = db.query(Service).filter(Service.id == reservation.service_id).first()
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if service and number:
                user_info += f"\n📱 **آخر رقم:** {number.phone_number}\n"
                user_info += f"🏷 **الخدمة:** {service.emoji} {service.name}\n"
        
        # Send to channel
        await bot.send_message(
            channel.channel_id,
            user_info,
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error sending user data to channel: {e}")
    finally:
        db.close()

async def send_all_users_data_periodically():
    """Send all users data to channel periodically"""
    while True:
        try:
            db = get_db()
            try:
                # Get active user data channel
                channel = db.query(UserDataChannel).filter(
                    UserDataChannel.active == True
                ).first()
                
                if not channel:
                    logger.info("No active user data channel configured")
                    await asyncio.sleep(1800)  # Check every 30 minutes if no channel
                    continue
                
                # Get all users
                users = db.query(User).all()
                
                if not users:
                    await asyncio.sleep(1800)  # Check every 30 minutes if no users
                    continue
                
                # Prepare comprehensive report
                report = f"📊 **تقرير شامل لجميع المستخدمين**\n"
                report += f"📅 **التاريخ:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                report += f"👥 **إجمالي المستخدمين:** {len(users)}\n\n"
                
                # Calculate statistics
                total_balance = sum(user.balance for user in users)
                active_users = [u for u in users if not u.is_banned]
                banned_users = [u for u in users if u.is_banned]
                
                report += f"💰 **إجمالي الأرصدة:** {total_balance:.2f}\n"
                report += f"✅ **المستخدمين النشطين:** {len(active_users)}\n"
                report += f"🚫 **المستخدمين المحظورين:** {len(banned_users)}\n\n"
                
                # Get reservations data
                active_reservations = db.query(Reservation).filter(
                    Reservation.status == ReservationStatus.WAITING_CODE
                ).count()
                completed_reservations = db.query(Reservation).filter(
                    Reservation.status == ReservationStatus.COMPLETED
                ).count()
                
                report += f"📱 **الحجوزات النشطة:** {active_reservations}\n"
                report += f"✅ **الحجوزات المكتملة:** {completed_reservations}\n\n"
                
                # Send summary first
                await bot.send_message(
                    channel.channel_id,
                    report,
                    parse_mode="Markdown"
                )
                
                # Skip sending individual user data to prevent flood control
                user_count = len(users)
                logger.info(f"Report sent successfully. Total users: {user_count}")
                
                # Individual user data sending disabled to prevent flood control
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error in periodic user data sending: {e}")
        
        # Wait 24 hours before next send to reduce frequency
        await asyncio.sleep(86400)

async def create_subscription_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard with subscription channels"""
    db = get_db()
    keyboard = InlineKeyboardBuilder()
    
    try:
        forced_subs = db.query(ForcedSubscription).filter(
            ForcedSubscription.active == True
        ).all()
        
        for sub in forced_subs:
            if sub.channel_username:
                keyboard.row(InlineKeyboardButton(
                    text=f"📢 {sub.channel_title or 'اشترك في القناة'}",
                    url=f"https://t.me/{sub.channel_username}"
                ))
        
        keyboard.row(InlineKeyboardButton(
            text="✅ تم الاشتراك",
            callback_data="check_subscription"
        ))
        
        return keyboard.as_markup()
    finally:
        db.close()

# Admin handlers for service group management
@dp.callback_query(F.data == "admin_add_service")
async def admin_add_service_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding new service"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_service_name)
    await callback.message.edit_text(
        "📝 إضافة خدمة جديدة\n\n"
        "أدخل اسم الخدمة (مثل: WhatsApp, Telegram, Instagram):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_services")
        ]])
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_name))
async def process_service_name(message: types.Message, state: FSMContext):
    """Process service name input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    service_name = message.text.strip()
    if not service_name:
        await message.reply("❌ يرجى إدخال اسم صحيح للخدمة")
        return
    
    await state.update_data(service_name=service_name)
    await state.set_state(AdminStates.waiting_for_service_emoji)
    await message.reply(
        f"✅ اسم الخدمة: {service_name}\n\n"
        "أدخل الإيموجي للخدمة (مثل: 📱, 💬, 📸):"
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_emoji))
async def process_service_emoji(message: types.Message, state: FSMContext):
    """Process service emoji input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    emoji = message.text.strip()
    if not emoji:
        emoji = "📱"  # Default emoji
    
    await state.update_data(service_emoji=emoji)
    await state.set_state(AdminStates.waiting_for_service_price)
    await message.reply(
        f"✅ الإيموجي: {emoji}\n\n"
        "أدخل السعر الافتراضي للخدمة (بالوحدات):"
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_price))
async def process_service_price(message: types.Message, state: FSMContext):
    """Process service price input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    try:
        price = float(message.text.strip())
        if price < 0:
            await message.reply("❌ السعر يجب أن يكون رقم موجب")
            return
    except ValueError:
        await message.reply("❌ يرجى إدخال رقم صحيح للسعر")
        return
    
    await state.update_data(service_price=price)
    await state.set_state(AdminStates.waiting_for_service_description)
    await message.reply(
        f"✅ السعر: {price} وحدة\n\n"
        "أدخل وصف الخدمة (اختياري - أرسل 'تخطي' للتخطي):"
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_description))
async def process_service_description(message: types.Message, state: FSMContext):
    """Process service description input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    description = message.text.strip() if message.text.strip().lower() != 'تخطي' else None
    
    await state.update_data(service_description=description)
    await state.set_state(AdminStates.waiting_for_service_regex)
    await message.reply(
        "📝 أدخل نمط Regex لاستخراج الكود من الرسائل\n\n"
        "أمثلة:\n"
        "• للأكواد من 4-6 أرقام: \\b\\d{4,6}\\b\n"
        "• للأكواد من 5 أرقام فقط: \\b\\d{5}\\b\n"
        "• أرسل 'افتراضي' لاستخدام النمط الافتراضي:"
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_regex))
async def process_service_regex(message: types.Message, state: FSMContext):
    """Process service regex pattern input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    regex_pattern = message.text.strip()
    if regex_pattern.lower() == 'افتراضي' or not regex_pattern:
        regex_pattern = r'\\b\\d{4,6}\\b'
    
    # Test regex pattern
    try:
        re.compile(regex_pattern)
    except re.error:
        await message.reply("❌ نمط Regex غير صحيح، يرجى المحاولة مرة أخرى")
        return
    
    await state.update_data(service_regex=regex_pattern)
    await state.set_state(AdminStates.waiting_for_service_group_id)
    await message.reply(
        f"✅ نمط Regex: {regex_pattern}\n\n"
        "📞 أدخل Group ID للجروب/القناة التي ستستقبل الرسائل\n\n"
        "مثال: -1001234567890\n"
        "💡 لمعرفة Group ID، أضف البوت للجروب واستخدم الأمر /chatinfo"
    )

@dp.message(StateFilter(AdminStates.waiting_for_service_group_id))
async def process_service_group_id(message: types.Message, state: FSMContext):
    """Process service group ID input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    group_id = message.text.strip()
    
    # Validate group ID format
    try:
        int(group_id)
    except ValueError:
        await message.reply("❌ Group ID يجب أن يكون رقم صحيح")
        return
    
    await state.update_data(service_group_id=group_id)
    
    # Skip security system completely and create service directly
    data = await state.get_data()
    
    db = get_db()
    try:
        # Check if service with same name already exists
        existing_service = db.query(Service).filter(Service.name == data['service_name']).first()
        
        if existing_service:
            if existing_service.active:
                await message.reply(
                    f"❌ خدمة باسم '{data['service_name']}' موجودة بالفعل ونشطة\n\n"
                    "يرجى اختيار اسم آخر للخدمة."
                )
                await state.clear()
                return
            else:
                # Service exists but is inactive - offer to reactivate
                await message.reply(
                    f"⚠️ خدمة باسم '{data['service_name']}' موجودة لكنها معطلة\n\n"
                    "هل تريد تفعيلها مرة أخرى؟\n"
                    "أم اختيار اسم جديد؟",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ تفعيل الخدمة الموجودة", callback_data=f"reactivate_service_{existing_service.id}")],
                        [InlineKeyboardButton(text="🔄 اختيار اسم جديد", callback_data="admin_create_service")],
                        [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_services")]
                    ])
                )
                await state.clear()
                return
        
        # Create new service (no conflicts)
        service = Service(
            name=data['service_name'],
            emoji=data['service_emoji'],
            description=data.get('service_description'),
            default_price=data['service_price'],
            active=True
        )
        db.add(service)
        db.flush()  # Get service ID
        
        # Create service group mapping without security
        service_group = ServiceGroup(
            service_id=service.id,
            group_chat_id=group_id,
            secret_token=None,
            regex_pattern=data['service_regex'],
            security_mode=SecurityMode.TOKEN_ONLY,  # Default mode
            active=True
        )
        db.add(service_group)
        db.commit()
        
        await state.clear()
        await message.reply(
            f"✅ تم إنشاء الخدمة بنجاح!\n\n"
            f"🏷 اسم الخدمة: {service.emoji} {service.name}\n"
            f"💰 السعر: {service.default_price} وحدة\n"
            f"📞 مربوطة بالجروب: {group_id}\n"
            f"🔍 نمط البحث: {data['service_regex']}"
        )
        
    except Exception as e:
        logger.error(f"Error creating service: {e}")
        await message.reply("❌ حدث خطأ في إنشاء الخدمة")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data.startswith("reactivate_service_"))
async def reactivate_service_handler(callback: CallbackQuery):
    """Reactivate an inactive service"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[2])
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        
        if not service:
            await callback.answer("❌ لم يتم العثور على الخدمة")
            return
        
        # Reactivate the service
        service.active = True
        db.commit()
        
        await callback.message.edit_text(
            f"✅ تم تفعيل الخدمة بنجاح!\n\n"
            f"🏷 اسم الخدمة: {service.emoji} {service.name}\n"
            f"💰 السعر: {service.default_price} وحدة\n"
            f"📊 الحالة: نشطة",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 إدارة الخدمات", callback_data="admin_services")]
            ])
        )
        
    except Exception as e:
        logger.error(f"Error reactivating service: {e}")
        await callback.answer("❌ حدث خطأ في تفعيل الخدمة")
        db.rollback()
    finally:
        db.close()

# Security system removed - services are created directly without security setup

@dp.callback_query(F.data.startswith("test_group_"))
async def test_group_handler(callback: CallbackQuery):
    """Test group connectivity"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[2])
    
    db = get_db()
    try:
        service_group = db.query(ServiceGroup).filter(
            ServiceGroup.service_id == service_id
        ).first()
        
        if not service_group:
            await callback.answer("❌ لم يتم العثور على الجروب")
            return
        
        try:
            # Try to get chat info
            chat = await bot.get_chat(str(service_group.group_chat_id))
            
            # Try to get bot member status
            bot_member = await bot.get_chat_member(str(service_group.group_chat_id), bot.id)
            
            status_text = {
                'creator': '👑 المؤسس',
                'administrator': '👮‍♂️ مشرف',
                'member': '👤 عضو',
                'restricted': '🚫 مقيد',
                'left': '❌ غير موجود',
                'kicked': '🚫 محظور'
            }
            
            await callback.message.edit_text(
                f"🔍 نتائج اختبار الجروب\n\n"
                f"📞 Group ID: {service_group.group_chat_id}\n"
                f"📝 اسم الجروب: {chat.title or 'غير محدد'}\n"
                f"👥 نوع الجروب: {chat.type}\n"
                f"🤖 حالة البوت: {status_text.get(bot_member.status, bot_member.status)}\n\n"
                "✅ الاتصال بالجروب ناجح!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إدارة الخدمات", callback_data="admin_services")
                ]])
            )
            
        except Exception as e:
            await callback.message.edit_text(
                f"❌ فشل في الاتصال بالجروب\n\n"
                f"📞 Group ID: {service_group.group_chat_id}\n"
                f"❗ الخطأ: {str(e)}\n\n"
                "تأكد من:\n"
                "• البوت عضو في الجروب\n"
                "• Group ID صحيح\n"
                "• البوت لديه صلاحيات قراءة الرسائل",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إدارة الخدمات", callback_data="admin_services")
                ]])
            )
    finally:
        db.close()

# Command to get chat info (helpful for admins)
@dp.message(Command("chatinfo"))
async def chatinfo_handler(message: types.Message):
    """Get chat information"""
    if not message.chat:
        return
    
    if message.chat.type == 'private':
        await message.reply("هذا الأمر يعمل فقط في الجروبات والقنوات")
        return
    
    # Check if user is admin
    try:
        chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in ['creator', 'administrator']:
            await message.reply("هذا الأمر متاح للمشرفين فقط")
            return
    except:
        await message.reply("لا يمكن التحقق من صلاحياتك")
        return
    
    chat_info = (
        f"📊 معلومات الدردشة\n\n"
        f"🆔 Chat ID: `{message.chat.id}`\n"
        f"📝 الاسم: {message.chat.title or 'غير محدد'}\n"
        f"👥 النوع: {message.chat.type}\n"
        f"👤 اليوزر: @{message.chat.username or 'غير محدد'}"
    )
    
    await message.reply(chat_info, parse_mode="Markdown")

@dp.callback_query(F.data == "admin_service_groups")
async def admin_service_groups_handler(callback: CallbackQuery):
    """Handle service groups management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        service_groups = db.query(ServiceGroup).join(Service).all()
        
        text = "🔗 إدارة ربط الخدمات بالجروبات\n\n"
        
        if service_groups:
            text += "الروابط الحالية:\n"
            for sg in service_groups:
                status = "✅" if sg.active else "❌"
                security_icon = {
                    SecurityMode.TOKEN_ONLY: "🔑",
                    SecurityMode.ADMIN_ONLY: "👑",
                    SecurityMode.HMAC: "🔐"
                }.get(sg.security_mode, "🔑")
                
                text += f"{status} {sg.service.emoji} {sg.service.name}\n"
                text += f"   📞 {sg.group_chat_id} {security_icon}\n\n"
        else:
            text += "لا توجد روابط محددة\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for sg in service_groups:
            status = "✅" if sg.active else "❌"
            security_icon = {
                SecurityMode.TOKEN_ONLY: "🔑",
                SecurityMode.ADMIN_ONLY: "👑", 
                SecurityMode.HMAC: "🔐"
            }.get(sg.security_mode, "🔑")
            
            # Check if bot is admin in the group
            bot_status = await verify_bot_in_group(sg.group_chat_id)
            bot_icon = "🤖✅" if bot_status else "🤖❌"
            
            keyboard.row(InlineKeyboardButton(
                text=f"{status} {sg.service.emoji} {sg.service.name} - {sg.group_chat_id} {security_icon} {bot_icon}",
                callback_data=f"edit_service_group_{sg.id}"
            ))
        
        keyboard.row(
            InlineKeyboardButton(text="➕ ربط خدمة بجروب", callback_data="admin_add_service"),
            InlineKeyboardButton(text="📊 إحصائيات الرسائل", callback_data="admin_messages_stats")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_messages_stats")
async def admin_messages_stats_handler(callback: CallbackQuery):
    """Handle messages statistics"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get message statistics
        total_messages = db.query(ProviderMessage).count()
        processed_messages = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.PROCESSED
        ).count()
        rejected_messages = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.REJECTED
        ).count()
        orphan_messages = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.ORPHAN
        ).count()
        blocked_messages = db.query(BlockedMessage).count()
        
        # Get recent completed reservations
        recent_completions = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.COMPLETED
        ).order_by(Reservation.completed_at.desc()).limit(5).all()
        
        text = f"📊 إحصائيات الرسائل\n\n"
        text += f"📬 إجمالي الرسائل: {total_messages}\n"
        text += f"✅ معالجة: {processed_messages}\n"
        text += f"❌ مرفوضة: {rejected_messages}\n"
        text += f"🔶 يتيمة: {orphan_messages}\n"
        text += f"🚫 محظورة: {blocked_messages}\n\n"
        
        if recent_completions:
            text += "🎉 آخر الإنجازات:\n"
            for res in recent_completions:
                service = db.query(Service).filter(Service.id == res.service_id).first()
                number = db.query(Number).filter(Number.id == res.number_id).first()
                if service and number:
                    text += f"• {service.emoji} {service.name} - {number.phone_number}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="🗑️ تنظيف الرسائل القديمة", callback_data="admin_cleanup_messages"),
            InlineKeyboardButton(text="🔄 تحديث", callback_data="admin_messages_stats")
        )
        keyboard.row(
            InlineKeyboardButton(text="🧹 مسح كل رسائل الجروب", callback_data="admin_cleanup_all_group_messages"),
            InlineKeyboardButton(text="🚫 مسح الرسائل المحظورة", callback_data="admin_cleanup_blocked_messages")
        )
        keyboard.row(
            InlineKeyboardButton(text="🔍 معالجة الرسائل اليتيمة", callback_data="admin_process_orphan_messages")
        )
        keyboard.row(
            InlineKeyboardButton(text="🗑️ حذف جميع رسائل الجروبات", callback_data="admin_delete_all_telegram_messages")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الخدمات", callback_data="admin_services"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_cleanup_messages")
async def admin_cleanup_messages_handler(callback: CallbackQuery):
    """Cleanup old messages"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Delete messages older than 7 days
        cutoff_date = datetime.now() - timedelta(days=7)
        
        deleted_provider = db.query(ProviderMessage).filter(
            ProviderMessage.received_at < cutoff_date
        ).delete()
        
        deleted_blocked = db.query(BlockedMessage).filter(
            BlockedMessage.created_at < cutoff_date
        ).delete()
        
        db.commit()
        
        await callback.answer(
            f"✅ تم حذف {deleted_provider + deleted_blocked} رسالة قديمة",
            show_alert=True
        )
        
        # Refresh the stats
        await admin_messages_stats_handler(callback)
        
    except Exception as e:
        logger.error(f"Error cleaning up messages: {e}")
        await callback.answer(f"❌ خطأ في التنظيف: {str(e)}")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_cleanup_all_group_messages")
async def admin_cleanup_all_group_messages_handler(callback: CallbackQuery):
    """Cleanup all group messages"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Delete all provider messages from groups
        deleted_provider = db.query(ProviderMessage).delete()
        
        # Delete all blocked messages from groups
        deleted_blocked = db.query(BlockedMessage).delete()
        
        db.commit()
        
        await callback.answer(
            f"✅ تم حذف {deleted_provider + deleted_blocked} رسالة من كل الجروبات",
            show_alert=True
        )
        
        # Refresh the stats
        await admin_messages_stats_handler(callback)
        
    except Exception as e:
        logger.error(f"Error cleaning up all group messages: {e}")
        await callback.answer(f"❌ خطأ في التنظيف: {str(e)}")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_delete_all_telegram_messages")
async def admin_delete_all_telegram_messages_handler(callback: CallbackQuery):
    """Delete all messages from groups where bot is admin"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await callback.answer("🔄 جاري البحث عن الجروبات وحذف الرسائل...")
    
    db = get_db()
    try:
        # Get all service groups where bot should be admin
        service_groups = db.query(ServiceGroup).filter(ServiceGroup.active == True).all()
        
        total_deleted = 0
        successful_groups = 0
        failed_groups = 0
        
        for sg in service_groups:
            try:
                group_chat_id = str(sg.group_chat_id)
                
                # Verify bot is admin in this group
                if await verify_bot_in_group(group_chat_id):
                    # Get chat info to determine how to delete messages
                    chat = await bot.get_chat(group_chat_id)
                    
                    # Delete ALL messages using Telegram Bot API
                    try:
                        deleted_count = 0
                        
                        # Send notification
                        notification_msg = await bot.send_message(group_chat_id, "🗑️ جاري حذف جميع الرسائل من الجروب...")
                        
                        # Strategy 1: Delete stored message IDs from database
                        provider_messages = db.query(ProviderMessage).filter(
                            ProviderMessage.group_chat_id == group_chat_id
                        ).all()
                        
                        # Try to delete stored messages first
                        for msg in provider_messages:
                            try:
                                if msg.raw_payload:
                                    payload = json.loads(msg.raw_payload)
                                    message_id = payload.get('message_id')
                                    if message_id:
                                        await bot.delete_message(group_chat_id, message_id)
                                        deleted_count += 1
                            except:
                                pass
                        
                        # Strategy 2: Comprehensive message deletion by range
                        try:
                            # Send a test message to get current message ID  
                            test_msg = await bot.send_message(group_chat_id, "📍")
                            current_msg_id = test_msg.message_id
                            await bot.delete_message(group_chat_id, current_msg_id)
                            
                            # Collect message IDs to delete in batches
                            message_ids_to_delete = []
                            
                            # Work backwards from current message ID
                            # Try to delete up to 2000 recent messages (within 48h limit)
                            for i in range(min(2000, current_msg_id)):
                                msg_id_to_delete = current_msg_id - i - 1
                                if msg_id_to_delete > 0:
                                    message_ids_to_delete.append(msg_id_to_delete)
                            
                            # Use batch deletion for efficiency (100 messages at a time)
                            for i in range(0, len(message_ids_to_delete), 100):
                                chunk = message_ids_to_delete[i:i+100]
                                try:
                                    await bot.delete_messages(group_chat_id, chunk)
                                    deleted_count += len(chunk)
                                    logger.info(f"Deleted batch of {len(chunk)} messages from group {group_chat_id}")
                                    # Small delay to avoid rate limiting
                                    await asyncio.sleep(0.2)
                                except Exception as batch_err:
                                    # Fallback to individual deletion for this batch
                                    logger.info(f"Batch failed, trying individual deletion: {batch_err}")
                                    for msg_id in chunk:
                                        try:
                                            await bot.delete_message(group_chat_id, msg_id)
                                            deleted_count += 1
                                        except Exception:
                                            # Message doesn't exist, too old, or no permission
                                            pass
                                        
                        except Exception as range_error:
                            logger.error(f"Range deletion failed for group {group_chat_id}: {range_error}")
                        
                        # Clean up database records after successful deletion
                        try:
                            # Remove the deleted messages from database
                            db.query(ProviderMessage).filter(
                                ProviderMessage.group_chat_id == group_chat_id
                            ).delete()
                            
                            # Also clean blocked messages for this group  
                            db.query(BlockedMessage).filter(
                                BlockedMessage.group_chat_id == group_chat_id
                            ).delete()
                            
                            db.commit()
                            logger.info(f"Cleaned database records for group {group_chat_id}")
                        except Exception as db_error:
                            logger.error(f"Error cleaning database for group {group_chat_id}: {db_error}")
                            db.rollback()
                        
                        # Delete our notification message
                        try:
                            await bot.delete_message(group_chat_id, notification_msg.message_id)
                        except:
                            pass
                        
                        # Send final notification
                        if deleted_count > 0:
                            final_msg = await bot.send_message(group_chat_id, f"✅ تم حذف {deleted_count} رسالة من الجروب")
                            await asyncio.sleep(3)  # Wait so admin can see the result
                            try:
                                await bot.delete_message(group_chat_id, final_msg.message_id)
                            except:
                                pass
                        
                        total_deleted += deleted_count
                        successful_groups += 1
                        
                    except Exception as delete_error:
                        logger.error(f"Error deleting messages in group {group_chat_id}: {delete_error}")
                        failed_groups += 1
                        
                else:
                    logger.info(f"Bot is not admin in group {group_chat_id}, skipping")
                    failed_groups += 1
                    
            except Exception as e:
                logger.error(f"Error processing group {sg.group_chat_id}: {e}")
                failed_groups += 1
        
        # Send final report
        if total_deleted > 0:
            await callback.message.edit_text(
                f"✅ تم تنظيف رسائل الجروبات\n\n"
                f"🎯 جروبات ناجحة: {successful_groups}\n"
                f"❌ جروبات فاشلة: {failed_groups}\n\n"
                f"ملاحظة: تم تنظيف الرسائل في الجروبات التي يملك البوت فيها صلاحيات الإدارة.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إحصائيات الرسائل", callback_data="admin_messages_stats")
                ]])
            )
        else:
            await callback.message.edit_text(
                f"⚠️ لم يتم حذف أي رسائل\n\n"
                f"❌ جروبات فاشلة: {failed_groups}\n\n"
                f"السبب: البوت غير مشرف في الجروبات أو لا توجد جروبات مفعلة.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إحصائيات الرسائل", callback_data="admin_messages_stats")
                ]])
            )
    
    except Exception as e:
        logger.error(f"Error in delete all telegram messages: {e}")
        await callback.answer(f"❌ خطأ عام: {str(e)}")
    finally:
        db.close()

# Handler to cleanup blocked messages (no number/code recognition)
# ==== AUTO CLEANUP ADMIN HANDLERS ====

@dp.callback_query(F.data == "admin_auto_cleanup")
async def admin_auto_cleanup_handler(callback: CallbackQuery):
    """Handle auto cleanup management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    status_text = "✅ مفعل" if auto_cleanup_enabled else "❌ معطل"
    
    text = f"🗑️ إدارة التنظيف التلقائي\n\n"
    text += f"🔄 الحالة: {status_text}\n"
    text += f"⏰ فترة التنظيف: كل {cleanup_interval_hours} ساعة\n"
    text += f"📅 الاحتفاظ بالرسائل: {message_retention_days} أيام\n"
    text += f"📦 الرسائل اليتيمة: {orphan_message_retention_hours} ساعة\n\n"
    text += "📋 هذا النظام ينظف تلقائياً:\n"
    text += "• الرسائل القديمة\n"
    text += "• الرسائل اليتيمة\n"
    text += "• الحجوزات المنتهية الصلاحية\n"
    text += "• الرسائل المحظورة"
    
    keyboard = InlineKeyboardBuilder()
    
    if auto_cleanup_enabled:
        keyboard.row(InlineKeyboardButton(text="⏸️ إيقاف التنظيف التلقائي", callback_data="admin_stop_cleanup"))
    else:
        keyboard.row(InlineKeyboardButton(text="▶️ تشغيل التنظيف التلقائي", callback_data="admin_start_cleanup"))
    
    keyboard.row(
        InlineKeyboardButton(text="🗑️ تنظيف يدوي الآن", callback_data="admin_manual_cleanup_now"),
        InlineKeyboardButton(text="⚙️ إعدادات", callback_data="admin_cleanup_settings")
    )
    keyboard.row(InlineKeyboardButton(text="🔙 عودة", callback_data="admin_services"))
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup())

@dp.callback_query(F.data == "admin_start_cleanup")
async def admin_start_cleanup_handler(callback: CallbackQuery):
    """Start auto cleanup system"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    start_auto_cleanup()
    await callback.answer("✅ تم تشغيل نظام التنظيف التلقائي!", show_alert=True)
    await admin_auto_cleanup_handler(callback)

@dp.callback_query(F.data == "admin_stop_cleanup")
async def admin_stop_cleanup_handler(callback: CallbackQuery):
    """Stop auto cleanup system"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    stop_auto_cleanup()
    await callback.answer("⏸️ تم إيقاف نظام التنظيف التلقائي", show_alert=True)
    await admin_auto_cleanup_handler(callback)

@dp.callback_query(F.data == "admin_manual_cleanup_now")
async def admin_manual_cleanup_now_handler(callback: CallbackQuery):
    """Run manual cleanup immediately"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await callback.answer("🔄 جاري تنظيف الرسائل...")
    
    # Run cleanup in background
    def run_cleanup():
        cleanup_dead_messages()
        cleanup_expired_reservations()
    
    cleanup_thread = threading.Thread(target=run_cleanup)
    cleanup_thread.start()
    
    await callback.answer("✅ تم بدء التنظيف اليدوي!", show_alert=True)
    await admin_auto_cleanup_handler(callback)

@dp.callback_query(F.data == "admin_cleanup_settings")
async def admin_cleanup_settings_handler(callback: CallbackQuery):
    """Show cleanup settings"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    text = f"⚙️ إعدادات التنظيف التلقائي\n\n"
    text += f"⏰ فترة التنظيف: {cleanup_interval_hours} ساعة\n"
    text += f"📅 الاحتفاظ بالرسائل: {message_retention_days} أيام\n"
    text += f"📦 الرسائل اليتيمة: {orphan_message_retention_hours} ساعة\n\n"
    text += "📝 لتغيير هذه الإعدادات، يمكنك تعديل المتغيرات في الكود أو إعادة تشغيل البوت"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔙 عودة", callback_data="admin_auto_cleanup"))
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup())

@dp.callback_query(F.data == "admin_cleanup_blocked_messages")
async def admin_cleanup_blocked_messages_handler(callback: CallbackQuery):
    """Clear all blocked messages (unrecognized messages)"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Delete all blocked messages (no number/code recognized)
        deleted_blocked = db.query(BlockedMessage).filter(
            BlockedMessage.reason == "no_number_or_no_code"
        ).delete()
        
        # Also delete rejected provider messages
        deleted_rejected = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.REJECTED
        ).delete()
        
        db.commit()
        logger.info(f"Cleaned up blocked messages: {deleted_blocked} blocked, {deleted_rejected} rejected")
        
        await callback.answer(
            f"✅ تم حذف الرسائل المحظورة\n"
            f"🚫 محذوف: {deleted_blocked} رسالة محظورة\n"
            f"❌ محذوف: {deleted_rejected} رسالة مرفوضة",
            show_alert=True
        )
        
        # Refresh stats
        await admin_messages_stats_handler(callback)
        
    except Exception as e:
        logger.error(f"Error cleaning up blocked messages: {e}")
        await callback.answer(f"❌ خطأ في المسح: {str(e)}")
        db.rollback()
    finally:
        db.close()

# Handler to process orphan messages manually
@dp.callback_query(F.data == "admin_process_orphan_messages")
async def admin_process_orphan_messages_handler(callback: CallbackQuery):
    """Process orphan messages to find matches by last digits"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        processed_count = 0
        matched_count = 0
        
        # Get all orphan messages from last 2 hours
        orphan_messages = db.query(ProviderMessage).filter(
            ProviderMessage.status == MessageStatus.ORPHAN,
            ProviderMessage.received_at >= datetime.now() - timedelta(hours=2)
        ).order_by(ProviderMessage.received_at.desc()).limit(100).all()
        
        for msg in orphan_messages:
            processed_count += 1
            
            # Extract codes from message
            service_provider_map = db.query(ServiceProviderMap).filter(
                ServiceProviderMap.service_id == msg.service_id
            ).first()
            regex_pattern = str(service_provider_map.regex_pattern) if service_provider_map else r'\b\d{5,6}\b'
            codes = re.findall(regex_pattern, msg.message_text)
            
            if codes:
                # Extract potential last digits from message
                digit_patterns = re.findall(r'\d{3,}', msg.message_text)
                for digit_group in digit_patterns:
                    if len(digit_group) >= 3:
                        last_digits = digit_group[-3:]
                        # Use a separate session for reservation lookup
                        reservation_db = get_db()
                        try:
                            reservation = reservation_db.query(Reservation).filter(
                                Reservation.status == ReservationStatus.WAITING_CODE,
                                Reservation.service_id == msg.service_id
                            ).join(Number).filter(
                                Number.phone_number.endswith(last_digits)
                            ).first()
                            
                            if reservation:
                                # Complete the reservation with separate session
                                success = await complete_reservation_atomic(reservation.id, codes[0])
                                if success:
                                    matched_count += 1
                                    # Update message status
                                    msg.status = MessageStatus.PROCESSED
                                    logger.info(f"Matched orphan message with reservation using last digits {last_digits}")
                                    break
                        finally:
                            reservation_db.close()
        
        db.commit()
        
        await callback.answer(
            f"✅ تمت معالجة الرسائل اليتيمة\n"
            f"📋 معالجة: {processed_count} رسالة\n"
            f"🎯 مطابقة: {matched_count} حجز",
            show_alert=True
        )
        
        # Refresh stats
        await admin_messages_stats_handler(callback)
        
    except Exception as e:
        logger.error(f"Error processing orphan messages: {e}")
        await callback.answer(f"❌ خطأ في المعالجة: {str(e)}")
        db.rollback()
    finally:
        db.close()

# Group message processing functions
async def process_incoming_group_message(message: types.Message):
    """Process incoming message from a registered group"""
    if not message.chat or not message.from_user or not message.text:
        return
    
    group_chat_id = str(message.chat.id)
    sender_id = str(message.from_user.id)
    message_text = message.text
    
    db = get_db()
    try:
        # Find service group mapping
        service_group = db.query(ServiceGroup).filter(
            ServiceGroup.group_chat_id == group_chat_id,
            ServiceGroup.active == True
        ).first()
        
        if not service_group:
            logger.info(f"Message from unregistered group: {group_chat_id}")
            return  # Not a registered group
            
        logger.info(f"Processing message from group: {group_chat_id}, service_id: {service_group.service_id}, service: {service_group.service.name if service_group.service else 'Unknown'}")
        
        # Store incoming message for audit
        provider_msg = ProviderMessage(
            service_id=service_group.service_id,
            group_chat_id=group_chat_id,
            sender_id=sender_id,
            message_text=message_text,
            raw_payload=json.dumps({
                'message_id': message.message_id,
                'chat_title': message.chat.title,
                'sender_username': message.from_user.username,
                'date': message.date.isoformat() if message.date else None
            }),
            status=MessageStatus.PENDING
        )
        db.add(provider_msg)
        db.commit()
        
        # No security checks - process all messages directly
        
        # Extract number and code with improved pattern
        regex_pattern = str(service_group.regex_pattern) if service_group.regex_pattern else r'\b\d{4,6}\b'
        number, code = extract_number_and_code(message_text, regex_pattern)
        
        # If failed with service pattern, try common patterns
        if not number or not code:
            # Try common format: "to:+1234567890 code:123456"
            number, code = extract_number_and_code(message_text, r'\b\d{4,6}\b')
            
        # Enhanced code extraction - try multiple patterns
        if not code:
            # Extract any 4-6 digit number as potential code
            code_matches = re.findall(r'\b\d{4,6}\b', message_text)
            if code_matches:
                code = code_matches[-1]  # Take last match (more likely to be verification code)
                logger.info(f"Extracted fallback code: {code} from message: {message_text}")
            else:
                # Try 3-digit codes as fallback
                three_digit_matches = re.findall(r'\b\d{3}\b', message_text)
                if three_digit_matches:
                    code = three_digit_matches[-1]
                    logger.info(f"Extracted 3-digit fallback code: {code} from message: {message_text}")
                
        logger.info(f"Final extraction result - Number: {number}, Code: {code}")
        
        if not number or not code:
            # If no direct extraction, try to find by last digits in message
            if code:  # If we have a code but no number, try to find by last digits
                # Extract all possible last digits from message
                digit_patterns = re.findall(r'\d{3,}', message_text)
                for digit_group in digit_patterns:
                    if len(digit_group) >= 3:
                        last_digits = digit_group[-3:]  # Get last 3 digits
                        reservation = await find_reservation_by_last_digits(last_digits, service_group.service_id)
                        if reservation:
                            number_obj = db.query(Number).filter(Number.id == reservation.number_id).first()
                            if number_obj:
                                number = number_obj.phone_number
                                logger.info(f"Found reservation by last digits {last_digits} in message without full number")
                                break
            
            if not number or not code:
                # Store as blocked - no valid number or code found
                blocked_msg = BlockedMessage(
                    service_id=service_group.service_id,
                    group_chat_id=group_chat_id,
                    sender_id=sender_id,
                    message_text=message_text,
                    reason="no_number_or_no_code"
                )
                db.add(blocked_msg)
                
                provider_msg.status = MessageStatus.REJECTED
                db.commit()
                return
        
        # Find matching reservation with detailed logging
        logger.info(f"Searching for reservation: number={number}, service_id={service_group.service_id}")
        
        # Try to find the number in ANY active service for this group
        all_service_groups = db.query(ServiceGroup).filter(
            ServiceGroup.group_chat_id == group_chat_id,
            ServiceGroup.active == True
        ).all()
        
        number_obj = None
        matching_service_id = None
        
        for sg in all_service_groups:
            temp_number_obj = db.query(Number).filter(
                Number.phone_number == number,
                Number.service_id == sg.service_id
            ).first()
            if temp_number_obj:
                number_obj = temp_number_obj
                matching_service_id = sg.service_id
                logger.info(f"Found number {number} in service_id {sg.service_id}")
                break
        
        # Track if we found the reservation via masked number search
        reservation_from_masked_search = None
        
        if not number_obj:
            # Try to extract last 2-3 digits from masked number format
            extracted_last_digits = extract_last_three_digits_from_masked_number(message_text)
            
            if extracted_last_digits:
                logger.info(f"Extracted last digits '{extracted_last_digits}' from masked message, searching for matching reservations")
                
                # Search for reservations with matching last digits across all services in this group
                for sg in all_service_groups:
                    reservation = await find_reservation_by_last_digits(extracted_last_digits, sg.service_id)
                    if reservation:
                        reservation_from_masked_search = reservation
                        matching_service_id = sg.service_id
                        number_obj = db.query(Number).filter(Number.id == reservation.number_id).first()
                        if number_obj:
                            number = str(number_obj.phone_number)
                            logger.info(f"Found matching reservation by last {len(extracted_last_digits)} digits: reservation_id={reservation.id}, number={number}")
                            break
                
                if not reservation_from_masked_search:
                    logger.warning(f"No reservation found for last digits '{extracted_last_digits}' in group {group_chat_id}")
                    provider_msg.status = MessageStatus.ORPHAN
                    db.commit()
                    return
            else:
                logger.warning(f"Number {number} not found and could not extract last digits from message: {message_text}")
                provider_msg.status = MessageStatus.ORPHAN
                db.commit()
                return
        
        logger.info(f"Found number: id={number_obj.id}, status={number_obj.status}, reserved_by={number_obj.reserved_by_user_id}")
        
        # Check for reservation - use reservation_from_masked_search if it exists
        reservation = None
        if reservation_from_masked_search:
            reservation = reservation_from_masked_search
            logger.info(f"Using reservation found by last digits: id={reservation.id}")
        else:
            # Try to find reservation by exact number match
            reservation = db.query(Reservation).filter(
                Reservation.number_id == number_obj.id,
                Reservation.status == ReservationStatus.WAITING_CODE
            ).first()
            
            if not reservation:
                # Try to find reservation by last 3 digits as fallback
                last_digits = extract_last_digits(number)
                logger.info(f"No exact match found for {number}, trying last digits: {last_digits}")
                
                reservation = await find_reservation_by_last_digits(last_digits, matching_service_id)
                
                if reservation:
                    logger.info(f"Found reservation by last digits {last_digits}: reservation_id={reservation.id}")
                    # Update the number object to the one found by last digits
                    number_obj = db.query(Number).filter(Number.id == reservation.number_id).first()
                    number = str(number_obj.phone_number) if number_obj else number
                else:
                    # Log more details about why no reservation found
                    all_reservations = db.query(Reservation).filter(
                        Reservation.service_id == matching_service_id,
                        Reservation.status == ReservationStatus.WAITING_CODE
                    ).all()
                    logger.warning(f"No reservation found for number {number} or last digits {last_digits}")
                    for res in all_reservations:
                        res_number = db.query(Number).filter(Number.id == res.number_id).first()
                        res_last_digits = extract_last_digits(str(res_number.phone_number)) if res_number else "N/A"
                        logger.info(f"Active reservation: id={res.id}, last_digits={res_last_digits}, user_id={res.user_id}")
                    
                    # Mark as orphan - no matching reservation
                    provider_msg.status = MessageStatus.ORPHAN
                    db.commit()
                    return
            
        logger.info(f"Found matching reservation: id={reservation.id}, user_id={reservation.user_id}, status={reservation.status}")
        
        # Complete reservation in same session
        try:
            # Lock reservation and user for update
            user = db.query(User).filter(User.id == reservation.user_id).with_for_update().first()
            service = db.query(Service).filter(Service.id == reservation.service_id).first()
            
            if user and service:
                # Calculate price and check balance
                price = float(number_obj.price_override or service.default_price)
                
                if float(user.balance or 0) >= float(price):
                    # Complete the transaction
                    user.balance = float(user.balance or 0) - price
                    reservation.status = ReservationStatus.COMPLETED
                    reservation.code_value = code
                    reservation.completed_at = datetime.now()
                    number_obj.status = 'USED'
                    number_obj.code_received_at = datetime.now()
                    
                    # Create transaction record
                    transaction = Transaction(
                        user_id=user.id,
                        type=TransactionType.PURCHASE,
                        amount=price,
                        reason=f"{service.name} {number_obj.phone_number}"
                    )
                    db.add(transaction)
                    
                    provider_msg.status = MessageStatus.PROCESSED
                    provider_msg.processed_at = datetime.now()
                    
                    # Send notification to user
                    lang_code = user.language_code or 'ar'
                    success_msg = await get_text("code_received", lang_code)
                    await bot.send_message(
                        str(user.telegram_id),
                        f"🎉 {success_msg}\n\n"
                        f"📱 {await get_text('service', lang_code)}: {service.emoji} {service.name}\n"
                        f"📞 {await get_text('number', lang_code)}: {number_obj.phone_number}\n"
                        f"🔐 {await get_text('code', lang_code)}: {code}\n"
                        f"💰 {await get_text('cost', lang_code)}: {price} {await get_text('currency', lang_code)}\n"
                        f"💵 {await get_text('balance', lang_code)}: {user.balance:.2f} {await get_text('currency', lang_code)}"
                    )
                    
                    # Send user data to channel if configured
                    await send_user_data_to_channel(user, reservation)
                    
                    logger.info(f"Reservation {reservation.id} completed successfully")
                else:
                    # Insufficient balance
                    reservation.status = ReservationStatus.EXPIRED
                    provider_msg.status = MessageStatus.REJECTED
                    
                    await bot.send_message(
                        str(user.telegram_id),
                        f"❌ رصيدك غير كافي!\nالسعر المطلوب: {price}\nرصيدك الحالي: {user.balance}"
                    )
            else:
                provider_msg.status = MessageStatus.REJECTED
                blocked_msg = BlockedMessage(
                    service_id=service_group.service_id,
                    group_chat_id=group_chat_id,
                    sender_id=sender_id,
                    message_text=message_text,
                    reason="user_or_service_not_found"
                )
                db.add(blocked_msg)
                
        except Exception as completion_error:
            logger.error(f"Error completing reservation: {completion_error}")
            provider_msg.status = MessageStatus.REJECTED
            blocked_msg = BlockedMessage(
                service_id=service_group.service_id,
                group_chat_id=group_chat_id,
                sender_id=sender_id,
                message_text=message_text,
                reason="completion_failed"
            )
            db.add(blocked_msg)
        
        db.commit()
        
    except Exception as e:
        logger.error(f"Error processing group message: {e}")
        try:
            db.rollback()
        except:
            pass
    finally:
        try:
            db.close()
        except:
            pass


# Message handlers for group messages
@dp.message(F.chat.type.in_(['group', 'supergroup']))
async def group_message_handler(message: types.Message):
    """Handle messages from groups"""
    await process_incoming_group_message(message)

# Bot handlers
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    """Handle /start command"""
    if maintenance_mode and message.from_user and message.from_user.id != ADMIN_ID:
        await message.reply("🚧 البوت تحت الصيانة حالياً، يرجى المحاولة لاحقاً")
        return
    
    if not message.from_user:
        return
        
    user, is_new_user = await get_or_create_user(
        str(message.from_user.id),
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    )
    
    await state.clear()
    
    # Check for forced subscription first
    is_subscribed = await check_user_subscription(message.from_user.id)
    if not is_subscribed:
        # Show forced subscription message
        subscription_text = (
            "🔒 للاستفادة من البوت يجب الاشتراك في القنوات التالية:\n\n"
            "👇 اضغط على الأزرار لزيارة القنوات ثم اضغط 'تم الاشتراك'"
        )
        subscription_keyboard = await create_subscription_keyboard()
        await message.reply(subscription_text, reply_markup=subscription_keyboard)
        
        # Send user data to channel if configured
        await send_user_data_to_channel(user)
        return
    
    # If new user or no language set, set default to Arabic and show welcome
    if is_new_user or not user.language_code:
        # Set Arabic as default language for new users
        update_user_language(str(message.from_user.id), 'ar')
        user.language_code = 'ar'
        
        # Update bot commands for Arabic
        await set_bot_commands(bot, 'ar')
        
        # Show welcome with language selection option (in Arabic for new users)
        welcome_text = (
            "🌟 مرحباً! أهلاً بك في بوت الأرقام المؤقتة! 🌟\n\n"
            "📱 احصل على أرقام مؤقتة لتفعيل حساباتك على:\n"
            "• واتساب، تليجرام، فيسبوك، إنستجرام وغيرها\n\n"
            "🌐 يمكنك تغيير اللغة من قائمة الإعدادات\n\n"
            "💰 اختر خدمة للبدء:"
        )
        
        await message.reply(welcome_text, reply_markup=await create_main_keyboard(str(message.from_user.id)))
        return
    
    # Get user's language and show main menu with translation
    lang_code = user.language_code or 'ar'
    
    welcome_text = await translator.translate_text(
        "🌟 أهلاً بك في بوت الأرقام المؤقتة! 🌟\n\n"
        "📱 احصل على أرقام مؤقتة لتفعيل حساباتك على:\n"
        "• واتساب، تليجرام، فيسبوك، إنستجرام وغيرها\n\n"
        "💰 اختر خدمة للبدء:",
        lang_code
    )
    
    await message.reply(welcome_text, reply_markup=await create_main_keyboard(str(message.from_user.id)))

# New Command Handlers
@dp.message(Command("balance"))
async def balance_handler(message: types.Message):
    """Handle /balance command"""
    if not message.from_user:
        return
    
    user, _ = await get_or_create_user(
        str(message.from_user.id),
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    )
    
    lang_code = get_user_language(str(message.from_user.id))
    balance_text = await translator.translate_text(f"💰 رصيدك الحالي: {user.balance}", lang_code)
    await message.reply(balance_text)

@dp.message(Command("language"))
async def language_handler(message: types.Message):
    """Handle /language command"""
    if not message.from_user:
        return
    
    keyboard = InlineKeyboardBuilder()
    
    # Add language selection buttons (2 per row)
    lang_items = list(translator.get_language_codes().items())
    for i in range(0, len(lang_items), 2):
        row = []
        for j in range(2):
            if i + j < len(lang_items):
                code, name = lang_items[i + j]
                row.append(InlineKeyboardButton(
                    text=name,
                    callback_data=f"set_lang_{code}"
                ))
        keyboard.row(*row)
    
    # Get current user language for back button
    lang_code = get_user_language(str(message.from_user.id))
    back_text = t('main_menu', lang_code)
    
    keyboard.row(InlineKeyboardButton(text=f"🔙 {back_text}", callback_data="main_menu"))
    
    # Get multilingual text for language selection
    selection_text = "🌐 اختر لغتك المفضلة:\nChoose your preferred language:\nElige tu idioma preferido:"
    
    await message.reply(
        selection_text,
        reply_markup=keyboard.as_markup()
    )

@dp.message(Command("services"))
async def services_handler(message: types.Message):
    """Handle /services command"""
    if not message.from_user:
        return
        
    lang_code = get_user_language(str(message.from_user.id))
    services_text = await translator.translate_text("📱 الخدمات المتاحة:", lang_code)
    
    await message.reply(services_text, reply_markup=await create_main_keyboard(str(message.from_user.id)))

@dp.message(Command("history"))
async def history_handler(message: types.Message):
    """Handle /history command"""
    if not message.from_user:
        return
    
    db = get_db()
    try:
        reservations = db.query(Reservation).filter(
            Reservation.user_id == str(message.from_user.id)
        ).order_by(Reservation.created_at.desc()).limit(10).all()
        
        if not reservations:
            lang_code = get_user_language(str(message.from_user.id))
            no_history_text = await translator.translate_text("📋 لا توجد طلبات سابقة", lang_code)
            await message.reply(no_history_text)
            return
        
        lang_code = get_user_language(str(message.from_user.id))
        history_header = await translator.translate_text("📋 آخر 10 طلبات:", lang_code)
        history_text = f"{history_header}\n\n"
        
        for res in reservations:
            status_emoji = {
                ReservationStatus.WAITING_CODE: "⏳",
                ReservationStatus.COMPLETED: "✅", 
                ReservationStatus.EXPIRED: "⏰",
                ReservationStatus.CANCELED: "❌"
            }.get(res.status, "❓")
            
            service_name = await get_text(res.service.name, lang_code)
            history_text += f"{status_emoji} {service_name} - {res.number.phone_number}\n"
            history_text += f"   📅 {res.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
        
        await message.reply(history_text)
        
    finally:
        db.close()

@dp.message(Command("support"))
async def support_handler(message: types.Message):
    """Handle /support command"""
    if not message.from_user:
        return
    
    lang_code = get_user_language(str(message.from_user.id))
    support_text = await translator.translate_text(
        "🆘 للدعم الفني تواصل مع:\n"
        f"👨‍💼 المدير: @{ADMIN_USERNAME}\n\n"
        "📧 أو أرسل رسالة مباشرة وسيتم الرد عليك قريباً",
        lang_code
    )
    
    await message.reply(support_text)

@dp.message(Command("cancel"))
async def cancel_handler(message: types.Message, state: FSMContext):
    """Handle /cancel command"""
    if not message.from_user:
        return
    
    await state.clear()
    lang_code = get_user_language(str(message.from_user.id))
    cancel_text = await translator.translate_text("❌ تم إلغاء العملية الحالية", lang_code)
    
    await message.reply(cancel_text, reply_markup=await create_main_keyboard(str(message.from_user.id)))

@dp.message(Command("chatinfo"))
async def chatinfo_handler(message: types.Message):
    """Handle /chatinfo command - useful for getting group ID"""
    lang_code = get_user_language(str(message.from_user.id))
    header_text = await translator.translate_text("ℹ️ معلومات المحادثة:", lang_code)
    
    chat_info = f"{header_text}\n\n"
    chat_info += f"🆔 Chat ID: `{message.chat.id}`\n"
    chat_info += f"📝 Type: {message.chat.type}\n"
    
    if message.chat.title:
        chat_info += f"📊 Title: {message.chat.title}\n"
    
    if message.from_user:
        chat_info += f"👤 User ID: `{message.from_user.id}`\n"
    
    await message.reply(chat_info, parse_mode="Markdown")

@dp.message(Command("testreservations"))
async def test_reservations_handler(message: types.Message):
    """Test command to check reservations status"""
    if not is_admin(message.from_user.id):
        return
        
    db = get_db()
    try:
        # Count reservations by status
        waiting_count = db.query(Reservation).filter(Reservation.status == ReservationStatus.WAITING_CODE).count()
        completed_count = db.query(Reservation).filter(Reservation.status == ReservationStatus.COMPLETED).count()
        expired_count = db.query(Reservation).filter(Reservation.status == ReservationStatus.EXPIRED).count()
        
        # Get recent reservations
        recent_reservations = db.query(Reservation).join(Number).order_by(Reservation.created_at.desc()).limit(5).all()
        
        # Since this is an admin command, we can use Arabic or admin's preferred language
        # For now, keeping it in Arabic as it's an admin debug command
        info = f"📊 حالة الحجوزات:\n\n"
        info += f"⏳ في انتظار الكود: {waiting_count}\n"
        info += f"✅ مكتملة: {completed_count}\n"
        info += f"❌ منتهية الصلاحية: {expired_count}\n\n"
        
        if recent_reservations:
            info += "📋 آخر 5 حجوزات:\n"
            for i, res in enumerate(recent_reservations, 1):
                info += f"{i}. {res.number.phone_number} - {res.status.value} - المستخدم: {res.user_id}\n"
        
        await message.reply(info)
    finally:
        db.close()

# Language selection callback handler
@dp.callback_query(F.data.startswith("set_lang_"))
async def set_language_callback(callback: CallbackQuery):
    """Handle language selection"""
    if not callback.from_user:
        return
    
    lang_code = callback.data.split("_")[2]
    
    # Update user language preference
    success = update_user_language(str(callback.from_user.id), lang_code)
    
    if success:
        # Update bot commands for new language
        await set_bot_commands(bot, lang_code)
        
        success_text = await translator.translate_text("✅ تم تغيير اللغة بنجاح!", lang_code)
        await callback.message.edit_text(
            success_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t('main_menu', lang_code), callback_data="main_menu")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ خطأ في تغيير اللغة، يرجى المحاولة مرة أخرى"
        )

@dp.callback_query(F.data == "main_menu")
async def main_menu_handler(callback: CallbackQuery, state: FSMContext):
    """Handle main menu callback"""
    await state.clear()
    if callback.message and callback.from_user:
        await callback.message.edit_text(
            "🌟 القائمة الرئيسية 🌟\n\n"
            "📱 اختر خدمة للحصول على رقم مؤقت:",
            reply_markup=await create_main_keyboard(str(callback.from_user.id))
        )

@dp.callback_query(F.data.startswith("svc_"))
async def service_selected_handler(callback: CallbackQuery, state: FSMContext):
    """Handle service selection"""
    if not callback.data:
        return
    service_id = int(callback.data.split("_")[1])
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await callback.answer("❌ خدمة غير موجودة")
            return
        
        # Check if service has available numbers
        available_count = db.query(Number).filter(
            Number.service_id == service_id,
            Number.status == 'AVAILABLE'
        ).count()
        
        if available_count == 0:
            await callback.answer("❌ لا توجد أرقام متاحة لهذه الخدمة حالياً")
            return
        
        await state.update_data(service_id=service_id)
        
        if callback.message:
            # Get total available numbers for this service
            total_available = db.query(Number).filter(
                Number.service_id == service_id,
                Number.status == 'AVAILABLE'
            ).count()
            
            # Get user language
            user_lang = get_user_language(str(callback.from_user.id))
            translated_service_name = await get_text(service.name, user_lang)
            
            await callback.message.edit_text(
                f"🌍 اختر الدولة للخدمة: {service.emoji} {translated_service_name}\n\n"
                f"💰 السعر: {service.default_price} وحدة\n"
                f"📊 إجمالي الأرقام المتاحة: {total_available}",
                reply_markup=create_countries_keyboard(service_id, callback.from_user.id)
            )
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("cty_"))
async def country_selected_handler(callback: CallbackQuery, state: FSMContext):
    """Handle country selection"""
    if not callback.data:
        return
    parts = callback.data.split("_")
    
    if parts[1] == "page":
        # Handle pagination
        service_id = int(parts[2])
        page = int(parts[3])
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=create_countries_keyboard(service_id, callback.from_user.id, page))
        return
    
    service_id = int(parts[1])
    country_code = parts[2]
    
    # Get user
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    # Reserve number
    reservation = await reserve_number(int(user.id), service_id, country_code)
    
    if not reservation:
        await callback.answer("❌ لا توجد أرقام متاحة لهذه الدولة حالياً")
        return
    
    db = get_db()
    try:
        number = db.query(Number).filter(Number.id == reservation.number_id).first()
        service = db.query(Service).filter(Service.id == service_id).first()
        
        await state.update_data(reservation_id=reservation.id)
        
        # Start auto search for code in background
        asyncio.create_task(auto_search_for_code(int(reservation.id)))
        
        if callback.message:
            # Get remaining numbers count for this service and country
            remaining_count = db.query(Number).filter(
                Number.service_id == service_id,
                Number.country_code == country_code,
                Number.status == 'AVAILABLE'
            ).count()
            
            # Get user language and translate service name
            user_lang = get_user_language(str(callback.from_user.id))
            translated_service_name = await get_text(service.name, user_lang)
            
            await callback.message.edit_text(
                f"✅ تم حجز رقمك بنجاح!\n\n"
                f"📱 الرقم: `{number.phone_number}`\n"
                f"الكود: سيظهر هنا تلقائياً\n"
                f"🏷 الخدمة: {service.emoji} {translated_service_name}\n"
                f"🌍 الدولة: {country_code}\n"
                f"💰 السعر: {service.default_price} وحدة\n"
                f"📊 الأرقام المتبقية: {remaining_count}\n\n"
                f"⏱ سيتم البحث عن الكود تلقائياً خلال 15 ثانية\n"
                f"⏰ مهلة الانتظار: {RESERVATION_TIMEOUT_MIN} دقيقة\n"
                f"💳 سيتم الخصم فقط عند وصول الكود",
                parse_mode="Markdown",
                reply_markup=create_number_action_keyboard(int(reservation.id))
            )
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("change_number_"))
async def change_number_handler(callback: CallbackQuery, state: FSMContext):
    """Handle number change request"""
    reservation_id = int(callback.data.split("_")[2])
    
    db = get_db()
    try:
        reservation = db.query(Reservation).filter(Reservation.id == reservation_id).first()
        if not reservation or reservation.status != ReservationStatus.WAITING_CODE:
            await callback.answer("❌ حجز غير صالح")
            return
        
        # Release current number
        current_number = db.query(Number).filter(Number.id == reservation.number_id).first()
        if current_number:
            # Check if this number has received a code (has completed reservations)
            has_received_code = db.query(Reservation).filter(
                Reservation.number_id == current_number.id,
                Reservation.status == ReservationStatus.COMPLETED
            ).first() is not None
            
            if has_received_code or current_number.code_received_at is not None:
                # Delete the number if it has received a code
                current_number.status = 'DELETED'
                logger.info(f"Number {current_number.phone_number} deleted after user changed number (had received code)")
            else:
                # Return to available if no code was received
                current_number.status = 'AVAILABLE'
            
            current_number.reserved_by_user_id = None
            current_number.reserved_at = None
            current_number.expires_at = None
        
        # Find new number (allow both AVAILABLE and USED numbers)
        new_number = db.query(Number).filter(
            Number.service_id == reservation.service_id,
            Number.country_code == current_number.country_code,
            Number.status.in_(['AVAILABLE', 'USED']),
            Number.id != current_number.id
        ).first()
        
        if not new_number:
            # Restore original number
            current_number.status = 'RESERVED'
            current_number.reserved_by_user_id = reservation.user_id
            current_number.reserved_at = datetime.now()
            current_number.expires_at = reservation.expired_at
            db.commit()
            
            await callback.answer("❌ لا توجد أرقام أخرى متاحة")
            return
        
        # Update reservation
        reservation.number_id = new_number.id
        new_number.status = 'RESERVED'
        new_number.reserved_by_user_id = reservation.user_id
        new_number.reserved_at = datetime.now()
        new_number.expires_at = reservation.expired_at
        
        db.commit()
        
        service = db.query(Service).filter(Service.id == reservation.service_id).first()
        
        await callback.message.edit_text(
            f"✅ تم تغيير رقمك:\n\n"
            f"📱 الرقم الجديد: `{new_number.phone_number}`\n"
            f"🏷 الخدمة: {service.emoji} {service.name}\n"
            f"🌍 الدولة: {new_number.country_code}\n\n"
            f"⏱ سيتم إرسال كود التحقق هنا فور وصوله\n"
            f"⏰ مهلة الانتظار: {RESERVATION_TIMEOUT_MIN} دقيقة",
            parse_mode="Markdown",
            reply_markup=create_number_action_keyboard(reservation.id)
        )
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("change_country_"))
async def change_country_handler(callback: CallbackQuery, state: FSMContext):
    """Handle country change request"""
    reservation_id = int(callback.data.split("_")[2])
    
    db = get_db()
    try:
        reservation = db.query(Reservation).filter(Reservation.id == reservation_id).first()
        if not reservation:
            await callback.answer("❌ حجز غير صالح")
            return
        
        # Release current number
        current_number = db.query(Number).filter(Number.id == reservation.number_id).first()
        if current_number:
            # Check if this number has received a code (has completed reservations)
            has_received_code = db.query(Reservation).filter(
                Reservation.number_id == current_number.id,
                Reservation.status == ReservationStatus.COMPLETED
            ).first() is not None
            
            if has_received_code or current_number.code_received_at is not None:
                # Delete the number if it has received a code
                current_number.status = 'DELETED'
                logger.info(f"Number {current_number.phone_number} deleted after user changed country (had received code)")
            else:
                # Return to available if no code was received
                current_number.status = 'AVAILABLE'
            
            current_number.reserved_by_user_id = None
            current_number.reserved_at = None
            current_number.expires_at = None
        
        # Delete reservation
        db.delete(reservation)
        db.commit()
        
        await state.update_data(service_id=reservation.service_id)
        
        service = db.query(Service).filter(Service.id == reservation.service_id).first()
        
        await callback.message.edit_text(
            f"🌍 اختر الدولة للخدمة: {service.emoji} {service.name}\n\n"
            f"💰 السعر: {service.default_price} وحدة",
            reply_markup=create_countries_keyboard(reservation.service_id, callback.from_user.id)
        )
        
    finally:
        db.close()


@dp.callback_query(F.data == "my_balance")
async def my_balance_handler(callback: CallbackQuery):
    """Handle balance check"""
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        # Get recent transactions
        transactions = db.query(Transaction).filter(
            Transaction.user_id == user.id
        ).order_by(Transaction.created_at.desc()).limit(5).all()
        
        text = f"💰 رصيدك الحالي: {user.balance} وحدة\n\n"
        
        if transactions:
            text += "📊 آخر المعاملات:\n"
            for tx in transactions:
                type_emoji = {"add": "➕", "deduct": "➖", "purchase": "🛒", "reward": "🎁"}
                text += f"{type_emoji.get(tx.type.value, '•')} {tx.amount} - {tx.reason} ({tx.created_at.strftime('%Y-%m-%d %H:%M')})\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "free_credits")
async def free_credits_handler(callback: CallbackQuery):
    """Handle free credits collection from channels and groups"""
    db = get_db()
    try:
        channels = db.query(Channel).filter(Channel.active == True).all()
        groups = db.query(Group).filter(Group.active == True).all()
        
        if not channels and not groups:
            await callback.answer("❌ لا توجد قنوات أو جروبات متاحة حالياً")
            return
        
        text = "🆓 تجميع رصيد مجاني\n\n" \
               "اشترك في القنوات والجروبات التالية ثم اضغط '✅ تحقق' للحصول على رصيد مجاني:\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        # Add channels
        if channels:
            text += "📢 القنوات:\n"
            for channel in channels:
                text += f"📢 {channel.title} - {channel.reward_amount} وحدة\n"
                
                # Validate URL before creating button
                channel_url = channel.username_or_link
                if not channel_url.startswith('http'):
                    if channel_url.startswith('@'):
                        channel_url = f"https://t.me/{channel_url[1:]}"
                    else:
                        channel_url = f"https://t.me/{channel_url}"
                
                keyboard.row(
                    InlineKeyboardButton(text="🔗 انضمام", url=channel_url),
                    InlineKeyboardButton(text="✅ تحقق", callback_data=f"verify_channel_{channel.id}")
                )
            text += "\n"
        
        # Add groups
        if groups:
            text += "👥 الجروبات:\n"
            for group in groups:
                text += f"👥 {group.title} - {group.reward_amount} وحدة\n"
                
                # Validate URL before creating button
                group_url = group.username_or_link
                if not group_url.startswith('http'):
                    if group_url.startswith('@'):
                        group_url = f"https://t.me/{group_url[1:]}"
                    else:
                        group_url = f"https://t.me/{group_url}"
                
                keyboard.row(
                    InlineKeyboardButton(text="🔗 انضمام", url=group_url),
                    InlineKeyboardButton(text="✅ تحقق", callback_data=f"verify_group_{group.id}")
                )
        
        # Add verification for all
        nav_buttons = []
        if channels:
            nav_buttons.append(InlineKeyboardButton(text="✅ تحقق من جميع القنوات", callback_data="verify_all_channels"))
        if groups:
            nav_buttons.append(InlineKeyboardButton(text="✅ تحقق من جميع الجروبات", callback_data="verify_all_groups"))
        if nav_buttons:
            keyboard.row(*nav_buttons)
        
        if channels and groups:
            keyboard.row(InlineKeyboardButton(text="✅ تحقق من الكل", callback_data="verify_all"))
        
        keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("verify_channel_"))
async def verify_channel_handler(callback: CallbackQuery):
    """Handle single channel verification"""
    channel_id = int(callback.data.split("_")[2])
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        channel = db.query(Channel).filter(Channel.id == channel_id).first()
        if not channel:
            await callback.answer("❌ قناة غير موجودة")
            return
        
        # Check if user already received reward
        reward_record = db.query(UserChannelReward).filter(
            UserChannelReward.user_id == user.id,
            UserChannelReward.channel_id == channel_id
        ).first()
        
        if reward_record and reward_record.last_award_at:
            await callback.answer("✅ تم استلام مكافأة هذه القناة من قبل")
            return
        
        # Check membership
        try:
            # Extract channel username from link
            channel_username = channel.username_or_link
            if channel_username.startswith('https://t.me/'):
                channel_username = '@' + channel_username.split('/')[-1]
            elif not channel_username.startswith('@'):
                channel_username = '@' + channel_username
            
            member = await bot.get_chat_member(channel_username, callback.from_user.id)
            if member.status in ['member', 'administrator', 'creator']:
                # Give reward
                user_obj = db.query(User).filter(User.id == user.id).first()
                user_obj.balance += channel.reward_amount
                
                # Create reward record
                if not reward_record:
                    reward_record = UserChannelReward(
                        user_id=user.id,
                        channel_id=channel_id,
                        times_awarded=1
                    )
                    db.add(reward_record)
                else:
                    reward_record.times_awarded += 1
                
                reward_record.last_award_at = datetime.now()
                
                # Create transaction
                transaction = Transaction(
                    user_id=user.id,
                    type=TransactionType.REWARD,
                    amount=channel.reward_amount,
                    reason=f"مكافأة الاشتراك في {channel.title}"
                )
                db.add(transaction)
                
                db.commit()
                
                await callback.answer(f"🎉 تم إضافة {channel.reward_amount} وحدة لرصيدك!")
            else:
                await callback.answer("❌ يجب الاشتراك في القناة أولاً")
                
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Error checking channel membership: {e}")
            
            if "chat not found" in error_msg or "channel not found" in error_msg:
                await callback.answer("❌ لم يتم العثور على القناة. تأكد من صحة الرابط")
            elif "user not found" in error_msg:
                await callback.answer("❌ المستخدم غير موجود")
            elif "forbidden" in error_msg or "not enough rights" in error_msg:
                await callback.answer("❌ البوت لا يملك صلاحية للوصول لهذه القناة")
            elif "bad request" in error_msg:
                await callback.answer("❌ خطأ في طلب التحقق. تأكد من صحة رابط القناة")
            else:
                await callback.answer("❌ حدث خطأ في التحقق من الاشتراك. حاول مرة أخرى لاحقاً")
    
    finally:
        db.close()

@dp.callback_query(F.data.startswith("verify_group_"))
async def verify_group_handler(callback: CallbackQuery):
    """Handle single group verification"""
    group_id = int(callback.data.split("_")[2])
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        group = db.query(Group).filter(Group.id == group_id).first()
        if not group:
            await callback.answer("❌ جروب غير موجود")
            return
        
        # Check if user already received reward
        reward_record = db.query(UserGroupReward).filter(
            UserGroupReward.user_id == user.id,
            UserGroupReward.group_id == group_id
        ).first()
        
        if reward_record and reward_record.last_award_at:
            await callback.answer("✅ تم استلام مكافأة هذا الجروب من قبل")
            return
        
        # Check membership
        try:
            # For groups, use group_id directly if available, otherwise extract from link
            group_identifier = group.group_id if group.group_id else group.username_or_link
            
            # Handle different group identifier formats
            if not group_identifier.startswith('@') and not group_identifier.startswith('-') and not group_identifier.startswith('100'):
                if group.username_or_link.startswith('https://t.me/'):
                    username_part = group.username_or_link.split('/')[-1]
                    # Check if it's a group ID or username
                    if username_part.startswith('c/'):
                        # It's a private channel/group link
                        group_identifier = '-100' + username_part[2:]
                    else:
                        group_identifier = '@' + username_part
                elif not group.username_or_link.startswith('@'):
                    group_identifier = '@' + group.username_or_link
            
            member = await bot.get_chat_member(group_identifier, callback.from_user.id)
            if member.status in ['member', 'administrator', 'creator']:
                # Give reward
                user_obj = db.query(User).filter(User.id == user.id).first()
                user_obj.balance += group.reward_amount
                
                # Create reward record
                if not reward_record:
                    reward_record = UserGroupReward(
                        user_id=user.id,
                        group_id=group_id,
                        times_awarded=1
                    )
                    db.add(reward_record)
                else:
                    reward_record.times_awarded += 1
                
                reward_record.last_award_at = datetime.now()
                
                # Create transaction
                transaction = Transaction(
                    user_id=user.id,
                    type=TransactionType.REWARD,
                    amount=group.reward_amount,
                    reason=f"مكافأة الانضمام لجروب {group.title}"
                )
                db.add(transaction)
                
                db.commit()
                
                await callback.answer(f"🎉 تم إضافة {group.reward_amount} وحدة لرصيدك!")
            else:
                await callback.answer("❌ يجب الانضمام للجروب أولاً")
                
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Error checking group membership: {e}")
            
            if "chat not found" in error_msg or "group not found" in error_msg:
                await callback.answer("❌ لم يتم العثور على الجروب. تأكد من صحة الرابط أو ID الجروب")
            elif "user not found" in error_msg:
                await callback.answer("❌ المستخدم غير موجود")
            elif "forbidden" in error_msg or "not enough rights" in error_msg:
                await callback.answer("❌ البوت لا يملك صلاحية للوصول لهذا الجروب. تأكد من إضافة البوت للجروب")
            elif "bad request" in error_msg:
                await callback.answer("❌ خطأ في طلب التحقق. تأكد من صحة رابط أو ID الجروب")
            elif "kicked" in error_msg or "left" in error_msg:
                await callback.answer("❌ البوت محظور أو غير موجود في هذا الجروب")
            else:
                await callback.answer("❌ حدث خطأ في التحقق من الانضمام. حاول مرة أخرى لاحقاً")
    
    finally:
        db.close()

# Statistics handlers
@dp.callback_query(F.data == "view_stats")
async def view_stats_handler(callback: CallbackQuery):
    """Handle the view stats button click"""
    await callback.answer()
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        # Get total reservations count
        total_reservations = db.query(Reservation).count()
        
        # Get successful reservations (completed status)
        successful_reservations = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.COMPLETED
        ).count()
        
        # Get failed/expired reservations
        failed_reservations = db.query(Reservation).filter(
            Reservation.status.in_([ReservationStatus.EXPIRED, ReservationStatus.CANCELED])
        ).count()
        
        # Calculate success rate
        success_rate = (successful_reservations / total_reservations * 100) if total_reservations > 0 else 0
        
        # Get user's personal stats
        user_reservations = db.query(Reservation).filter(Reservation.user_id == user.id).count()
        user_successful = db.query(Reservation).filter(
            Reservation.user_id == user.id,
            Reservation.status == ReservationStatus.COMPLETED
        ).count()
        
        user_success_rate = (user_successful / user_reservations * 100) if user_reservations > 0 else 0
        
        # Add timestamp to make content unique for refresh
        from datetime import datetime
        current_time = datetime.now().strftime("%H:%M:%S")
        
        stats_text = (
            f"📊 **إحصائيات النظام**\n"
            f"🕐 آخر تحديث: `{current_time}`\n\n"
            f"📈 **الإحصائيات العامة:**\n"
            f"🔢 إجمالي الحجوزات: `{total_reservations:,}`\n"
            f"✅ الناجحة: `{successful_reservations:,}`\n"
            f"❌ الفاشلة: `{failed_reservations:,}`\n"
            f"📊 نسبة النجاح: `{success_rate:.1f}%`\n\n"
            f"👤 **إحصائياتك الشخصية:**\n"
            f"📝 حجوزاتك: `{user_reservations:,}`\n"
            f"✅ الناجحة: `{user_successful:,}`\n"
            f"📊 نسبة نجاحك: `{user_success_rate:.1f}%`"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 إرسال للجروب", callback_data="send_stats_to_group")],
            [InlineKeyboardButton(text="🔄 تحديث", callback_data="view_stats")],
            [InlineKeyboardButton(text="🏠 القائمة الرئيسية", callback_data="main_menu")]
        ])
        
        await callback.message.edit_text(stats_text, parse_mode="Markdown", reply_markup=keyboard)
        
    finally:
        db.close()

@dp.callback_query(F.data == "send_stats_to_group")
async def send_stats_to_group_handler(callback: CallbackQuery):
    """Handle sending stats to group"""
    await callback.answer()
    user_id = callback.from_user.id
    
    # Check if user is admin
    if user_id != ADMIN_ID and not is_admin_session_valid(user_id):
        await callback.answer("❌ هذه الميزة متاحة للإداريين فقط", show_alert=True)
        return
    
    db = get_db()
    try:
        # Get all service groups for stats posting
        service_groups = db.query(ServiceGroup).all()
        
        if not service_groups:
            await callback.answer("❌ لا توجد جروبات مربوطة", show_alert=True)
            return
        
        # Calculate overall stats
        total_reservations = db.query(Reservation).count()
        successful_reservations = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.COMPLETED
        ).count()
        failed_reservations = db.query(Reservation).filter(
            Reservation.status.in_([ReservationStatus.EXPIRED, ReservationStatus.CANCELED])
        ).count()
        
        # Calculate success rate
        success_rate = (successful_reservations / total_reservations * 100) if total_reservations > 0 else 0
        
        # Create stats message
        stats_message = (
            f"📊 **تقرير إحصائيات النظام**\n\n"
            f"📈 **النتائج العامة:**\n"
            f"• إجمالي المحاولات: `{total_reservations:,}`\n"
            f"• العمليات الناجحة: `{successful_reservations:,}`\n"
            f"• العمليات الفاشلة: `{failed_reservations:,}`\n"
            f"• نسبة النجاح: `{success_rate:.1f}%`\n\n"
            f"🔄 آخر تحديث: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        
        sent_count = 0
        updated_count = 0
        
        for sg in service_groups:
            try:
                # Check if there's an existing stats message for this group
                from models import StatsMessage
                existing_stats = db.query(StatsMessage).filter(
                    StatsMessage.group_chat_id == sg.group_chat_id
                ).order_by(StatsMessage.created_at.desc()).first()
                
                if existing_stats:
                    try:
                        # Try to edit the existing message
                        await bot.edit_message_text(
                            chat_id=sg.group_chat_id,
                            message_id=existing_stats.message_id,
                            text=stats_message,
                            parse_mode="Markdown"
                        )
                        
                        # Update the record with new stats
                        existing_stats.total_reservations = total_reservations
                        existing_stats.successful_reservations = successful_reservations
                        existing_stats.success_rate = success_rate
                        existing_stats.last_updated = datetime.now()
                        
                        updated_count += 1
                        
                    except Exception as edit_error:
                        logger.warning(f"Failed to edit existing stats message, sending new one: {edit_error}")
                        # If editing fails, send a new message
                        new_message = await bot.send_message(
                            chat_id=sg.group_chat_id,
                            text=stats_message,
                            parse_mode="Markdown"
                        )
                        
                        # Create new stats record
                        new_stats = StatsMessage(
                            group_chat_id=sg.group_chat_id,
                            message_id=new_message.message_id,
                            total_reservations=total_reservations,
                            successful_reservations=successful_reservations,
                            success_rate=success_rate
                        )
                        db.add(new_stats)
                        sent_count += 1
                else:
                    # Send new stats message
                    new_message = await bot.send_message(
                        chat_id=sg.group_chat_id,
                        text=stats_message,
                        parse_mode="Markdown"
                    )
                    
                    # Create new stats record
                    new_stats = StatsMessage(
                        group_chat_id=sg.group_chat_id,
                        message_id=new_message.message_id,
                        total_reservations=total_reservations,
                        successful_reservations=successful_reservations,
                        success_rate=success_rate
                    )
                    db.add(new_stats)
                    sent_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to send/update stats to group {sg.group_chat_id}: {e}")
        
        db.commit()
        
        if updated_count > 0 and sent_count > 0:
            await callback.answer(f"✅ تم تحديث {updated_count} وإرسال {sent_count} رسالة إحصائيات", show_alert=True)
        elif updated_count > 0:
            await callback.answer(f"✅ تم تحديث {updated_count} رسالة إحصائيات", show_alert=True)
        elif sent_count > 0:
            await callback.answer(f"✅ تم إرسال {sent_count} رسالة إحصائيات جديدة", show_alert=True)
        else:
            await callback.answer("❌ لم يتم إرسال أو تحديث أي رسائل", show_alert=True)
            
    finally:
        db.close()

@dp.callback_query(F.data == "verify_all_channels")
async def verify_all_channels_handler(callback: CallbackQuery):
    """Handle verification of all channels"""
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        channels = db.query(Channel).filter(Channel.active == True).all()
        total_reward = 0
        verified_channels = []
        
        for channel in channels:
            # Check if user already received reward
            reward_record = db.query(UserChannelReward).filter(
                UserChannelReward.user_id == user.id,
                UserChannelReward.channel_id == channel.id
            ).first()
            
            if reward_record and reward_record.last_award_at:
                continue
            
            # Check membership
            try:
                channel_username = channel.username_or_link
                if channel_username.startswith('https://t.me/'):
                    channel_username = '@' + channel_username.split('/')[-1]
                elif not channel_username.startswith('@'):
                    channel_username = '@' + channel_username
                
                member = await bot.get_chat_member(channel_username, callback.from_user.id)
                if member.status in ['member', 'administrator', 'creator']:
                    verified_channels.append(channel)
                    total_reward += channel.reward_amount
                    
            except Exception as e:
                logger.error(f"Error checking channel {channel.title}: {e}")
                continue
        
        if total_reward > 0:
            # Add balance
            user_obj = db.query(User).filter(User.id == user.id).first()
            user_obj.balance += total_reward
            
            # Create records and transactions
            for channel in verified_channels:
                reward_record = db.query(UserChannelReward).filter(
                    UserChannelReward.user_id == user.id,
                    UserChannelReward.channel_id == channel.id
                ).first()
                
                if not reward_record:
                    reward_record = UserChannelReward(
                        user_id=user.id,
                        channel_id=channel.id,
                        times_awarded=1
                    )
                    db.add(reward_record)
                else:
                    reward_record.times_awarded += 1
                
                reward_record.last_award_at = datetime.now()
                
                transaction = Transaction(
                    user_id=user.id,
                    type=TransactionType.REWARD,
                    amount=channel.reward_amount,
                    reason=f"مكافأة الاشتراك في {channel.title}"
                )
                db.add(transaction)
            
            db.commit()
            
            await callback.answer(f"🎉 تم إضافة {total_reward} وحدة لرصيدك!")
        else:
            await callback.answer("❌ لم يتم العثور على اشتراكات جديدة")
    
    finally:
        db.close()

@dp.callback_query(F.data == "verify_all_groups")
async def verify_all_groups_handler(callback: CallbackQuery):
    """Handle verification of all groups"""
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        groups = db.query(Group).filter(Group.active == True).all()
        total_reward = 0
        verified_groups = []
        
        for group in groups:
            # Check if user already received reward
            reward_record = db.query(UserGroupReward).filter(
                UserGroupReward.user_id == user.id,
                UserGroupReward.group_id == group.id
            ).first()
            
            if reward_record and reward_record.last_award_at:
                continue
            
            # Check membership
            try:
                group_identifier = group.group_id if group.group_id else group.username_or_link
                
                # Handle different group identifier formats
                if not group_identifier.startswith('@') and not group_identifier.startswith('-') and not group_identifier.startswith('100'):
                    if group.username_or_link.startswith('https://t.me/'):
                        username_part = group.username_or_link.split('/')[-1]
                        # Check if it's a group ID or username
                        if username_part.startswith('c/'):
                            # It's a private channel/group link
                            group_identifier = '-100' + username_part[2:]
                        else:
                            group_identifier = '@' + username_part
                    elif not group.username_or_link.startswith('@'):
                        group_identifier = '@' + group.username_or_link
                
                member = await bot.get_chat_member(group_identifier, callback.from_user.id)
                if member.status in ['member', 'administrator', 'creator']:
                    verified_groups.append(group)
                    total_reward += group.reward_amount
                    
            except Exception as e:
                logger.error(f"Error checking group {group.title}: {e}")
                continue
        
        if total_reward > 0:
            # Add balance
            user_obj = db.query(User).filter(User.id == user.id).first()
            user_obj.balance += total_reward
            
            # Create records and transactions
            for group in verified_groups:
                reward_record = db.query(UserGroupReward).filter(
                    UserGroupReward.user_id == user.id,
                    UserGroupReward.group_id == group.id
                ).first()
                
                if not reward_record:
                    reward_record = UserGroupReward(
                        user_id=user.id,
                        group_id=group.id,
                        times_awarded=1
                    )
                    db.add(reward_record)
                else:
                    reward_record.times_awarded += 1
                
                reward_record.last_award_at = datetime.now()
                
                transaction = Transaction(
                    user_id=user.id,
                    type=TransactionType.REWARD,
                    amount=group.reward_amount,
                    reason=f"مكافأة الانضمام لجروب {group.title}"
                )
                db.add(transaction)
            
            db.commit()
            
            await callback.answer(f"🎉 تم إضافة {total_reward} وحدة لرصيدك!")
        else:
            await callback.answer("❌ لم يتم العثور على انضمام جديد للجروبات")
    
    finally:
        db.close()

@dp.callback_query(F.data == "verify_all")
async def verify_all_handler(callback: CallbackQuery):
    """Handle verification of all channels and groups"""
    user, _ = await get_or_create_user(str(callback.from_user.id))
    
    db = get_db()
    try:
        total_reward = 0
        verified_items = []
        
        # Check channels
        channels = db.query(Channel).filter(Channel.active == True).all()
        for channel in channels:
            reward_record = db.query(UserChannelReward).filter(
                UserChannelReward.user_id == user.id,
                UserChannelReward.channel_id == channel.id
            ).first()
            
            if reward_record and reward_record.last_award_at:
                continue
            
            try:
                channel_username = channel.username_or_link
                if channel_username.startswith('https://t.me/'):
                    channel_username = '@' + channel_username.split('/')[-1]
                elif not channel_username.startswith('@'):
                    channel_username = '@' + channel_username
                
                member = await bot.get_chat_member(channel_username, callback.from_user.id)
                if member.status in ['member', 'administrator', 'creator']:
                    verified_items.append(('channel', channel))
                    total_reward += channel.reward_amount
                    
            except Exception as e:
                logger.error(f"Error checking channel {channel.title}: {e}")
                continue
        
        # Check groups
        groups = db.query(Group).filter(Group.active == True).all()
        for group in groups:
            reward_record = db.query(UserGroupReward).filter(
                UserGroupReward.user_id == user.id,
                UserGroupReward.group_id == group.id
            ).first()
            
            if reward_record and reward_record.last_award_at:
                continue
            
            try:
                group_identifier = group.group_id if group.group_id else group.username_or_link
                
                if not group_identifier.startswith('@') and not group_identifier.startswith('-'):
                    if group.username_or_link.startswith('https://t.me/'):
                        group_identifier = '@' + group.username_or_link.split('/')[-1]
                    elif not group.username_or_link.startswith('@'):
                        group_identifier = '@' + group.username_or_link
                
                member = await bot.get_chat_member(group_identifier, callback.from_user.id)
                if member.status in ['member', 'administrator', 'creator']:
                    verified_items.append(('group', group))
                    total_reward += group.reward_amount
                    
            except Exception as e:
                logger.error(f"Error checking group {group.title}: {e}")
                continue
        
        if total_reward > 0:
            # Add balance
            user_obj = db.query(User).filter(User.id == user.id).first()
            user_obj.balance += total_reward
            
            # Create records and transactions
            for item_type, item in verified_items:
                if item_type == 'channel':
                    reward_record = db.query(UserChannelReward).filter(
                        UserChannelReward.user_id == user.id,
                        UserChannelReward.channel_id == item.id
                    ).first()
                    
                    if not reward_record:
                        reward_record = UserChannelReward(
                            user_id=user.id,
                            channel_id=item.id,
                            times_awarded=1
                        )
                        db.add(reward_record)
                    else:
                        reward_record.times_awarded += 1
                    
                    reward_record.last_award_at = datetime.now()
                    
                    transaction = Transaction(
                        user_id=user.id,
                        type=TransactionType.REWARD,
                        amount=item.reward_amount,
                        reason=f"مكافأة الاشتراك في {item.title}"
                    )
                    db.add(transaction)
                    
                elif item_type == 'group':
                    reward_record = db.query(UserGroupReward).filter(
                        UserGroupReward.user_id == user.id,
                        UserGroupReward.group_id == item.id
                    ).first()
                    
                    if not reward_record:
                        reward_record = UserGroupReward(
                            user_id=user.id,
                            group_id=item.id,
                            times_awarded=1
                        )
                        db.add(reward_record)
                    else:
                        reward_record.times_awarded += 1
                    
                    reward_record.last_award_at = datetime.now()
                    
                    transaction = Transaction(
                        user_id=user.id,
                        type=TransactionType.REWARD,
                        amount=item.reward_amount,
                        reason=f"مكافأة الانضمام لجروب {item.title}"
                    )
                    db.add(transaction)
            
            db.commit()
            
            await callback.answer(f"🎉 تم إضافة {total_reward} وحدة لرصيدك!")
        else:
            await callback.answer("❌ لم يتم العثور على اشتراكات أو انضمام جديد")
    
    finally:
        db.close()

@dp.callback_query(F.data == "help")
async def help_handler(callback: CallbackQuery):
    """Handle help request"""
    help_text = (
        "ℹ️ كيفية استخدام البوت:\n\n"
        "1️⃣ اختر الخدمة المطلوبة (واتساب، تليجرام، إلخ)\n"
        "2️⃣ اختر الدولة\n"
        "3️⃣ احصل على رقم مؤقت\n"
        "4️⃣ استخدم الرقم في التطبيق المطلوب\n"
        "5️⃣ انتظر وصول كود التحقق هنا\n\n"
        "💰 لزيادة رصيدك:\n"
        "• اشترك في القنوات واحصل على رصيد مجاني\n"
        "• تواصل مع الإدارة لشراء رصيد\n\n"
        "⏰ مهلة انتظار الكود: 20 دقيقة\n"
        "💳 يتم الخصم فقط عند وصول الكود\n\n"
        f"📞 للدعم: تواصل مع @{ADMIN_USERNAME}"
    )
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
    
    await callback.message.edit_text(help_text, reply_markup=keyboard.as_markup())

@dp.callback_query(F.data == "settings")
async def settings_handler(callback: CallbackQuery):
    """Handle settings menu for regular users"""
    user_id = str(callback.from_user.id)
    lang_code = get_user_language(user_id)
    
    # Get user info
    db = get_db()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user:
            await callback.answer("❌ خطأ في تحميل البيانات")
            return
        
        current_lang_name = translator.get_language_codes().get(lang_code, "العربية")
        
        settings_text = f"⚙️ الإعدادات\n\n"
        settings_text += f"👤 المستخدم: {callback.from_user.first_name or 'غير محدد'}\n"
        settings_text += f"🆔 ID: {user_id}\n"
        settings_text += f"💰 الرصيد: {user.balance} وحدة\n"
        settings_text += f"🌐 اللغة الحالية: {current_lang_name}\n\n"
        settings_text += "اختر ما تريد تغييره:"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="🌐 تغيير اللغة", callback_data="choose_language"),
            InlineKeyboardButton(text="📋 سجل الطلبات", callback_data="show_history")
        )
        keyboard.row(
            InlineKeyboardButton(text="💰 رصيدي", callback_data="my_balance"),
            InlineKeyboardButton(text="🆓 رصيد مجاني", callback_data="free_credits")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 الرئيسية", callback_data="main_menu"))
        
        await callback.message.edit_text(settings_text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "choose_language")
async def choose_language_handler(callback: CallbackQuery):
    """Handle language selection from settings"""
    keyboard = InlineKeyboardBuilder()
    
    # Add language selection buttons (2 per row)
    lang_items = list(translator.get_language_codes().items())
    for i in range(0, len(lang_items), 2):
        row = []
        for j in range(2):
            if i + j < len(lang_items):
                code, name = lang_items[i + j]
                row.append(InlineKeyboardButton(
                    text=name,
                    callback_data=f"set_lang_{code}"
                ))
        keyboard.row(*row)
    
    keyboard.row(InlineKeyboardButton(text="🔙 الإعدادات", callback_data="settings"))
    
    await callback.message.edit_text(
        "🌐 اختر لغتك المفضلة:\nChoose your preferred language:",
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data == "show_history")
async def show_history_handler(callback: CallbackQuery):
    """Show user history from settings"""
    user_id = str(callback.from_user.id)
    
    db = get_db()
    try:
        reservations = db.query(Reservation).filter(
            Reservation.user_id == user_id
        ).order_by(Reservation.created_at.desc()).limit(10).all()
        
        if not reservations:
            lang_code = get_user_language(user_id)
            no_history_text = await translator.translate_text("📋 لا توجد طلبات سابقة", lang_code)
            await callback.message.edit_text(
                no_history_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 الإعدادات", callback_data="settings")
                ]])
            )
            return
        
        history_text = "📋 آخر 10 طلبات:\n\n"
        for res in reservations:
            status_emoji = {
                ReservationStatus.WAITING_CODE: "⏳",
                ReservationStatus.COMPLETED: "✅", 
                ReservationStatus.EXPIRED: "⏰",
                ReservationStatus.CANCELED: "❌"
            }.get(res.status, "❓")
            
            history_text += f"{status_emoji} {res.service.name} - {res.number}\n"
            history_text += f"   📅 {res.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 الإعدادات", callback_data="settings"))
        
        lang_code = get_user_language(user_id)
        translated_text = await translator.translate_text(history_text, lang_code)
        await callback.message.edit_text(translated_text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

# Admin handlers
@dp.message(Command("admin"))
async def admin_command_handler(message: types.Message, state: FSMContext):
    """Handle /admin command"""
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID and not is_admin_session_valid(user_id):
        await state.set_state(AdminStates.waiting_for_password)
        lang_code = get_user_language(str(message.from_user.id)) 
        password_prompt = t('admin_password_prompt', lang_code)
        cancel_text = t('main_menu', lang_code)
        
        await message.reply(
            password_prompt,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"🔙 {cancel_text}", callback_data="main_menu")]
            ])
        )
        return
    
    lang_code = get_user_language(str(message.from_user.id))
    admin_panel_text = t('admin_panel', lang_code)
    choose_section_text = t('choose_section', lang_code)
    
    await message.reply(
        f"{admin_panel_text}\n\n{choose_section_text}",
        reply_markup=create_admin_keyboard(user_id)
    )

@dp.callback_query(F.data == "admin")
async def admin_handler(callback: CallbackQuery, state: FSMContext):
    """Handle admin panel access"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID and not is_admin_session_valid(user_id):
        await state.set_state(AdminStates.waiting_for_password)
        lang_code = get_user_language(str(callback.from_user.id)) 
        password_prompt = t('admin_password_prompt', lang_code)
        cancel_text = t('main_menu', lang_code)
        
        await callback.message.edit_text(
            password_prompt,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"🔙 {cancel_text}", callback_data="main_menu")]
            ])
        )
        return
    
    lang_code = get_user_language(str(callback.from_user.id))
    admin_panel_text = t('admin_panel', lang_code)
    choose_section_text = t('choose_section', lang_code)
    
    await callback.message.edit_text(
        f"{admin_panel_text}\n\n{choose_section_text}",
        reply_markup=create_admin_keyboard(user_id)
    )

@dp.message(AdminStates.waiting_for_password)
async def admin_password_handler(message: types.Message, state: FSMContext):
    """Handle admin password verification"""
    if message.text == ADMIN_PASSWORD:
        admin_sessions[message.from_user.id] = datetime.now()
        await state.clear()
        lang_code = get_user_language(str(message.from_user.id))
        success_text = t('admin_login_success', lang_code)
        admin_panel_text = t('admin_panel', lang_code)
        
        await message.reply(
            f"{success_text}\n\n{admin_panel_text}:",
            reply_markup=create_admin_keyboard(message.from_user.id)
        )
    else:
        lang_code = get_user_language(str(message.from_user.id))
        failed_text = t('admin_login_failed', lang_code)
        await message.reply(failed_text)

# Enhanced Password change handlers
@dp.callback_query(F.data == "admin_change_password")
async def admin_change_password_handler(callback: CallbackQuery, state: FSMContext):
    """Handle password change request with enhanced security"""
    user_id = callback.from_user.id
    
    # Allow all admins to change password
    if not is_admin_session_valid(user_id):
        await callback.answer("❌ انتهت صلاحية الجلسة", show_alert=True)
        return
    
    # For non-main admins, require current password verification
    if user_id != ADMIN_ID:
        await state.set_state(AdminStates.waiting_for_current_password)
        await callback.message.edit_text(
            "🔐 تغيير كلمة المرور\n\n"
            "للأمان، أدخل كلمة المرور الحالية أولاً:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")
            ]])
        )
    else:
        await state.set_state(AdminStates.waiting_for_new_password)
        await callback.message.edit_text(
            "🔑 تغيير كلمة المرور\n\n"
            "أدخل كلمة المرور الجديدة:\n"
            "📋 المتطلبات:\n"
            "• 8 أحرف على الأقل\n"
            "• يُفضل استخدام أرقام ورموز\n"
            "⚠️ تأكد من حفظها في مكان آمن",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")
            ]])
        )

@dp.message(AdminStates.waiting_for_current_password)
async def process_current_password(message: types.Message, state: FSMContext):
    """Process current password verification"""
    user_id = message.from_user.id
    
    if not is_admin_session_valid(user_id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    current_password = message.text.strip()
    
    if current_password != ADMIN_PASSWORD:
        await message.reply("❌ كلمة المرور الحالية خاطئة")
        return
    
    await state.set_state(AdminStates.waiting_for_new_password)
    await message.reply(
        "✅ تم التحقق من كلمة المرور\n\n"
        "🔑 أدخل كلمة المرور الجديدة:\n"
        "📋 المتطلبات:\n"
        "• 8 أحرف على الأقل\n"
        "• يُفضل استخدام أرقام ورموز\n"
        "⚠️ تأكد من حفظها في مكان آمن",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")
        ]])
    )

@dp.message(AdminStates.waiting_for_new_password)
async def process_new_password(message: types.Message, state: FSMContext):
    """Process new password input with enhanced validation"""
    global ADMIN_PASSWORD
    user_id = message.from_user.id
    
    if not is_admin_session_valid(user_id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    new_password = message.text.strip()
    
    # Enhanced password validation
    if len(new_password) < 8:
        await message.reply("❌ كلمة المرور يجب أن تكون 8 أحرف على الأقل")
        return
    
    if new_password == ADMIN_PASSWORD:
        await message.reply("❌ كلمة المرور الجديدة يجب أن تكون مختلفة عن الحالية")
        return
    
    # Check password strength
    has_digit = any(c.isdigit() for c in new_password)
    has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in new_password)
    
    strength_score = len(new_password) >= 12
    if has_digit:
        strength_score += 1
    if has_special:
        strength_score += 1
    
    strength_text = "ضعيفة" if strength_score == 0 else "متوسطة" if strength_score == 1 else "قوية" if strength_score == 2 else "قوية جداً"
    
    # Update password in environment
    old_password = ADMIN_PASSWORD
    ADMIN_PASSWORD = new_password
    
    # Clear all admin sessions to force re-login
    admin_sessions.clear()
    
    await state.clear()
    
    # Delete the password message for security
    try:
        await message.delete()
    except:
        pass
    
    await message.reply(
        f"✅ تم تغيير كلمة المرور بنجاح!\n\n"
        f"🔐 قوة كلمة المرور: {strength_text}\n"
        f"📝 الطول: {len(new_password)} حرف\n\n"
        f"⚠️ مهم جداً:\n"
        f"• تم حذف الرسالة التي تحتوي على كلمة المرور\n"
        f"• احفظ كلمة المرور الجديدة في مكان آمن\n"
        f"• سيتم تسجيل الخروج من جميع الجلسات\n"
        f"• لجعل التغيير دائم، حدث متغير ADMIN_PASSWORD في ملف .env\n\n"
        f"🔄 سيتم إعادة توجيهك للصفحة الرئيسية...",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🏠 الرئيسية", callback_data="main_menu")
        ]])
    )

@dp.callback_query(F.data == "admin_services")
async def admin_services_handler(callback: CallbackQuery):
    """Handle admin services management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Show loading indicator
    await callback.answer("🔄 جاري تحميل الخدمات...")
    
    db = get_db()
    try:
        services = db.query(Service).filter(Service.active == True).all()
        
        text = "🛠 إدارة الخدمات\n\n"
        if services:
            text += "الخدمات الحالية:\n"
            for service in services:
                status = "✅" if service.active else "❌"
                text += f"{status} {service.emoji} {service.name} - {service.default_price} وحدة\n"
        else:
            text += "لا توجد خدمات مضافة\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة خدمة", callback_data="admin_add_service"),
            InlineKeyboardButton(text="🔗 إدارة الجروبات", callback_data="admin_service_groups")
        )
        keyboard.row(
            InlineKeyboardButton(text="📋 عرض الخدمات", callback_data="admin_list_services"),
            InlineKeyboardButton(text="📊 إحصائيات الرسائل", callback_data="admin_messages_stats")
        )
        keyboard.row(
            InlineKeyboardButton(text="🗑️ إدارة التنظيف التلقائي", callback_data="admin_auto_cleanup")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_users")
async def admin_users_handler(callback: CallbackQuery):
    """Handle admin users management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        users_count = db.query(User).count()
        active_users = db.query(User).filter(User.is_banned == False).count()
        banned_users = db.query(User).filter(User.is_banned == True).count()
        
        text = f"👥 إدارة المستخدمين\n\n"
        text += f"📊 الإحصائيات:\n"
        text += f"• إجمالي المستخدمين: {users_count}\n"
        text += f"• المستخدمين النشطين: {active_users}\n"
        text += f"• المستخدمين المحظورين: {banned_users}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="👤 البحث عن مستخدم", callback_data="admin_search_user"),
            InlineKeyboardButton(text="📋 قائمة المستخدمين", callback_data="admin_list_users")
        )
        keyboard.row(
            InlineKeyboardButton(text="📊 تصدير بيانات المستخدمين", callback_data="admin_export_users"),
            InlineKeyboardButton(text="👥 عرض تفاصيل المستخدمين", callback_data="admin_users_details")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_handler(callback: CallbackQuery, state: FSMContext):
    """Handle admin add balance request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_user_id_balance)
    await state.update_data(action_type="add")
    
    if callback.message:
        await callback.message.edit_text(
            "💰 شحن رصيد مستخدم\n\n"
            "أرسل ID المستخدم (الرقم الطويل) أو @username:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
            ])
        )

@dp.callback_query(F.data == "admin_deduct_balance")
async def admin_deduct_balance_handler(callback: CallbackQuery, state: FSMContext):
    """Handle admin deduct balance request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_user_id_balance)
    await state.update_data(action_type="deduct")
    
    if callback.message:
        await callback.message.edit_text(
            "💳 خصم رصيد مستخدم\n\n"
            "أرسل ID المستخدم (الرقم الطويل) أو @username:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
            ])
        )

@dp.message(AdminStates.waiting_for_user_id_balance)
async def handle_user_id_for_balance(message: types.Message, state: FSMContext):
    """Handle user ID input for balance operations"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    user_input = message.text
    db = get_db()
    
    try:
        target_user = None
        
        # Try to find user by telegram_id or username
        if user_input.startswith('@'):
            username = user_input[1:]  # Remove @
            target_user = db.query(User).filter(User.username == username).first()
        else:
            # Try as telegram_id
            target_user = db.query(User).filter(User.telegram_id == user_input).first()
        
        if not target_user:
            await message.reply(
                "❌ لم يتم العثور على المستخدم\n"
                "تأكد من أن المستخدم قد استخدم البوت من قبل"
            )
            return
        
        data = await state.get_data()
        action_type = data.get("action_type", "add")
        
        # Handle different action types
        if action_type == "search":
            # Display user information
            status = "✅ نشط" if not target_user.is_banned else "❌ محظور"
            admin_status = "👑 أدمن" if target_user.is_admin else "👤 مستخدم عادي"
            
            text = f"👤 معلومات المستخدم\n\n"
            text += f"📝 الاسم: {target_user.first_name or 'غير محدد'}\n"
            text += f"📱 المعرف: @{target_user.username or 'غير محدد'}\n"
            text += f"🆔 الرقم: {target_user.telegram_id}\n"
            text += f"💰 الرصيد: {target_user.balance} وحدة\n"
            text += f"📊 الحالة: {status}\n"
            text += f"👨‍💼 النوع: {admin_status}\n"
            text += f"📅 تاريخ الانضمام: {target_user.joined_at.strftime('%Y-%m-%d')}\n"
            
            # Add action buttons
            keyboard = InlineKeyboardBuilder()
            if not target_user.is_banned:
                keyboard.row(InlineKeyboardButton(text="🚫 حظر المستخدم", callback_data=f"ban_user_{target_user.id}"))
            else:
                keyboard.row(InlineKeyboardButton(text="✅ إلغاء الحظر", callback_data=f"unban_user_{target_user.id}"))
            
            keyboard.row(
                InlineKeyboardButton(text="💰 شحن رصيد", callback_data=f"quick_add_balance_{target_user.id}"),
                InlineKeyboardButton(text="💳 خصم رصيد", callback_data=f"quick_deduct_balance_{target_user.id}")
            )
            keyboard.row(InlineKeyboardButton(text="🔙 إدارة المستخدمين", callback_data="admin_users"))
            
            await message.reply(text, reply_markup=keyboard.as_markup())
            await state.clear()
            return
            
        elif action_type == "private_message":
            # Store user for private message
            await state.update_data(target_user_id=target_user.id)
            await state.set_state(AdminStates.waiting_for_broadcast_message)  # Reuse this state
            await state.update_data(is_private=True)
            
            await message.reply(
                f"💬 إرسال رسالة خاصة\n\n"
                f"👤 إلى: {target_user.first_name or target_user.username or target_user.telegram_id}\n\n"
                f"أرسل الرسالة:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
                ])
            )
            return
        
        # Balance operations
        await state.update_data(target_user_id=target_user.id)
        await state.set_state(AdminStates.waiting_for_balance_amount)
        
        action_text = "شحن" if action_type == "add" else "خصم"
        emoji = "💰" if action_type == "add" else "💳"
        
        await message.reply(
            f"{emoji} {action_text} رصيد\n\n"
            f"👤 المستخدم: {target_user.first_name or target_user.username or target_user.telegram_id}\n"
            f"💰 رصيده الحالي: {target_user.balance} وحدة\n\n"
            f"أرسل المبلغ المراد {action_text}ه:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
            ])
        )
        
    finally:
        db.close()

@dp.message(AdminStates.waiting_for_balance_amount)
async def handle_balance_amount(message: types.Message, state: FSMContext):
    """Handle balance amount input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    try:
        amount = float(message.text)
        if amount <= 0:
            await message.reply("❌ المبلغ يجب أن يكون أكبر من الصفر")
            return
            
    except ValueError:
        await message.reply("❌ الرجاء إدخال رقم صحيح")
        return
    
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    action_type = data.get("action_type", "add")
    
    db = get_db()
    try:
        target_user = db.query(User).filter(User.id == target_user_id).first()
        if not target_user:
            await message.reply("❌ حدث خطأ، لم يتم العثور على المستخدم")
            await state.clear()
            return
        
        old_balance = float(target_user.balance)
        
        if action_type == "add":
            target_user.balance = old_balance + amount
            transaction_type = TransactionType.ADD
            transaction_reason = f"شحن رصيد بواسطة الأدمن"
            emoji = "💰"
            action_text = "شحن"
        else:
            if old_balance < amount:
                await message.reply(
                    f"❌ رصيد المستخدم غير كافي للخصم\n"
                    f"الرصيد الحالي: {old_balance} وحدة\n"
                    f"المبلغ المطلوب خصمه: {amount} وحدة"
                )
                return
            
            target_user.balance = old_balance - amount
            transaction_type = TransactionType.DEDUCT
            transaction_reason = f"خصم رصيد بواسطة الأدمن"
            emoji = "💳"
            action_text = "خصم"
        
        # Create transaction record
        transaction = Transaction(
            user_id=target_user.id,
            type=transaction_type,
            amount=amount,
            reason=transaction_reason
        )
        db.add(transaction)
        
        db.commit()
        
        new_balance = float(target_user.balance)
        
        # Send success message
        await message.reply(
            f"✅ تم {action_text} الرصيد بنجاح!\n\n"
            f"👤 المستخدم: {target_user.first_name or target_user.username or target_user.telegram_id}\n"
            f"{emoji} المبلغ: {amount} وحدة\n"
            f"💰 الرصيد السابق: {old_balance} وحدة\n"
            f"💰 الرصيد الجديد: {new_balance} وحدة",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin")]
            ])
        )
        
        # Notify the user about balance change
        try:
            await bot.send_message(
                target_user.telegram_id,
                f"{emoji} تم {action_text} رصيدك!\n\n"
                f"💰 المبلغ: {amount} وحدة\n"
                f"💰 رصيدك الجديد: {new_balance} وحدة"
            )
        except Exception as e:
            logger.error(f"Failed to notify user about balance change: {e}")
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Error processing balance operation: {e}")
        await message.reply("❌ حدث خطأ أثناء معالجة العملية")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_inventory")
async def admin_inventory_handler(callback: CallbackQuery):
    """Handle admin inventory management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get inventory statistics
        total_numbers = db.query(Number).count()
        available_numbers = db.query(Number).filter(Number.status == 'AVAILABLE').count()
        reserved_numbers = db.query(Number).filter(Number.status == 'RESERVED').count()
        used_numbers = db.query(Number).filter(Number.status == 'USED').count()
        
        # Get numbers by service
        services = db.query(Service).filter(Service.active == True).all()
        
        text = f"📦 إدارة المخزون\n\n"
        text += f"📊 الإحصائيات العامة:\n"
        text += f"• إجمالي الأرقام: {total_numbers}\n"
        text += f"• ✅ متاحة: {available_numbers}\n"
        text += f"• 🔒 محجوزة: {reserved_numbers}\n"
        text += f"• ❌ مستخدمة: {used_numbers}\n\n"
        
        text += f"📱 الأرقام حسب الخدمة:\n"
        for service in services:
            service_total = db.query(Number).filter(Number.service_id == service.id).count()
            service_available = db.query(Number).filter(
                Number.service_id == service.id,
                Number.status == 'AVAILABLE'
            ).count()
            
            text += f"{service.emoji} {service.name}: {service_available}/{service_total}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="📊 تفاصيل الخدمات", callback_data="admin_inventory_services"),
            InlineKeyboardButton(text="🌍 تفاصيل الدول", callback_data="admin_inventory_countries")
        )
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة أرقام", callback_data="admin_add_numbers"),
            InlineKeyboardButton(text="🗑 تنظيف الأرقام", callback_data="admin_cleanup_numbers")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        if callback.message:
            await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_inventory_services")
async def admin_inventory_services_handler(callback: CallbackQuery):
    """Handle admin inventory by services"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        services = db.query(Service).filter(Service.active == True).all()
        
        text = f"📊 تفاصيل المخزون حسب الخدمات\n\n"
        
        for service in services[:5]:  # Limit to first 5 services for better performance
            text += f"{service.emoji} {service.name}:\n"
            
            # Get countries for this service with limit
            countries = db.query(ServiceCountry).filter(
                ServiceCountry.service_id == service.id,
                ServiceCountry.active == True
            ).limit(5).all()  # Limit countries per service
            
            for country in countries:
                available_count = db.query(Number).filter(
                    Number.service_id == service.id,
                    Number.country_code == country.country_code,
                    Number.status == 'AVAILABLE'
                ).count()
                
                total_count = db.query(Number).filter(
                    Number.service_id == service.id,
                    Number.country_code == country.country_code
                ).count()
                
                status = "✅" if available_count > 0 else "❌"
                text += f"  {country.flag} {country.country_name}: {status} {available_count}/{total_count}\n"
            
            text += "\n"
        
        if len(services) > 5:
            text += f"... وعرض {len(services) - 5} خدمة أخرى (للأداء الأفضل)\n\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 المخزون", callback_data="admin_inventory"))
        
        if callback.message:
            await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_inventory_countries")
async def admin_inventory_countries_handler(callback: CallbackQuery):
    """Handle admin inventory by countries"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get all countries with their total numbers
        countries_data = db.query(ServiceCountry.country_name, ServiceCountry.country_code, ServiceCountry.flag).distinct().all()
        
        text = f"🌍 تفاصيل المخزون حسب الدول\n\n"
        
        for country_name, country_code, flag in countries_data:
            total_numbers = db.query(Number).filter(Number.country_code == country_code).count()
            available_numbers = db.query(Number).filter(
                Number.country_code == country_code,
                Number.status == 'AVAILABLE'
            ).count()
            
            status = "✅" if available_numbers > 0 else "❌"
            text += f"{flag} {country_name} ({country_code}): {status} {available_numbers}/{total_numbers}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 المخزون", callback_data="admin_inventory"))
        
        if callback.message:
            await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_numbers")
async def admin_numbers_handler(callback: CallbackQuery):
    """Handle admin numbers management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Show loading indicator
    await callback.answer("🔄 جاري تحميل إحصائيات الأرقام...")
    
    db = get_db()
    try:
        # Get number statistics
        total_numbers = db.query(Number).count()
        available_numbers = db.query(Number).filter(Number.status == 'AVAILABLE').count()
        reserved_numbers = db.query(Number).filter(Number.status == 'RESERVED').count()
        used_numbers = db.query(Number).filter(Number.status == 'USED').count()
        
        text = f"📱 إدارة الأرقام\n\n"
        text += f"📊 الإحصائيات:\n"
        text += f"• إجمالي الأرقام: {total_numbers}\n"
        text += f"• متاحة: {available_numbers}\n"
        text += f"• محجوزة: {reserved_numbers}\n"
        text += f"• مستخدمة: {used_numbers}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة أرقام", callback_data="admin_add_numbers"),
            InlineKeyboardButton(text="📋 عرض الأرقام", callback_data="admin_list_numbers")
        )
        keyboard.row(
            InlineKeyboardButton(text="🗑 تنظيف الأرقام", callback_data="admin_cleanup_menu"),
            InlineKeyboardButton(text="📊 إحصائيات تفصيلية", callback_data="admin_inventory")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_channels")
async def admin_channels_handler(callback: CallbackQuery):
    """Handle admin channels management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        channels = db.query(Channel).all()
        
        text = "📢 إدارة القنوات\n\n"
        if channels:
            text += "القنوات الحالية:\n"
            for channel in channels:
                status = "✅" if channel.active else "❌"
                text += f"{status} {channel.title} - {channel.reward_amount} وحدة\n"
                text += f"   🔗 {channel.username_or_link}\n\n"
        else:
            text += "لا توجد قنوات مضافة\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة قناة", callback_data="admin_add_channel"),
            InlineKeyboardButton(text="📋 عرض القنوات", callback_data="admin_list_channels")
        )
        if channels:
            keyboard.row(
                InlineKeyboardButton(text="🗑 حذف قناة", callback_data="admin_delete_channel"),
                InlineKeyboardButton(text="👥 إدارة الجروبات", callback_data="admin_groups")
            )
        else:
            keyboard.row(InlineKeyboardButton(text="👥 إدارة الجروبات", callback_data="admin_groups"))
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_cleanup_numbers")
async def admin_cleanup_numbers_handler(callback: CallbackQuery):
    """Cleanup old used numbers"""
    try:
        logger.info(f"Cleanup numbers handler called by user: {callback.from_user.id}")
        
        if not is_admin_session_valid(callback.from_user.id):
            await callback.answer("❌ انتهت صلاحية الجلسة")
            return
        
        db = get_db()
        try:
            # Strategic cleanup - Delete used numbers older than 3 days
            used_cutoff_date = datetime.now() - timedelta(days=3)
            deleted_used = db.query(Number).filter(
                Number.status == 'USED',
                Number.code_received_at < used_cutoff_date
            ).delete()
            
            # Delete numbers that have been used 3+ times
            deleted_overused = db.query(Number).filter(
                Number.usage_count >= 3
            ).delete()
            
            # Delete orphaned numbers (no reservations, older than 2 days)
            cutoff_date = datetime.now() - timedelta(days=2)
            orphaned_numbers = db.query(Number).filter(
                Number.reserved_by_user_id.is_(None),
                Number.reserved_at.is_(None),
                Number.status == 'AVAILABLE',
                Number.id.notin_(
                    db.query(Reservation.number_id).filter(
                        Reservation.created_at > cutoff_date
                    ).distinct()
                )
            ).delete()
            
            deleted_count = deleted_used + deleted_overused + orphaned_numbers
            
            # Reset ALL expired reservations
            expired_reservations = db.query(Reservation).filter(
                Reservation.status == ReservationStatus.WAITING_CODE,
                Reservation.expired_at < datetime.now()
            ).all()
            
            reset_count = 0
            for reservation in expired_reservations:
                # Reset number status only if it hasn't received a code
                number = db.query(Number).filter(Number.id == reservation.number_id).first()
                if number and number.status != 'USED':  # Don't reset numbers that received codes
                    number.status = 'AVAILABLE'
                    number.reserved_by_user_id = None
                    number.reserved_at = None
                    number.expires_at = None
                    reset_count += 1
                
                # Update reservation status
                reservation.status = ReservationStatus.EXPIRED
            
            db.commit()
            logger.info(f"Cleanup successful: deleted {deleted_count} numbers, reset {reset_count} reservations")
            
            await callback.answer(
                f"✅ تنظيف حازم مكتمل!\n🗑️ حُذف {deleted_count} رقم\n🔄 أُعيد تعيين {reset_count} حجز",
                show_alert=True
            )
            
            # Refresh the numbers page
            await admin_numbers_handler(callback)
            
        except Exception as e:
            logger.error(f"Error cleaning up numbers: {e}")
            await callback.answer("❌ حدث خطأ أثناء التنظيف", show_alert=True)
            db.rollback()
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Critical error in cleanup numbers handler: {e}")
        await callback.answer("❌ حدث خطأ في النظام")

@dp.callback_query(F.data == "admin_cleanup_menu")
async def admin_cleanup_menu_handler(callback: CallbackQuery):
    """Show cleanup options menu"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    lang_code = get_user_language(str(callback.from_user.id))
    
    db = get_db()
    try:
        # Get unique service-country combinations with number counts
        combinations = db.query(
            Service.id, Service.name, Service.emoji,
            ServiceCountry.country_name, ServiceCountry.country_code, ServiceCountry.flag
        ).join(
            ServiceCountry, Service.id == ServiceCountry.service_id
        ).join(
            Number, and_(
                Number.service_id == Service.id,
                Number.country_code == ServiceCountry.country_code
            )
        ).filter(
            Service.active == True,
            ServiceCountry.active == True
        ).distinct().all()
        
        if not combinations:
            await callback.answer("❌ لا توجد أرقام للتنظيف")
            return
        
        text = await translator.translate_text("🗑 اختر ما تريد تنظيفه:", lang_code)
        text += "\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        # Add service-country combinations
        for service_id, service_name, emoji, country_name, country_code, flag in combinations[:20]:  # Limit to 20 for performance
            # Count numbers for this combination
            used_count = db.query(Number).filter(
                Number.service_id == service_id,
                Number.country_code == country_code,
                Number.status == 'USED'
            ).count()
            
            if used_count > 0:
                text += f"{emoji} {flag} {await get_text(service_name, lang_code)} - {country_name}: {used_count} رقم مستخدم\n"
                
                button_text = f"{emoji} {flag} {await get_text(service_name, lang_code)[:10]}"
                callback_data = f"cleanup_{service_id}_{country_code}"
                keyboard.row(InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        # Add general cleanup options
        keyboard.row(
            InlineKeyboardButton(text="🗑 تنظيف شامل (الكل)", callback_data="admin_cleanup_all"),
            InlineKeyboardButton(text="⏰ تنظيف المنتهية فقط", callback_data="admin_cleanup_expired")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الأرقام", callback_data="admin_numbers"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("cleanup_"))
async def admin_cleanup_specific_handler(callback: CallbackQuery):
    """Handle specific service-country cleanup"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Parse callback data: cleanup_service_id_country_code
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ خطأ في البيانات")
        return
    
    service_id = int(parts[1])
    country_code = parts[2]
    
    lang_code = get_user_language(str(callback.from_user.id))
    
    db = get_db()
    try:
        # Get service and country info
        service = db.query(Service).filter(Service.id == service_id).first()
        country = db.query(ServiceCountry).filter(
            ServiceCountry.service_id == service_id,
            ServiceCountry.country_code == country_code
        ).first()
        
        if not service or not country:
            await callback.answer("❌ البيانات غير صحيحة")
            return
        
        # Delete used numbers older than 7 days for this specific combination
        cutoff_date = datetime.now() - timedelta(days=7)
        
        deleted_count = db.query(Number).filter(
            Number.service_id == service_id,
            Number.country_code == country_code,
            Number.status == 'USED',
            Number.code_received_at < cutoff_date
        ).delete()
        
        # Reset expired reservations for this combination
        expired_reservations = db.query(Reservation).join(Number).filter(
            Number.service_id == service_id,
            Number.country_code == country_code,
            Reservation.status == ReservationStatus.WAITING_CODE,
            Reservation.expired_at < datetime.now()
        ).all()
        
        reset_count = 0
        for reservation in expired_reservations:
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if number:
                number.status = 'AVAILABLE'
                number.reserved_by_user_id = None
                number.reserved_at = None
                number.expires_at = None
                reset_count += 1
            reservation.status = ReservationStatus.EXPIRED
        
        db.commit()
        
        service_name = await get_text(service.name, lang_code)
        success_msg = await translator.translate_text(
            f"✅ تم تنظيف {service_name} - {country.country_name}\n"
            f"🗑 حذف: {deleted_count} رقم قديم\n"
            f"🔄 إعادة تعيين: {reset_count} حجز منتهي",
            lang_code
        )
        
        await callback.answer(success_msg, show_alert=True)
        
        # Return to cleanup menu
        await admin_cleanup_menu_handler(callback)
        
    except Exception as e:
        logger.error(f"Error in specific cleanup: {e}")
        await callback.answer("❌ حدث خطأ أثناء التنظيف")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_cleanup_all")
async def admin_cleanup_all_handler(callback: CallbackQuery):
    """Handle complete cleanup (original functionality)"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Call the original cleanup function
    await admin_cleanup_numbers_handler(callback)

@dp.callback_query(F.data == "admin_cleanup_expired")
async def admin_cleanup_expired_handler(callback: CallbackQuery):
    """Handle cleanup of only expired reservations"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    lang_code = get_user_language(str(callback.from_user.id))
    
    db = get_db()
    try:
        # Reset expired reservations only
        expired_reservations = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.WAITING_CODE,
            Reservation.expired_at < datetime.now()
        ).all()
        
        reset_count = 0
        for reservation in expired_reservations:
            number = db.query(Number).filter(Number.id == reservation.number_id).first()
            if number:
                number.status = 'AVAILABLE'
                number.reserved_by_user_id = None
                number.reserved_at = None
                number.expires_at = None
                reset_count += 1
            reservation.status = ReservationStatus.EXPIRED
        
        db.commit()
        
        success_msg = await translator.translate_text(
            f"✅ تم إعادة تعيين {reset_count} حجز منتهي الصلاحية فقط",
            lang_code
        )
        
        await callback.answer(success_msg, show_alert=True)
        
        # Return to cleanup menu
        await admin_cleanup_menu_handler(callback)
        
    except Exception as e:
        logger.error(f"Error cleaning expired reservations: {e}")
        await callback.answer("❌ حدث خطأ أثناء التنظيف")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: CallbackQuery):
    """Handle admin statistics"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get general statistics
        total_users = db.query(User).count()
        active_users = db.query(User).filter(User.is_banned == False).count()
        total_services = db.query(Service).count()
        active_services = db.query(Service).filter(Service.active == True).count()
        total_numbers = db.query(Number).count()
        available_numbers = db.query(Number).filter(Number.status == 'AVAILABLE').count()
        total_reservations = db.query(Reservation).count()
        completed_reservations = db.query(Reservation).filter(Reservation.status == ReservationStatus.COMPLETED).count()
        total_channels = db.query(Channel).count()
        
        # Get transaction statistics
        total_transactions = db.query(Transaction).count()
        total_revenue = db.query(Transaction).filter(Transaction.type == TransactionType.PURCHASE).count()
        
        text = f"📊 الإحصائيات العامة\n\n"
        text += f"👥 المستخدمين:\n"
        text += f"• إجمالي: {total_users}\n"
        text += f"• نشط: {active_users}\n\n"
        
        text += f"🛠 الخدمات:\n"
        text += f"• إجمالي: {total_services}\n"
        text += f"• نشط: {active_services}\n\n"
        
        text += f"📱 الأرقام:\n"
        text += f"• إجمالي: {total_numbers}\n"
        text += f"• متاح: {available_numbers}\n\n"
        
        text += f"📋 الحجوزات:\n"
        text += f"• إجمالي: {total_reservations}\n"
        text += f"• مكتمل: {completed_reservations}\n\n"
        
        text += f"📢 القنوات: {total_channels}\n"
        text += f"💰 المعاملات: {total_transactions}\n"
        text += f"💳 المبيعات: {total_revenue}\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="📊 إحصائيات الرسائل", callback_data="admin_messages_stats"),
            InlineKeyboardButton(text="🔄 تحديث الآن", callback_data="admin_stats_refresh")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

# Add optimized refresh handler
@dp.callback_query(F.data == "admin_stats_refresh")
async def admin_stats_refresh_handler(callback: CallbackQuery):
    """Handle admin statistics refresh with loading indicator"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Show loading
    await callback.answer("🔄 جاري تحديث الإحصائيات...")
    
    # Call the main stats handler
    await admin_stats_handler(callback)

@dp.callback_query(F.data == "admin_search_user")
async def admin_search_user_handler(callback: CallbackQuery, state: FSMContext):
    """Handle search user request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_user_id_balance)
    await state.update_data(action_type="search")
    
    await callback.message.edit_text(
        "🔍 البحث عن مستخدم\n\n"
        "أرسل ID المستخدم أو @username للبحث:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_users")]
        ])
    )

@dp.callback_query(F.data == "admin_list_users")
async def admin_list_users_handler(callback: CallbackQuery):
    """Handle list users request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Optimize user list query with pagination
        users = db.query(User).order_by(User.joined_at.desc()).limit(10).all()
        
        text = "📋 قائمة المستخدمين (آخر 20)\n\n"
        
        for user in users:
            status = "✅" if not user.is_banned else "❌"
            admin_badge = "👑" if user.is_admin else ""
            username = f"@{user.username}" if user.username else "لا يوجد"
            
            text += f"{status}{admin_badge} {user.first_name or 'بدون اسم'}\n"
            text += f"   🆔 الآيدي: {user.telegram_id}\n"
            text += f"   👤 اليوزر: {username}\n"
            text += f"   💰 الرصيد: {user.balance} وحدة\n"
            text += f"   📅 انضم: {user.joined_at.strftime('%Y-%m-%d')}\n\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة المستخدمين", callback_data="admin_users"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_handler(callback: CallbackQuery, state: FSMContext):
    """Handle broadcast message request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    
    await callback.message.edit_text(
        "📢 إرسال رسالة جماعية\n\n"
        "أرسل الرسالة التي تريد إرسالها لجميع المستخدمين:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
        ])
    )

@dp.message(AdminStates.waiting_for_broadcast_message)
async def handle_broadcast_message(message: types.Message, state: FSMContext):
    """Handle broadcast message input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    broadcast_text = message.text
    data = await state.get_data()
    is_private = data.get("is_private", False)
    
    db = get_db()
    try:
        if is_private:
            # Send private message
            target_user_id = data.get("target_user_id")
            target_user = db.query(User).filter(User.id == target_user_id).first()
            
            if not target_user:
                await message.reply("❌ حدث خطأ، لم يتم العثور على المستخدم")
                await state.clear()
                return
            
            try:
                await bot.send_message(int(target_user.telegram_id), broadcast_text)
                await message.reply(
                    f"✅ تم إرسال الرسالة الخاصة!\n\n"
                    f"👤 إلى: {target_user.first_name or target_user.username or target_user.telegram_id}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin")]
                    ])
                )
            except Exception as e:
                logger.error(f"Failed to send private message to {target_user.telegram_id}: {e}")
                await message.reply("❌ فشل في إرسال الرسالة")
        else:
            # Send broadcast message
            users = db.query(User).filter(User.is_banned == False).all()
            
            sent_count = 0
            failed_count = 0
            
            await message.reply(f"⏳ بدء إرسال الرسالة إلى {len(users)} مستخدم...")
            
            for user in users:
                try:
                    await bot.send_message(int(user.telegram_id), broadcast_text)
                    sent_count += 1
                except Exception as e:
                    logger.error(f"Failed to send broadcast to {user.telegram_id}: {e}")
                    failed_count += 1
            
            await message.reply(
                f"✅ تم إرسال الرسالة الجماعية!\n\n"
                f"📤 تم الإرسال إلى: {sent_count} مستخدم\n"
                f"❌ فشل الإرسال إلى: {failed_count} مستخدم",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin")]
                ])
            )
        
        await state.clear()
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_private_message")
async def admin_private_message_handler(callback: CallbackQuery, state: FSMContext):
    """Handle private message request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_user_id_balance)
    await state.update_data(action_type="private_message")
    
    await callback.message.edit_text(
        "💬 إرسال رسالة خاصة\n\n"
        "أرسل ID المستخدم أو @username:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin")]
        ])
    )

@dp.callback_query(F.data == "admin_maintenance")
async def admin_maintenance_handler(callback: CallbackQuery):
    """Handle maintenance mode toggle"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    global maintenance_mode
    
    current_status = "🔴 مفعل" if maintenance_mode else "🟢 معطل"
    new_status = "🟢 معطل" if maintenance_mode else "🔴 مفعل"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(
            text=f"{'🔴 إيقاف الصيانة' if maintenance_mode else '🔧 تفعيل الصيانة'}", 
            callback_data=f"toggle_maintenance_{'off' if maintenance_mode else 'on'}"
        )
    )
    keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
    
    await callback.message.edit_text(
        f"🔧 وضع الصيانة\n\n"
        f"الحالة الحالية: {current_status}\n\n"
        f"في وضع الصيانة، لن يتمكن المستخدمون من استخدام البوت عدا الأدمن.",
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data.startswith("toggle_maintenance_"))
async def toggle_maintenance_handler(callback: CallbackQuery):
    """Toggle maintenance mode"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    global maintenance_mode
    action = callback.data.split("_")[-1]
    
    if action == "on":
        maintenance_mode = True
        save_maintenance_mode(True)
        await callback.answer("🔧 تم تفعيل وضع الصيانة - المستخدمون محظورون الآن", show_alert=True)
    else:
        maintenance_mode = False
        save_maintenance_mode(False)
        await callback.answer("🟢 تم إيقاف وضع الصيانة - المستخدمون يمكنهم الوصول الآن", show_alert=True)
    
    # Refresh the maintenance page
    await admin_maintenance_handler(callback)

@dp.callback_query(F.data == "admin_add_channel")
async def admin_add_channel_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding new channel"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_channel_title)
    await callback.message.edit_text(
        "📢 إضافة قناة جديدة\n\n"
        "أدخل عنوان القناة:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_channels")
        ]])
    )

@dp.callback_query(F.data == "admin_delete_channel")
async def admin_delete_channel_handler(callback: CallbackQuery):
    """Handle delete channel selection"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        channels = db.query(Channel).all()
        
        if not channels:
            await callback.answer("❌ لا توجد قنوات للحذف")
            return
        
        text = "🗑 حذف قناة\n\n"
        text += "اختر القناة التي تريد حذفها:\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for channel in channels:
            status = "✅" if channel.active else "❌"
            keyboard.row(InlineKeyboardButton(
                text=f"{status} {channel.title} ({channel.reward_amount} وحدة)",
                callback_data=f"delete_channel_confirm_{channel.id}"
            ))
        
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_channels"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("delete_channel_confirm_"))
async def delete_channel_confirm_handler(callback: CallbackQuery):
    """Handle channel deletion confirmation"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    channel_id = int(callback.data.split("_")[3])
    
    db = get_db()
    try:
        channel = db.query(Channel).filter(Channel.id == channel_id).first()
        if not channel:
            await callback.answer("❌ القناة غير موجودة")
            return
        
        # Delete all related user rewards
        deleted_rewards = db.query(UserChannelReward).filter(
            UserChannelReward.channel_id == channel_id
        ).delete()
        
        # Delete the channel
        channel_title = channel.title
        db.delete(channel)
        db.commit()
        
        await callback.answer(
            f"✅ تم حذف قناة {channel_title}\n"
            f"🗑 محذوف: {deleted_rewards} مكافأة", 
            show_alert=True
        )
        
        # Go back to channels management
        await admin_channels_handler(callback)
        
    except Exception as e:
        logger.error(f"Error deleting channel: {e}")
        await callback.answer("❌ حدث خطأ أثناء الحذف")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_groups")
async def admin_groups_handler(callback: CallbackQuery):
    """Handle admin groups management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        groups = db.query(Group).all()
        
        text = "👥 إدارة الجروبات\n\n"
        if groups:
            text += "الجروبات الحالية:\n"
            for group in groups:
                status = "✅" if group.active else "❌"
                text += f"{status} {group.title} - {group.reward_amount} وحدة\n"
                text += f"   🔗 {group.username_or_link}\n"
                text += f"   🆔 {group.group_id}\n\n"
        else:
            text += "لا توجد جروبات مضافة\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة جروب", callback_data="admin_add_group"),
            InlineKeyboardButton(text="📋 عرض الجروبات", callback_data="admin_list_groups")
        )
        if groups:
            keyboard.row(InlineKeyboardButton(text="🗑 حذف جروب", callback_data="admin_delete_group"))
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_channels"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_delete_group")
async def admin_delete_group_handler(callback: CallbackQuery):
    """Handle delete group selection"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        groups = db.query(Group).all()
        
        if not groups:
            await callback.answer("❌ لا توجد جروبات للحذف")
            return
        
        text = "🗑 حذف جروب\n\n"
        text += "اختر الجروب الذي تريد حذفه:\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for group in groups:
            status = "✅" if group.active else "❌"
            keyboard.row(InlineKeyboardButton(
                text=f"{status} {group.title} ({group.reward_amount} وحدة)",
                callback_data=f"delete_group_confirm_{group.id}"
            ))
        
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الجروبات", callback_data="admin_groups"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("delete_group_confirm_"))
async def delete_group_confirm_handler(callback: CallbackQuery):
    """Handle group deletion confirmation"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    group_id = int(callback.data.split("_")[3])
    
    db = get_db()
    try:
        group = db.query(Group).filter(Group.id == group_id).first()
        if not group:
            await callback.answer("❌ الجروب غير موجود")
            return
        
        # Delete all related user rewards
        deleted_rewards = db.query(UserGroupReward).filter(
            UserGroupReward.group_id == group_id
        ).delete()
        
        # Delete the group
        group_title = group.title
        db.delete(group)
        db.commit()
        
        await callback.answer(
            f"✅ تم حذف جروب {group_title}\n"
            f"🗑 محذوف: {deleted_rewards} مكافأة", 
            show_alert=True
        )
        
        # Go back to groups management
        await admin_groups_handler(callback)
        
    except Exception as e:
        logger.error(f"Error deleting group: {e}")
        await callback.answer("❌ حدث خطأ أثناء الحذف")
        db.rollback()
    finally:
        db.close()

@dp.callback_query(F.data == "admin_list_channels")
async def admin_list_channels_handler(callback: CallbackQuery):
    """Handle list channels request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        channels = db.query(Channel).all()
        
        text = "📋 قائمة القنوات\n\n"
        
        if channels:
            for channel in channels:
                status = "✅" if channel.active else "❌"
                text += f"{status} {channel.title}\n"
                text += f"   💰 المكافأة: {channel.reward_amount} وحدة\n"
                text += f"   🔗 {channel.username_or_link}\n"
                
                # Check if bot is in the channel
                try:
                    channel_username = channel.username_or_link
                    if channel_username.startswith('https://t.me/'):
                        channel_username = '@' + channel_username.split('/')[-1]
                    elif not channel_username.startswith('@'):
                        channel_username = '@' + channel_username
                    
                    bot_member = await bot.get_chat_member(channel_username, bot.id)
                    if bot_member.status in ['administrator', 'member']:
                        text += f"   🤖 البوت: متواجد\n"
                    else:
                        text += f"   🤖 البوت: غير متواجد ❌\n"
                except:
                    text += f"   🤖 البوت: غير معروف ❓\n"
                
                text += "\n"
        else:
            text += "لا توجد قنوات مضافة"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_channels"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_list_services")
async def admin_list_services_handler(callback: CallbackQuery):
    """Handle list services with delete/disable options"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    # Show loading indicator
    await callback.answer("🔄 جاري تحميل قائمة الخدمات...")
    
    db = get_db()
    try:
        services = db.query(Service).filter(Service.active == True).all()
        
        text = "📋 قائمة الخدمات\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for service in services:
            status = "✅" if service.active else "❌"
            text += f"{status} {service.emoji} {service.name} - {service.default_price} وحدة\n"
            
            # Add buttons for each service
            toggle_text = "❌ إيقاف" if service.active else "✅ تفعيل"
            keyboard.row(
                InlineKeyboardButton(text=f"{toggle_text} {service.name}", callback_data=f"toggle_service_{service.id}"),
                InlineKeyboardButton(text=f"✏️ تعديل {service.name}", callback_data=f"edit_service_{service.id}")
            )
            keyboard.row(
                InlineKeyboardButton(text=f"🗑 حذف {service.name}", callback_data=f"delete_service_{service.id}")
            )
        
        text += "\n📝 اختر الإجراء المطلوب للخدمة:"
        
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الخدمات", callback_data="admin_services"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("toggle_service_"))
async def toggle_service_handler(callback: CallbackQuery):
    """Toggle service active status"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await callback.answer("❌ الخدمة غير موجودة")
            return
        
        service.active = not service.active
        db.commit()
        
        status_text = "تفعيل" if service.active else "إيقاف"
        await callback.answer(f"✅ تم {status_text} خدمة {service.name}")
        
        # Refresh the services list
        await admin_list_services_handler(callback)
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("delete_service_"))
async def delete_service_handler(callback: CallbackQuery):
    """Delete service immediately without confirmation"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await callback.answer("❌ الخدمة غير موجودة")
            return
        
        service_name = service.name
        
        # Delete all related data to avoid foreign key constraints
        
        # Delete provider messages and blocked messages first
        db.query(ProviderMessage).filter(ProviderMessage.service_id == service_id).delete()
        db.query(BlockedMessage).filter(BlockedMessage.service_id == service_id).delete()
        
        # Cancel all active reservations
        deleted_reservations = db.query(Reservation).filter(
            Reservation.service_id == service_id,
            Reservation.status == ReservationStatus.ACTIVE
        ).update({'status': ReservationStatus.CANCELLED})
        
        # Mark all numbers for this service as deleted 
        deleted_numbers = db.query(Number).filter(
            Number.service_id == service_id
        ).update({'status': 'DELETED'})
        
        # Mark service and related entries as inactive
        service.active = False
        db.query(ServiceCountry).filter(ServiceCountry.service_id == service_id).update(
            {'active': False}
        )
        
        db.commit()
        
        # Show success message with what was deleted
        if deleted_numbers > 0 or deleted_reservations > 0:
            await callback.answer(
                f"✅ تم حذف خدمة {service_name}\n"
                f"🗑 محذوف: {deleted_numbers} رقم، {deleted_reservations} حجز", 
                show_alert=True
            )
        else:
            await callback.answer(f"✅ تم حذف خدمة {service_name}", show_alert=True)
        
        # Go back to services list with fresh page to avoid message editing issues
        try:
            await admin_list_services_handler(callback)
        except Exception as edit_error:
            # If message editing fails, send a new message
            logger.warning(f"Message edit failed, sending new message: {edit_error}")
            keyboard = InlineKeyboardBuilder()
            keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
            await callback.message.answer("✅ تم الحذف بنجاح", reply_markup=keyboard.as_markup())
        
    except Exception as e:
        logger.error(f"Error deleting service: {e}")
        await callback.answer("❌ حدث خطأ أثناء الحذف")
        db.rollback()
    finally:
        db.close()


@dp.callback_query(F.data.regexp(r"^edit_service_\d+$"))
async def edit_service_handler(callback: CallbackQuery):
    """Handle service editing"""
    try:
        logger.info(f"Edit service handler called with data: {callback.data}")
        
        if not is_admin_session_valid(callback.from_user.id):
            await callback.answer("❌ انتهت صلاحية الجلسة")
            return
        
        service_id = int(callback.data.split("_")[-1])
        logger.info(f"Editing service ID: {service_id}")
        
        db = get_db()
        try:
            service = db.query(Service).filter(Service.id == service_id).first()
            if not service:
                await callback.answer("❌ الخدمة غير موجودة")
                return
            
            # Show service details with edit options
            text = f"✏️ تعديل الخدمة\n\n"
            text += f"🏷️ الاسم: {service.name}\n"
            text += f"🎨 الإيموجي: {service.emoji}\n"
            text += f"💰 السعر: {service.default_price} وحدة\n"
            text += f"📝 الوصف: {service.description or 'غير محدد'}\n"
            text += f"🔄 الحالة: {'نشط' if service.active else 'غير نشط'}\n\n"
            text += "اختر ما تريد تعديله:"
            
            keyboard = InlineKeyboardBuilder()
            keyboard.row(
                InlineKeyboardButton(text="🏷️ تعديل الاسم", callback_data=f"edit_service_name_{service_id}"),
                InlineKeyboardButton(text="🎨 تعديل الإيموجي", callback_data=f"edit_service_emoji_{service_id}")
            )
            keyboard.row(
                InlineKeyboardButton(text="💰 تعديل السعر", callback_data=f"edit_service_price_{service_id}"),
                InlineKeyboardButton(text="📝 تعديل الوصف", callback_data=f"edit_service_desc_{service_id}")
            )
            keyboard.row(InlineKeyboardButton(text="🔙 قائمة الخدمات", callback_data="admin_list_services"))
            
            await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
            
        except Exception as e:
            logger.error(f"Error in edit service handler: {e}")
            await callback.answer(f"❌ خطأ: {str(e)}")
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Critical error in edit service handler: {e}")
        await callback.answer("❌ حدث خطأ في النظام")

# Edit service property handlers
@dp.callback_query(F.data.startswith("edit_service_name_"))
async def edit_service_name_handler(callback: CallbackQuery, state: FSMContext):
    """Handle edit service name"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_service_id=service_id)
    await state.set_state(AdminStates.waiting_for_edit_service_name)
    
    await callback.message.edit_text(
        "🏷️ أدخل الاسم الجديد للخدمة:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data=f"edit_service_{service_id}")
        ]])
    )

@dp.callback_query(F.data.startswith("edit_service_emoji_"))
async def edit_service_emoji_handler(callback: CallbackQuery, state: FSMContext):
    """Handle edit service emoji"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_service_id=service_id)
    await state.set_state(AdminStates.waiting_for_edit_service_emoji)
    
    await callback.message.edit_text(
        "🎨 أدخل الإيموجي الجديد للخدمة:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data=f"edit_service_{service_id}")
        ]])
    )

@dp.callback_query(F.data.startswith("edit_service_price_"))
async def edit_service_price_handler(callback: CallbackQuery, state: FSMContext):
    """Handle edit service price"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_service_id=service_id)
    await state.set_state(AdminStates.waiting_for_edit_service_price)
    
    await callback.message.edit_text(
        "💰 أدخل السعر الجديد للخدمة (بالوحدات):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data=f"edit_service_{service_id}")
        ]])
    )

@dp.callback_query(F.data.startswith("edit_service_desc_"))
async def edit_service_desc_handler(callback: CallbackQuery, state: FSMContext):
    """Handle edit service description"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    await state.update_data(edit_service_id=service_id)
    await state.set_state(AdminStates.waiting_for_edit_service_description)
    
    await callback.message.edit_text(
        "📝 أدخل الوصف الجديد للخدمة (أو أرسل 'حذف' لحذف الوصف):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data=f"edit_service_{service_id}")
        ]])
    )

# Message handlers for editing service properties
@dp.message(StateFilter(AdminStates.waiting_for_edit_service_name))
async def process_edit_service_name(message: types.Message, state: FSMContext):
    """Process edited service name"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    data = await state.get_data()
    service_id = data.get('edit_service_id')
    new_name = message.text.strip()
    
    if not new_name:
        await message.reply("❌ يرجى إدخال اسم صحيح للخدمة")
        return
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await message.reply("❌ الخدمة غير موجودة")
            return
        
        # Check if name already exists
        existing = db.query(Service).filter(
            Service.name == new_name,
            Service.id != service_id
        ).first()
        
        if existing:
            await message.reply("❌ اسم الخدمة موجود مسبقاً")
            return
        
        old_name = service.name
        service.name = new_name
        db.commit()
        
        await state.clear()
        await message.reply(
            f"✅ تم تغيير اسم الخدمة\n"
            f"من: {old_name}\n"
            f"إلى: {new_name}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 قائمة الخدمات", callback_data="admin_list_services")
            ]])
        )
        
    finally:
        db.close()

@dp.message(StateFilter(AdminStates.waiting_for_edit_service_emoji))
async def process_edit_service_emoji(message: types.Message, state: FSMContext):
    """Process edited service emoji"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    data = await state.get_data()
    service_id = data.get('edit_service_id')
    new_emoji = message.text.strip()
    
    if not new_emoji:
        new_emoji = "📱"  # Default emoji
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await message.reply("❌ الخدمة غير موجودة")
            return
        
        old_emoji = service.emoji
        service.emoji = new_emoji
        db.commit()
        
        await state.clear()
        await message.reply(
            f"✅ تم تغيير إيموجي الخدمة {service.name}\n"
            f"من: {old_emoji}\n"
            f"إلى: {new_emoji}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 قائمة الخدمات", callback_data="admin_list_services")
            ]])
        )
        
    finally:
        db.close()

@dp.message(StateFilter(AdminStates.waiting_for_edit_service_price))
async def process_edit_service_price(message: types.Message, state: FSMContext):
    """Process edited service price"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    data = await state.get_data()
    service_id = data.get('edit_service_id')
    
    try:
        new_price = float(message.text.strip())
        if new_price < 0:
            await message.reply("❌ السعر يجب أن يكون رقم موجب")
            return
    except ValueError:
        await message.reply("❌ يرجى إدخال رقم صحيح للسعر")
        return
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await message.reply("❌ الخدمة غير موجودة")
            return
        
        old_price = service.default_price
        service.default_price = new_price
        db.commit()
        
        await state.clear()
        await message.reply(
            f"✅ تم تغيير سعر الخدمة {service.name}\n"
            f"من: {old_price} وحدة\n"
            f"إلى: {new_price} وحدة",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 قائمة الخدمات", callback_data="admin_list_services")
            ]])
        )
        
    finally:
        db.close()

@dp.message(StateFilter(AdminStates.waiting_for_edit_service_description))
async def process_edit_service_description(message: types.Message, state: FSMContext):
    """Process edited service description"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    data = await state.get_data()
    service_id = data.get('edit_service_id')
    new_description = message.text.strip()
    
    # Allow deletion of description
    if new_description.lower() in ['حذف', 'delete', 'remove']:
        new_description = None
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await message.reply("❌ الخدمة غير موجودة")
            return
        
        old_description = service.description or "غير محدد"
        service.description = new_description
        db.commit()
        
        await state.clear()
        
        new_desc_text = new_description or "تم حذف الوصف"
        await message.reply(
            f"✅ تم تغيير وصف الخدمة {service.name}\n"
            f"من: {old_description}\n"
            f"إلى: {new_desc_text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 قائمة الخدمات", callback_data="admin_list_services")
            ]])
        )
        
    finally:
        db.close()

# Additional handlers for user management actions
@dp.callback_query(F.data.startswith("ban_user_"))
async def ban_user_handler(callback: CallbackQuery):
    """Ban a user"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            await callback.answer("❌ المستخدم غير موجود")
            return
        
        user.is_banned = True
        db.commit()
        
        await callback.answer(f"✅ تم حظر المستخدم {user.first_name or user.username}")
        
        # Notify the user
        try:
            await bot.send_message(int(user.telegram_id), "❌ تم حظرك من استخدام البوت")
        except:
            pass
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("unban_user_"))
async def unban_user_handler(callback: CallbackQuery):
    """Unban a user"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            await callback.answer("❌ المستخدم غير موجود")
            return
        
        user.is_banned = False
        db.commit()
        
        await callback.answer(f"✅ تم إلغاء حظر المستخدم {user.first_name or user.username}")
        
        # Notify the user
        try:
            await bot.send_message(int(user.telegram_id), "✅ تم إلغاء حظرك، يمكنك الآن استخدام البوت")
        except:
            pass
        
    finally:
        db.close()

@dp.callback_query(F.data.startswith("quick_add_balance_"))
async def quick_add_balance_handler(callback: CallbackQuery, state: FSMContext):
    """Quick add balance"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    await state.set_state(AdminStates.waiting_for_balance_amount)
    await state.update_data(action_type="add", target_user_id=user_id)
    
    await callback.message.edit_text(
        "💰 شحن رصيد سريع\n\n"
        "أرسل المبلغ المراد إضافته:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_users")]
        ])
    )

@dp.callback_query(F.data.startswith("quick_deduct_balance_"))
async def quick_deduct_balance_handler(callback: CallbackQuery, state: FSMContext):
    """Quick deduct balance"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    await state.set_state(AdminStates.waiting_for_balance_amount)
    await state.update_data(action_type="deduct", target_user_id=user_id)
    
    await callback.message.edit_text(
        "💳 خصم رصيد سريع\n\n"
        "أرسل المبلغ المراد خصمه:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_users")]
        ])
    )

# Improved group verification for service groups
async def verify_bot_in_group(group_chat_id: str) -> bool:
    """Verify if bot is admin in the group"""
    try:
        # Check if bot is admin in the group
        bot_member = await bot.get_chat_member(str(group_chat_id), bot.id)
        return bot_member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking bot admin status in group {group_chat_id}: {e}")
        return False

@dp.callback_query(F.data == "admin_countries")
async def admin_countries_handler(callback: CallbackQuery):
    """Handle admin countries management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        countries = db.query(Country).all()
        
        text = "🌍 إدارة الدول\n\n"
        
        if countries:
            text += "الدول المتاحة:\n"
            for country in countries:
                text += f"🏳️ {country.name} ({country.code})\n"
        else:
            text += "لا توجد دول مضافة\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="➕ إضافة دولة", callback_data="admin_add_country"),
            InlineKeyboardButton(text="📋 عرض الدول", callback_data="admin_list_countries")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_add_country")
async def admin_add_country_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding new country"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_country_name)
    await callback.message.edit_text(
        "🌍 إضافة دولة جديدة\n\n"
        "أدخل اسم الدولة:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_countries")
        ]])
    )

@dp.callback_query(F.data == "admin_list_countries")
async def admin_list_countries_handler(callback: CallbackQuery):
    """Handle list countries request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        countries = db.query(Country).all()
        
        text = "📋 قائمة الدول\n\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for country in countries:
            text += f"🏳️ {country.name} ({country.code})\n"
            keyboard.row(
                InlineKeyboardButton(text=f"🗑 حذف {country.name}", callback_data=f"delete_country_{country.id}")
            )
        
        if not countries:
            text += "لا توجد دول مضافة"
        
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الدول", callback_data="admin_countries"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_settings")
async def admin_settings_handler(callback: CallbackQuery):
    """Handle admin settings"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    text = "⚙️ إعدادات النظام\n\n"
    text += f"🤖 البوت: نشط\n"
    text += f"🔧 وضع الصيانة: {'مفعل' if maintenance_mode else 'معطل'}\n"
    text += f"👑 أدمن ID: {ADMIN_ID}\n"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔧 تغيير وضع الصيانة", callback_data="admin_maintenance"),
        InlineKeyboardButton(text="📊 إحصائيات النظام", callback_data="admin_stats")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔄 إعادة تشغيل البوت", callback_data="admin_restart_bot"),
        InlineKeyboardButton(text="📄 تصدير البيانات", callback_data="admin_export_data")
    )
    keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup())

# User Data Channel Handlers
@dp.callback_query(F.data == "admin_user_data_channel")
async def admin_user_data_channel_handler(callback: CallbackQuery):
    """Handle user data channel management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get current channel
        channel = db.query(UserDataChannel).filter(
            UserDataChannel.active == True
        ).first()
        
        text = "📋 إدارة قناة بيانات المستخدمين\n\n"
        
        if channel:
            text += f"📢 القناة الحالية:\n"
            text += f"🆔 المعرف: `{channel.channel_id}`\n"
            if channel.channel_username:
                text += f"📝 اليوزر: @{channel.channel_username}\n"
            if channel.channel_title:
                text += f"📝 العنوان: {channel.channel_title}\n"
            text += f"📅 تاريخ الإضافة: {channel.created_at.strftime('%Y-%m-%d')}\n"
        else:
            text += "❌ لا توجد قناة مُعرفة\n"
        
        keyboard = InlineKeyboardBuilder()
        
        if channel:
            keyboard.row(
                InlineKeyboardButton(text="✏️ تعديل القناة", callback_data="edit_user_data_channel"),
                InlineKeyboardButton(text="🗑 حذف القناة", callback_data="delete_user_data_channel")
            )
        else:
            keyboard.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="add_user_data_channel"))
        
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="Markdown")
    
    finally:
        db.close()

# Forced Subscription Handlers  
@dp.callback_query(F.data == "admin_forced_subscription")
async def admin_forced_subscription_handler(callback: CallbackQuery):
    """Handle forced subscription management"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get all forced subscriptions
        subs = db.query(ForcedSubscription).filter(
            ForcedSubscription.active == True
        ).all()
        
        text = "🔒 إدارة الاشتراك الإجباري\n\n"
        
        if subs:
            text += "📢 القنوات المُعرفة:\n\n"
            for sub in subs:
                text += f"🆔 المعرف: `{sub.channel_id}`\n"
                if sub.channel_username:
                    text += f"📝 اليوزر: @{sub.channel_username}\n"
                if sub.channel_title:
                    text += f"📝 العنوان: {sub.channel_title}\n"
                text += f"📅 تاريخ الإضافة: {sub.created_at.strftime('%Y-%m-%d')}\n\n"
        else:
            text += "❌ لا توجد قنوات اشتراك إجباري\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="add_forced_subscription"))
        
        if subs:
            keyboard.row(InlineKeyboardButton(text="📋 إدارة القنوات", callback_data="manage_forced_subscriptions"))
        
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="Markdown")
    
    finally:
        db.close()

# Check subscription handler
@dp.callback_query(F.data == "check_subscription")
async def check_subscription_handler(callback: CallbackQuery):
    """Handle subscription check"""
    user_id = callback.from_user.id
    
    # Check if user is subscribed to all required channels
    is_subscribed = await check_user_subscription(user_id)
    
    if is_subscribed:
        # Remove any pending subscription tasks and proceed
        # Here you can add logic to remove the subscription check and continue with normal flow
        await callback.message.edit_text(
            "✅ تم التحقق من اشتراكك بنجاح!\n\n"
            "يمكنك الآن استخدام البوت بحرية 🎉",
            reply_markup=await create_main_keyboard(str(user_id))
        )
    else:
        await callback.answer("❌ يجب الاشتراك في جميع القنوات المطلوبة أولاً")

@dp.callback_query(F.data == "admin_messages_stats")
async def admin_messages_stats_handler(callback: CallbackQuery):
    """Handle admin messages statistics"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get message statistics from service groups
        service_groups = db.query(ServiceGroup).all()
        
        text = "📊 إحصائيات الرسائل\n\n"
        
        if service_groups:
            text += "📱 حسب الخدمات:\n"
            for sg in service_groups:
                # Count received messages (you can add a messages table to track this)
                text += f"{sg.service.emoji} {sg.service.name}:\n"
                text += f"   📞 جروب: {sg.group_chat_id}\n"
                text += f"   📊 الحالة: {'نشط' if sg.active else 'معطل'}\n\n"
        else:
            text += "لا توجد خدمات مربوطة بجروبات\n"
        
        # Get general message stats
        total_reservations = db.query(Reservation).count()
        completed_reservations = db.query(Reservation).filter(
            Reservation.status == ReservationStatus.COMPLETED
        ).count()
        
        text += f"📈 إحصائيات عامة:\n"
        text += f"• إجمالي الطلبات: {total_reservations}\n"
        text += f"• طلبات مكتملة: {completed_reservations}\n"
        text += f"• معدل النجاح: {(completed_reservations/total_reservations*100):.1f}%" if total_reservations > 0 else "• معدل النجاح: 0%\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="🔄 تحديث", callback_data="admin_messages_stats"),
            InlineKeyboardButton(text="📊 إحصائيات عامة", callback_data="admin_stats")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 لوحة الإدارة", callback_data="admin"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

@dp.callback_query(F.data == "admin_add_numbers")
async def admin_add_numbers_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding new numbers"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        services = db.query(Service).filter(Service.active == True).all()
        
        if not services:
            await callback.answer("❌ لا توجد خدمات نشطة لإضافة أرقام لها")
            return
        
        text = "➕ إضافة أرقام جديدة\n\n"
        text += "اختر الخدمة:\n"
        
        keyboard = InlineKeyboardBuilder()
        
        for service in services:
            keyboard.row(InlineKeyboardButton(
                text=f"{service.emoji} {service.name}",
                callback_data=f"add_numbers_service_{service.id}"
            ))
        
        keyboard.row(InlineKeyboardButton(text="🔙 إدارة الأرقام", callback_data="admin_numbers"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

# Additional handlers for channel management
@dp.message(AdminStates.waiting_for_channel_title)
async def handle_channel_title(message: types.Message, state: FSMContext):
    """Handle channel title input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    channel_title = message.text
    await state.update_data(channel_title=channel_title)
    await state.set_state(AdminStates.waiting_for_channel_username)
    
    await message.reply(
        f"📢 إضافة قناة: {channel_title}\n\n"
        "أدخل معرف القناة أو رابطها:\n"
        "مثال: @channel_name أو https://t.me/channel_name",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_channels")
        ]])
    )

@dp.message(AdminStates.waiting_for_channel_username)
async def handle_channel_username(message: types.Message, state: FSMContext):
    """Handle channel username input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    channel_username = message.text
    await state.update_data(channel_username=channel_username)
    await state.set_state(AdminStates.waiting_for_channel_reward)
    
    await message.reply(
        f"💰 مكافأة القناة\n\n"
        f"أدخل مقدار المكافأة بالوحدات:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_channels")
        ]])
    )

@dp.message(AdminStates.waiting_for_channel_reward)
async def handle_channel_reward(message: types.Message, state: FSMContext):
    """Handle channel reward input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    try:
        reward_amount = int(message.text)
        if reward_amount <= 0:
            await message.reply("❌ يجب أن تكون المكافأة أكبر من 0")
            return
        
        data = await state.get_data()
        channel_title = data.get('channel_title')
        channel_username = data.get('channel_username')
        
        # Add channel to database
        db = get_db()
        try:
            new_channel = Channel(
                title=channel_title,
                username_or_link=channel_username,
                reward_amount=reward_amount,
                active=True
            )
            db.add(new_channel)
            db.commit()
            
            await message.reply(
                f"✅ تم إضافة القناة بنجاح!\n\n"
                f"📢 العنوان: {channel_title}\n"
                f"🔗 الرابط: {channel_username}\n"
                f"💰 المكافأة: {reward_amount} وحدة",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_channels")
                ]])
            )
            
        finally:
            db.close()
            
        await state.clear()
        
    except ValueError:
        await message.reply("❌ يرجى إدخال رقم صحيح للمكافأة")

# Country management handlers
@dp.message(AdminStates.waiting_for_country_name)
async def handle_country_name(message: types.Message, state: FSMContext):
    """Handle country name input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    country_name = message.text
    await state.update_data(country_name=country_name)
    await state.set_state(AdminStates.waiting_for_country_code)
    
    await message.reply(
        f"🌍 إضافة دولة: {country_name}\n\n"
        "أدخل رمز الدولة (مثال: SA, EG, AE):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_countries")
        ]])
    )

@dp.message(AdminStates.waiting_for_country_code)
async def handle_country_code(message: types.Message, state: FSMContext):
    """Handle country code input"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    country_code = message.text.upper()
    
    if len(country_code) != 2:
        await message.reply("❌ رمز الدولة يجب أن يكون حرفين فقط")
        return
    
    data = await state.get_data()
    country_name = data.get('country_name')
    
    db = get_db()
    try:
        # Check if country already exists
        existing = db.query(Country).filter(
            (Country.name == country_name) | (Country.code == country_code)
        ).first()
        
        if existing:
            await message.reply("❌ الدولة موجودة بالفعل")
            return
        
        # Add new country
        new_country = Country(
            name=country_name,
            code=country_code
        )
        db.add(new_country)
        db.commit()
        
        await message.reply(
            f"✅ تم إضافة الدولة بنجاح!\n\n"
            f"🏳️ الاسم: {country_name}\n"
            f"🔤 الرمز: {country_code}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إدارة الدول", callback_data="admin_countries")
            ]])
        )
        
    finally:
        db.close()
    
    await state.clear()

# Additional settings handlers
@dp.callback_query(F.data == "admin_restart_bot")
async def admin_restart_bot_handler(callback: CallbackQuery):
    """Handle bot restart request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await callback.answer("🔄 إعادة تشغيل البوت...")
    await callback.message.edit_text(
        "🔄 جاري إعادة تشغيل البوت...\n\n"
        "سيتم إعادة تشغيل البوت خلال ثوانٍ"
    )
    
    # Exit the application (systemd or process manager will restart it)
    import sys
    sys.exit(0)

@dp.callback_query(F.data == "admin_export_data")
async def admin_export_data_handler(callback: CallbackQuery):
    """Handle data export request"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Get basic statistics for export summary
        users_count = db.query(User).count()
        services_count = db.query(Service).count()
        numbers_count = db.query(Number).count()
        reservations_count = db.query(Reservation).count()
        
        text = f"📄 تصدير البيانات\n\n"
        text += f"📊 ملخص البيانات:\n"
        text += f"• المستخدمين: {users_count}\n"
        text += f"• الخدمات: {services_count}\n"
        text += f"• الأرقام: {numbers_count}\n"
        text += f"• الحجوزات: {reservations_count}\n\n"
        text += f"💾 يمكنك تصدير البيانات كملف CSV"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="👥 تصدير المستخدمين", callback_data="export_users"),
            InlineKeyboardButton(text="📱 تصدير الأرقام", callback_data="export_numbers")
        )
        keyboard.row(
            InlineKeyboardButton(text="📋 تصدير الحجوزات", callback_data="export_reservations"),
            InlineKeyboardButton(text="💰 تصدير المعاملات", callback_data="export_transactions")
        )
        keyboard.row(InlineKeyboardButton(text="🔙 الإعدادات", callback_data="admin_settings"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
        
    finally:
        db.close()

# Additional handlers for adding numbers
@dp.callback_query(F.data.startswith("add_numbers_service_"))
async def add_numbers_service_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding numbers for specific service"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    service_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            await callback.answer("❌ الخدمة غير موجودة")
            return
        
        await state.update_data(service_id=service_id)
        await state.set_state(AdminStates.waiting_for_numbers_input)
        
        await callback.message.edit_text(
            f"➕ إضافة أرقام لخدمة {service.emoji} {service.name}\n\n"
            f"📝 **طريقة 1: إدخال مباشر**\n"
            f"أدخل الأرقام (رقم واحد في كل سطر):\n"
            f"مثال:\n"
            f"+966501234567\n"
            f"+966507654321\n\n"
            f"📁 **طريقة 2: رفع ملف**\n"
            f"ارفع ملف .txt أو .csv يحتوي على الأرقام\n"
            f"(رقم واحد في كل سطر)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_add_numbers")
            ]])
        )
        
    finally:
        db.close()

@dp.message(AdminStates.waiting_for_numbers_input)
async def handle_numbers_input(message: types.Message, state: FSMContext):
    """Handle numbers input for adding - supports both text and file uploads"""
    if not message.from_user or not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        await state.clear()
        return
    
    numbers = []
    
    # Check if it's a file upload
    if message.document:
        # Handle file upload
        file_name = message.document.file_name.lower()
        if not (file_name.endswith('.txt') or file_name.endswith('.csv')):
            await message.reply("❌ نوع الملف غير مدعوم. يرجى رفع ملف .txt أو .csv")
            return
        
        try:
            # Download the file
            file = await bot.get_file(message.document.file_id)
            file_content = await bot.download_file(file.file_path)
            
            # Read file content
            content = file_content.read().decode('utf-8', errors='ignore')
            
            # Extract numbers from file
            if file_name.endswith('.csv'):
                # For CSV, try to find numbers in any column
                import csv
                import io
                reader = csv.reader(io.StringIO(content))
                for row in reader:
                    for cell in row:
                        cell = cell.strip()
                        if cell and (cell.startswith('+') or cell.isdigit()):
                            numbers.append(cell)
            else:
                # For TXT, each line is a number
                numbers = [line.strip() for line in content.split('\n') if line.strip()]
                
            await message.reply(f"📁 تم قراءة الملف بنجاح - وجد {len(numbers)} رقم")
            
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            await message.reply("❌ حدث خطأ في قراءة الملف")
            return
    
    elif message.text:
        # Handle text input
        numbers = [line.strip() for line in message.text.split('\n') if line.strip()]
    
    else:
        await message.reply("❌ يرجى إدخال أرقام أو رفع ملف")
        return
    
    if not numbers:
        await message.reply("❌ لم يتم العثور على أي أرقام")
        return
    
    data = await state.get_data()
    service_id = data.get('service_id')
    
    # Process numbers progressively with live updates
    if len(numbers) > 100:
        await process_numbers_progressive(numbers, service_id, message)
    else:
        # For small batches, use the old method
        result = await process_numbers_bulk(numbers, service_id)
        result_text = f"✅ تم الانتهاء من معالجة الأرقام!\n\n"
        result_text += f"📱 تم إضافة: {result['added']} رقم\n"
        if result['duplicates'] > 0:
            result_text += f"🔄 مكرر: {result['duplicates']} رقم\n"
        if result['invalid'] > 0:
            result_text += f"❌ غير صالح: {result['invalid']} رقم\n"
        if result.get('error'):
            result_text += f"⚠️ تحذير: {result['error']}\n"
        
        await message.reply(
            result_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إدارة الأرقام", callback_data="admin_numbers")
            ]])
        )
    
    await state.clear()

async def process_numbers_bulk(numbers: list, service_id: int) -> dict:
    """Process numbers in bulk with optimized database operations and enhanced error handling"""
    db = get_db()
    try:
        service = db.query(Service).filter(Service.id == service_id).first()
        if not service:
            logger.error(f"Service not found: {service_id}")
            return {"added": 0, "duplicates": 0, "invalid": 0, "error": "Service not found"}
        
        # Get all existing numbers for this service in one query
        existing_numbers = set()
        existing_query = db.query(Number.phone_number).filter(Number.service_id == service_id).all()
        for row in existing_query:
            existing_numbers.add(row[0])
        
        added_count = 0
        duplicate_count = 0
        invalid_count = 0
        
        # Prepare bulk insert data
        numbers_to_add = []
        processed_countries = set()
        
        for number in numbers:
            # Normalize the number
            normalized_number = normalize_phone_number(number)
            
            # Enhanced validation
            if not normalized_number or not normalized_number.startswith('+'):
                invalid_count += 1
                logger.debug(f"Invalid number format (no + or empty): {number}")
                continue
                
            # Check minimum and maximum length
            digits_only = normalized_number[1:]  # Remove +
            if not digits_only.isdigit() or len(digits_only) < 7 or len(digits_only) > 15:
                invalid_count += 1
                logger.debug(f"Invalid number length: {normalized_number} ({len(digits_only)} digits)")
                continue
            
            # Check for obviously invalid patterns
            if digits_only.startswith('0000') or digits_only == '0' * len(digits_only):
                invalid_count += 1
                logger.debug(f"Invalid pattern detected: {normalized_number}")
                continue
            
            # Check if number already exists (from our cached set)
            if normalized_number in existing_numbers:
                duplicate_count += 1
                continue
            
            # Detect country code
            detected_country_code = detect_country_code(normalized_number)
            processed_countries.add(detected_country_code)
            
            # Prepare for bulk insert
            numbers_to_add.append({
                'phone_number': normalized_number,
                'service_id': service_id,
                'country_code': detected_country_code,
                'status': 'AVAILABLE',
                'usage_count': 0
            })
            added_count += 1
        
        # Ensure all required ServiceCountry entries exist
        for country_code in processed_countries:
            ensure_service_country_exists(service_id, country_code, db)
        
        # Bulk insert all numbers at once
        if numbers_to_add:
            from sqlalchemy import text
            insert_stmt = text("""
                INSERT INTO numbers (phone_number, service_id, country_code, status, usage_count)
                VALUES (:phone_number, :service_id, :country_code, :status, :usage_count)
            """)
            db.execute(insert_stmt, numbers_to_add)
            db.commit()
        
        return {
            "added": added_count,
            "duplicates": duplicate_count,
            "invalid": invalid_count
        }
        
    except Exception as e:
        logger.error(f"Error in bulk number processing: {e}")
        db.rollback()
        return {"added": 0, "duplicates": 0, "invalid": 0, "error": str(e)}
    finally:
        db.close()

async def process_numbers_progressive(numbers: list, service_id: int, message) -> None:
    """Process numbers progressively with live updates to user"""
    BATCH_SIZE = 1000  # Process 1000 numbers at a time
    
    total_numbers = len(numbers)
    total_added = 0
    total_duplicates = 0
    total_invalid = 0
    total_processed = 0
    
    # Initial progress message
    progress_msg = await message.reply(
        f"🚀 بدء معالجة {total_numbers:,} رقم...\n"
        f"📊 سيتم التحديث كل {BATCH_SIZE:,} رقم\n"
        f"⏳ جاري التحضير..."
    )
    
    # Get all existing numbers for this service once
    db = get_db()
    try:
        existing_numbers = set()
        existing_query = db.query(Number.phone_number).filter(Number.service_id == service_id).all()
        for row in existing_query:
            existing_numbers.add(row[0])
    finally:
        db.close()
    
    # Process numbers in batches
    for i in range(0, total_numbers, BATCH_SIZE):
        batch = numbers[i:i + BATCH_SIZE]
        batch_end = min(i + BATCH_SIZE, total_numbers)
        
        # Process this batch
        result = await process_batch_progressive(batch, service_id, existing_numbers)
        
        # Update counters
        total_added += result['added']
        total_duplicates += result['duplicates'] 
        total_invalid += result['invalid']
        total_processed = batch_end
        
        # Add newly added numbers to existing set to avoid duplicates in next batches
        for number_data in result.get('new_numbers', []):
            existing_numbers.add(number_data['phone_number'])
        
        # Calculate progress percentage
        progress_percent = (total_processed / total_numbers) * 100
        
        # Update progress message
        progress_text = (
            f"📊 معالجة الأرقام - {progress_percent:.1f}%\n\n"
            f"🔄 تمت معالجة: {total_processed:,} / {total_numbers:,}\n"
            f"✅ تم إضافة: {total_added:,} رقم\n"
            f"🔄 مكرر: {total_duplicates:,} رقم\n"
            f"❌ غير صالح: {total_invalid:,} رقم\n\n"
        )
        
        if total_processed < total_numbers:
            progress_text += f"⏳ جاري معالجة الدفعة التالية..."
        else:
            progress_text += f"🎉 تم الانتهاء من المعالجة!"
            
        try:
            await progress_msg.edit_text(progress_text)
        except:
            # If edit fails, send new message
            try:
                await progress_msg.delete()
            except:
                pass
            progress_msg = await message.reply(progress_text)
        
        # Small delay to prevent hitting rate limits
        if total_processed < total_numbers:
            await asyncio.sleep(0.5)
    
    # Final summary message
    final_text = (
        f"✅ تم الانتهاء من معالجة جميع الأرقام!\n\n"
        f"📊 إجمالي الأرقام: {total_numbers:,}\n"
        f"📱 تم إضافة: {total_added:,} رقم جديد\n"
        f"🔄 مكرر: {total_duplicates:,} رقم\n" 
        f"❌ غير صالح: {total_invalid:,} رقم\n\n"
        f"🎯 معدل النجاح: {(total_added/total_numbers*100):.1f}%"
    )
    
    await message.reply(
        final_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إدارة الأرقام", callback_data="admin_numbers")
        ]])
    )

async def process_batch_progressive(numbers: list, service_id: int, existing_numbers: set) -> dict:
    """Process a batch of numbers progressively"""
    db = get_db()
    try:
        added_count = 0
        duplicate_count = 0
        invalid_count = 0
        numbers_to_add = []
        processed_countries = set()
        new_numbers = []
        
        for number in numbers:
            # Normalize the number
            normalized_number = normalize_phone_number(number)
            
            # Enhanced validation
            if not normalized_number or not normalized_number.startswith('+'):
                invalid_count += 1
                continue
                
            # Check minimum and maximum length
            digits_only = normalized_number[1:]  # Remove +
            if not digits_only.isdigit() or len(digits_only) < 7 or len(digits_only) > 15:
                invalid_count += 1
                continue
            
            # Check for obviously invalid patterns
            if digits_only.startswith('0000') or digits_only == '0' * len(digits_only):
                invalid_count += 1
                continue
            
            # Check if number already exists
            if normalized_number in existing_numbers:
                duplicate_count += 1
                continue
            
            # Detect country code
            detected_country_code = detect_country_code(normalized_number)
            processed_countries.add(detected_country_code)
            
            # Prepare for insert
            number_data = {
                'phone_number': normalized_number,
                'service_id': service_id,
                'country_code': detected_country_code,
                'status': 'AVAILABLE',
                'usage_count': 0
            }
            numbers_to_add.append(number_data)
            new_numbers.append(number_data)
            added_count += 1
        
        # Ensure all required ServiceCountry entries exist
        for country_code in processed_countries:
            ensure_service_country_exists(service_id, country_code, db)
        
        # Insert this batch
        if numbers_to_add:
            from sqlalchemy import text
            insert_stmt = text("""
                INSERT INTO numbers (phone_number, service_id, country_code, status, usage_count)
                VALUES (:phone_number, :service_id, :country_code, :status, :usage_count)
            """)
            db.execute(insert_stmt, numbers_to_add)
            db.commit()
        
        return {
            "added": added_count,
            "duplicates": duplicate_count,
            "invalid": invalid_count,
            "new_numbers": new_numbers
        }
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        db.rollback()
        return {"added": 0, "duplicates": 0, "invalid": 0, "new_numbers": []}
    finally:
        db.close()

# Country deletion handler
@dp.callback_query(F.data.startswith("delete_country_"))
async def delete_country_handler(callback: CallbackQuery):
    """Handle country deletion"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    country_id = int(callback.data.split("_")[-1])
    
    db = get_db()
    try:
        country = db.query(Country).filter(Country.id == country_id).first()
        if not country:
            await callback.answer("❌ الدولة غير موجودة")
            return
        
        # Check if country is used in any service
        used_services = db.query(ServiceCountry).filter(ServiceCountry.country_id == country_id).count()
        if used_services > 0:
            await callback.answer(
                f"❌ لا يمكن حذف الدولة لأنها مربوطة بـ {used_services} خدمة",
                show_alert=True
            )
            return
        
        country_name = country.name
        db.delete(country)
        db.commit()
        
        await callback.answer(f"✅ تم حذف دولة {country_name}")
        
        # Refresh the countries list
        await admin_list_countries_handler(callback)
        
    finally:
        db.close()

# Initialize database
def init_db():
    """Initialize database tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
        
        # Add default data
        db = get_db()
        try:
            # Add default admin user
            admin_user = db.query(User).filter(User.telegram_id == str(ADMIN_ID)).first()
            if not admin_user:
                admin_user = User(
                    telegram_id=str(ADMIN_ID),
                    username="admin",
                    first_name="Admin",
                    is_admin=True,
                    balance=1000
                )
                db.add(admin_user)
            
            # Add default services
            services_data = [
                {"name": "WhatsApp", "emoji": "📱", "default_price": 10},
                {"name": "Telegram", "emoji": "✈️", "default_price": 8},
                {"name": "Facebook", "emoji": "📘", "default_price": 12},
                {"name": "Instagram", "emoji": "📷", "default_price": 12},
                {"name": "Twitter", "emoji": "🐦", "default_price": 10},
            ]
            
            for service_data in services_data:
                existing = db.query(Service).filter(Service.name == service_data["name"]).first()
                if not existing:
                    # Only create if it was never created before (not just deleted)
                    service = Service(**service_data)
                    db.add(service)
                elif not existing.active:
                    # Don't reactivate deleted services automatically
                    logger.info(f"Service {service_data['name']} exists but is inactive - not reactivating")
            
            # Add default countries
            countries_data = [
                {"country_name": "مصر", "country_code": "+20", "flag": "🇪🇬"},
                {"country_name": "السعودية", "country_code": "+966", "flag": "🇸🇦"},
                {"country_name": "الإمارات", "country_code": "+971", "flag": "🇦🇪"},
                {"country_name": "الكويت", "country_code": "+965", "flag": "🇰🇼"},
                {"country_name": "قطر", "country_code": "+974", "flag": "🇶🇦"},
            ]
            
            services = db.query(Service).filter(Service.active == True).all()
            for service in services:
                for country_data in countries_data:
                    existing = db.query(ServiceCountry).filter(
                        ServiceCountry.service_id == service.id,
                        ServiceCountry.country_code == country_data["country_code"]
                    ).first()
                    if not existing:
                        country = ServiceCountry(
                            service_id=service.id,
                            **country_data
                        )
                        db.add(country)
            
            db.commit()
            logger.info("Default data added successfully")
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

# Export data handlers
@dp.callback_query(F.data == "export_users")
async def export_users_handler(callback: CallbackQuery):
    """Export users data to CSV"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        users = db.query(User).all()
        
        import csv
        import io
        from datetime import datetime
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(['ID', 'Telegram ID', 'First Name', 'Username', 'Balance', 'Is Admin', 'Is Banned', 'Joined Date'])
        
        # Write data
        for user in users:
            writer.writerow([
                user.id,
                user.telegram_id,
                user.first_name or '',
                user.username or '',
                user.balance,
                user.is_admin,
                user.is_banned,
                user.joined_at.strftime('%Y-%m-%d %H:%M:%S') if user.joined_at else ''
            ])
        
        csv_content = output.getvalue()
        output.close()
        
        # Send as document
        filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        document = types.BufferedInputFile(csv_content.encode('utf-8'), filename=filename)
        
        await callback.message.reply_document(
            document,
            caption=f"✅ تم تصدير بيانات {len(users)} مستخدم"
        )
        
        await callback.answer("✅ تم التصدير بنجاح")
        
    except Exception as e:
        logger.error(f"Error exporting users: {e}")
        await callback.answer("❌ حدث خطأ أثناء التصدير")
    finally:
        db.close()

@dp.callback_query(F.data == "export_numbers")
async def export_numbers_handler(callback: CallbackQuery):
    """Export numbers data to CSV"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        numbers = db.query(Number).join(Service).join(Country).all()
        
        import csv
        import io
        from datetime import datetime
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(['ID', 'Phone Number', 'Service', 'Country', 'Status', 'Created Date'])
        
        # Write data
        for number in numbers:
            writer.writerow([
                number.id,
                number.phone_number,
                number.service.name,
                number.country.name,
                number.status.value,
                number.created_at.strftime('%Y-%m-%d %H:%M:%S') if number.created_at else ''
            ])
        
        csv_content = output.getvalue()
        output.close()
        
        # Send as document
        filename = f"numbers_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        document = types.BufferedInputFile(csv_content.encode('utf-8'), filename=filename)
        
        await callback.message.reply_document(
            document,
            caption=f"✅ تم تصدير بيانات {len(numbers)} رقم"
        )
        
        await callback.answer("✅ تم التصدير بنجاح")
        
    except Exception as e:
        logger.error(f"Error exporting numbers: {e}")
        await callback.answer("❌ حدث خطأ أثناء التصدير")
    finally:
        db.close()

@dp.callback_query(F.data == "export_reservations")
async def export_reservations_handler(callback: CallbackQuery):
    """Export reservations data to CSV"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        reservations = db.query(Reservation).join(User).join(Service).join(Number).all()
        
        import csv
        import io
        from datetime import datetime
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(['ID', 'User ID', 'User Name', 'Phone Number', 'Service', 'Status', 'Created Date', 'Expires At'])
        
        # Write data
        for reservation in reservations:
            writer.writerow([
                reservation.id,
                reservation.user.telegram_id,
                reservation.user.first_name or '',
                reservation.number.phone_number,
                reservation.service.name,
                reservation.status.value,
                reservation.created_at.strftime('%Y-%m-%d %H:%M:%S') if reservation.created_at else '',
                reservation.expires_at.strftime('%Y-%m-%d %H:%M:%S') if reservation.expires_at else ''
            ])
        
        csv_content = output.getvalue()
        output.close()
        
        # Send as document
        filename = f"reservations_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        document = types.BufferedInputFile(csv_content.encode('utf-8'), filename=filename)
        
        await callback.message.reply_document(
            document,
            caption=f"✅ تم تصدير بيانات {len(reservations)} حجز"
        )
        
        await callback.answer("✅ تم التصدير بنجاح")
        
    except Exception as e:
        logger.error(f"Error exporting reservations: {e}")
        await callback.answer("❌ حدث خطأ أثناء التصدير")
    finally:
        db.close()

@dp.callback_query(F.data == "export_transactions")
async def export_transactions_handler(callback: CallbackQuery):
    """Export transactions data to CSV"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        transactions = db.query(Transaction).join(User).all()
        
        import csv
        import io
        from datetime import datetime
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(['ID', 'User ID', 'User Name', 'Type', 'Amount', 'Description', 'Created Date'])
        
        # Write data
        for transaction in transactions:
            writer.writerow([
                transaction.id,
                transaction.user.telegram_id,
                transaction.user.first_name or '',
                transaction.type.value,
                transaction.amount,
                transaction.description or '',
                transaction.created_at.strftime('%Y-%m-%d %H:%M:%S') if transaction.created_at else ''
            ])
        
        csv_content = output.getvalue()
        output.close()
        
        # Send as document
        filename = f"transactions_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        document = types.BufferedInputFile(csv_content.encode('utf-8'), filename=filename)
        
        await callback.message.reply_document(
            document,
            caption=f"✅ تم تصدير بيانات {len(transactions)} معاملة"
        )
        
        await callback.answer("✅ تم التصدير بنجاح")
        
    except Exception as e:
        logger.error(f"Error exporting transactions: {e}")
        await callback.answer("❌ حدث خطأ أثناء التصدير")
    finally:
        db.close()

# Additional handlers for new features
@dp.callback_query(F.data == "add_user_data_channel")
async def add_user_data_channel_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding user data channel"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_channel_id)
    await callback.message.edit_text(
        "📋 إضافة قناة بيانات المستخدمين\n\n"
        "أدخل معرف القناة (مثل: -1001234567890):\n\n"
        "💡 لإيجاد المعرف:\n"
        "1. أضف البوت كمشرف في القناة\n"
        "2. أرسل رسالة في القناة وادخل إعادة توجيه\n"
        "3. استخدم بوت للحصول على المعرف",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_user_data_channel")
        ]])
    )

@dp.callback_query(F.data == "add_forced_subscription")
async def add_forced_subscription_handler(callback: CallbackQuery, state: FSMContext):
    """Handle adding forced subscription channel"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    await state.set_state(AdminStates.waiting_for_subscription_channel_id)
    await callback.message.edit_text(
        "🔒 إضافة قناة اشتراك إجباري\n\n"
        "أدخل معرف القناة (مثل: -1001234567890):\n\n"
        "💡 لإيجاد المعرف:\n"
        "1. أضف البوت كمشرف في القناة\n"
        "2. أرسل رسالة في القناة وادخل إعادة توجيه\n"
        "3. استخدم بوت للحصول على المعرف",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 إلغاء", callback_data="admin_forced_subscription")
        ]])
    )

@dp.message(StateFilter(AdminStates.waiting_for_channel_id))
async def process_user_data_channel_id(message: types.Message, state: FSMContext):
    """Process user data channel ID input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    try:
        channel_id = int(message.text.strip())
        
        # Test bot access to channel
        try:
            chat = await bot.get_chat(channel_id)
            chat_member = await bot.get_chat_member(channel_id, bot.id)
            
            if chat_member.status not in ['administrator', 'creator']:
                await message.reply("❌ البوت يجب أن يكون مشرف في القناة")
                return
            
        except Exception as e:
            await message.reply(f"❌ لا يمكن الوصول للقناة: {str(e)}")
            return
        
        # Save channel to database
        db = get_db()
        try:
            # Deactivate existing channels
            db.query(UserDataChannel).update({UserDataChannel.active: False})
            
            # Add new channel
            new_channel = UserDataChannel(
                channel_id=str(channel_id),
                channel_title=chat.title if hasattr(chat, 'title') else None,
                channel_username=chat.username if hasattr(chat, 'username') else None,
                active=True
            )
            db.add(new_channel)
            db.commit()
            
            await message.reply(
                f"✅ تم إضافة قناة بيانات المستخدمين بنجاح!\n"
                f"📢 القناة: {chat.title if hasattr(chat, 'title') else channel_id}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_user_data_channel")
                ]])
            )
            
        finally:
            db.close()
            
    except ValueError:
        await message.reply("❌ معرف القناة يجب أن يكون رقم")
        return
    except Exception as e:
        logger.error(f"Error adding user data channel: {e}")
        await message.reply("❌ حدث خطأ أثناء إضافة القناة")
    
    await state.clear()

@dp.message(StateFilter(AdminStates.waiting_for_subscription_channel_id))
async def process_forced_subscription_channel_id(message: types.Message, state: FSMContext):
    """Process forced subscription channel ID input"""
    if not is_admin_session_valid(message.from_user.id):
        await message.reply("❌ انتهت صلاحية الجلسة")
        return
    
    try:
        channel_id = int(message.text.strip())
        
        # Test bot access to channel
        try:
            chat = await bot.get_chat(channel_id)
            chat_member = await bot.get_chat_member(channel_id, bot.id)
            
            if chat_member.status not in ['administrator', 'creator']:
                await message.reply("❌ البوت يجب أن يكون مشرف في القناة")
                return
            
        except Exception as e:
            await message.reply(f"❌ لا يمكن الوصول للقناة: {str(e)}")
            return
        
        # Save channel to database
        db = get_db()
        try:
            # Add new forced subscription
            new_subscription = ForcedSubscription(
                channel_id=str(channel_id),
                channel_title=chat.title if hasattr(chat, 'title') else None,
                channel_username=chat.username if hasattr(chat, 'username') else None,
                active=True
            )
            db.add(new_subscription)
            db.commit()
            
            await message.reply(
                f"✅ تم إضافة قناة الاشتراك الإجباري بنجاح!\n"
                f"📢 القناة: {chat.title if hasattr(chat, 'title') else channel_id}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 إدارة الاشتراك", callback_data="admin_forced_subscription")
                ]])
            )
            
        finally:
            db.close()
            
    except ValueError:
        await message.reply("❌ معرف القناة يجب أن يكون رقم")
        return
    except Exception as e:
        logger.error(f"Error adding forced subscription: {e}")
        await message.reply("❌ حدث خطأ أثناء إضافة القناة")
    
    await state.clear()

@dp.callback_query(F.data == "delete_user_data_channel")
async def delete_user_data_channel_handler(callback: CallbackQuery):
    """Handle deleting user data channel"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        # Deactivate all user data channels
        db.query(UserDataChannel).update({UserDataChannel.active: False})
        db.commit()
        
        await callback.message.edit_text(
            "✅ تم حذف قناة بيانات المستخدمين بنجاح!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 إدارة القنوات", callback_data="admin_user_data_channel")
            ]])
        )
    finally:
        db.close()

@dp.callback_query(F.data == "manage_forced_subscriptions")
async def manage_forced_subscriptions_handler(callback: CallbackQuery):
    """Handle managing forced subscriptions"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    db = get_db()
    try:
        subs = db.query(ForcedSubscription).filter(
            ForcedSubscription.active == True
        ).all()
        
        text = "📋 إدارة قنوات الاشتراك الإجباري\n\n"
        keyboard = InlineKeyboardBuilder()
        
        for sub in subs:
            title = sub.channel_title or f"Channel {sub.channel_id}"
            keyboard.row(InlineKeyboardButton(
                text=f"🗑 حذف {title}",
                callback_data=f"delete_forced_sub_{sub.id}"
            ))
        
        keyboard.row(InlineKeyboardButton(text="🔙 الاشتراك الإجباري", callback_data="admin_forced_subscription"))
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
    finally:
        db.close()

@dp.callback_query(F.data.startswith("delete_forced_sub_"))
async def delete_forced_subscription_handler(callback: CallbackQuery):
    """Handle deleting specific forced subscription"""
    if not is_admin_session_valid(callback.from_user.id):
        await callback.answer("❌ انتهت صلاحية الجلسة")
        return
    
    sub_id = int(callback.data.split("_")[3])
    
    db = get_db()
    try:
        sub = db.query(ForcedSubscription).filter(ForcedSubscription.id == sub_id).first()
        if sub:
            sub.active = False
            db.commit()
            await callback.answer("✅ تم حذف القناة")
        else:
            await callback.answer("❌ القناة غير موجودة")
        
        # Refresh the management view
        await manage_forced_subscriptions_handler(callback)
    finally:
        db.close()

async def main():
    """Main function"""
    # Initialize database
    init_db()
    
    # Set bot commands menu
    await set_bot_commands(bot)
    
    # Start auto cleanup system
    start_auto_cleanup()
    logger.info("🗑️ Auto cleanup system initialized")
    
    # Start essential background tasks only (reduced for better performance)
    asyncio.create_task(check_expired_reservations())
    asyncio.create_task(check_user_subscriptions_periodically())
    # Removed heavy tasks: poll_provider_messages and send_all_users_data_periodically
    
    # Start bot with simple retry mechanism
    logger.info("Starting bot...")
    max_retries = 5
    retry_delay = 30  # 30 seconds
    
    for attempt in range(max_retries):
        try:
            await dp.start_polling(bot)
            break
        except Exception as e:
            if "Conflict" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Polling conflict detected (attempt {attempt + 1}/{max_retries}). Waiting {retry_delay} seconds before retry...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)  # Max 5 minutes
                continue
            else:
                logger.error(f"Failed to start bot after {attempt + 1} attempts: {e}")
                raise

if __name__ == "__main__":
    asyncio.run(main())
