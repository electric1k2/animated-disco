"""
Simplified translation system - Arabic and English only
No external translation APIs - static translations only
"""

from functools import lru_cache

# Static translations for common phrases - Arabic and English only
STATIC_TRANSLATIONS = {
    'main_menu': {
        'ar': '🏠 الرئيسية',
        'en': '🏠 Home'
    },
    'balance': {
        'ar': '💰 الرصيد',
        'en': '💰 Balance'
    },
    'services': {
        'ar': '📱 الخدمات',
        'en': '📱 Services'
    },
    'language': {
        'ar': '🌍 اللغة',
        'en': '🌍 Language'
    },
    'free_credits': {
        'ar': '🎁 رصيد مجاني',
        'en': '🎁 Free Credits'
    },
    'contact_support': {
        'ar': '📞 الدعم الفني',
        'en': '📞 Support'
    },
    'admin_panel': {
        'ar': '⚙️ لوحة الإدارة',
        'en': '⚙️ Admin Panel'
    },
    'back': {
        'ar': '🔙 رجوع',
        'en': '🔙 Back'
    },
    'cancel': {
        'ar': '❌ إلغاء',
        'en': '❌ Cancel'
    },
    'confirm': {
        'ar': '✅ تأكيد',
        'en': '✅ Confirm'
    },
    'settings': {
        'ar': '⚙️ الإعدادات',
        'en': '⚙️ Settings'
    },
    'help': {
        'ar': '❓ المساعدة',
        'en': '❓ Help'
    },
    'about': {
        'ar': '📋 حول البوت',
        'en': '📋 About Bot'
    },
    'statistics': {
        'ar': '📊 الإحصائيات',
        'en': '📊 Statistics'
    },
    'transactions': {
        'ar': '💳 المعاملات',
        'en': '💳 Transactions'
    },
    'profile': {
        'ar': '👤 الملف الشخصي',
        'en': '👤 Profile'
    },
    'notifications': {
        'ar': '🔔 الإشعارات',
        'en': '🔔 Notifications'
    },
    'history': {
        'ar': '📜 السجل',
        'en': '📜 History'
    },
    'search': {
        'ar': '🔍 البحث',
        'en': '🔍 Search'
    },
    'edit': {
        'ar': '✏️ تعديل',
        'en': '✏️ Edit'
    },
    'delete': {
        'ar': '🗑️ حذف',
        'en': '🗑️ Delete'
    },
    'add': {
        'ar': '➕ إضافة',
        'en': '➕ Add'
    },
    'save': {
        'ar': '💾 حفظ',
        'en': '💾 Save'
    },
    'loading': {
        'ar': '⏳ جاري التحميل...',
        'en': '⏳ Loading...'
    },
    'success': {
        'ar': '✅ تم بنجاح',
        'en': '✅ Success'
    },
    'error': {
        'ar': '❌ خطأ',
        'en': '❌ Error'
    },
    'warning': {
        'ar': '⚠️ تحذير',
        'en': '⚠️ Warning'
    },
    'info': {
        'ar': 'ℹ️ معلومات',
        'en': 'ℹ️ Info'
    },
    'welcome': {
        'ar': '👋 أهلاً وسهلاً',
        'en': '👋 Welcome'
    },
    'goodbye': {
        'ar': '👋 مع السلامة',
        'en': '👋 Goodbye'
    },
    'choose_option': {
        'ar': 'اختر خياراً:',
        'en': 'Choose an option:'
    },
    'invalid_choice': {
        'ar': 'خيار غير صحيح',
        'en': 'Invalid choice'
    },
    'try_again': {
        'ar': 'حاول مرة أخرى',
        'en': 'Try again'
    },
    'phone_number': {
        'ar': '📱 رقم الهاتف',
        'en': '📱 Phone Number'
    },
    'verification_code': {
        'ar': '🔐 رمز التحقق',
        'en': '🔐 Verification Code'
    },
    'country': {
        'ar': '🌍 الدولة',
        'en': '🌍 Country'
    },
    'service': {
        'ar': '🏷️ الخدمة',
        'en': '🏷️ Service'
    },
    'price': {
        'ar': '💰 السعر',
        'en': '💰 Price'
    },
    'status': {
        'ar': '📊 الحالة',
        'en': '📊 Status'
    },
    'available': {
        'ar': '✅ متاح',
        'en': '✅ Available'
    },
    'unavailable': {
        'ar': '❌ غير متاح',
        'en': '❌ Unavailable'
    },
    'pending': {
        'ar': '⏳ في الانتظار',
        'en': '⏳ Pending'
    },
    'completed': {
        'ar': '✅ مكتمل',
        'en': '✅ Completed'
    },
    'expired': {
        'ar': '⏰ منتهي الصلاحية',
        'en': '⏰ Expired'
    },
    'insufficient_balance': {
        'ar': '💰 رصيد غير كافي',
        'en': '💰 Insufficient Balance'
    },
    'purchase_successful': {
        'ar': '✅ تم الشراء بنجاح',
        'en': '✅ Purchase Successful'
    },
    'purchase_failed': {
        'ar': '❌ فشل في الشراء',
        'en': '❌ Purchase Failed'
    },
    'timeout': {
        'ar': '⏰ انتهت المهلة الزمنية',
        'en': '⏰ Timeout'
    },
    'channel_join': {
        'ar': '📢 انضم للقناة',
        'en': '📢 Join Channel'
    },
    'group_join': {
        'ar': '👥 انضم للجروب',
        'en': '👥 Join Group'
    },
    'verify': {
        'ar': '✅ تحقق',
        'en': '✅ Verify'
    },
    'reward_received': {
        'ar': '🎉 تم استلام المكافأة',
        'en': '🎉 Reward Received'
    },
    'already_claimed': {
        'ar': '✅ تم الاستلام مسبقاً',
        'en': '✅ Already Claimed'
    },
    'must_join_first': {
        'ar': '❌ يجب الانضمام أولاً',
        'en': '❌ Must Join First'
    },
    'admin_users': {
        'ar': '👥 إدارة المستخدمين',
        'en': '👥 User Management'
    },
    'admin_services': {
        'ar': '📱 إدارة الخدمات',
        'en': '📱 Service Management'
    },
    'admin_numbers': {
        'ar': '📞 إدارة الأرقام',
        'en': '📞 Number Management'
    },
    'admin_channels': {
        'ar': '📢 إدارة القنوات',
        'en': '📢 Channel Management'
    },
    'admin_groups': {
        'ar': '👥 إدارة الجروبات',
        'en': '👥 Group Management'
    },
    'admin_settings': {
        'ar': '⚙️ إعدادات النظام',
        'en': '⚙️ System Settings'
    },
    'user_list': {
        'ar': '📋 قائمة المستخدمين',
        'en': '📋 User List'
    },
    'ban_user': {
        'ar': '🚫 حظر مستخدم',
        'en': '🚫 Ban User'
    },
    'unban_user': {
        'ar': '✅ إلغاء حظر مستخدم',
        'en': '✅ Unban User'
    },
    'add_balance': {
        'ar': '💰 إضافة رصيد',
        'en': '💰 Add Balance'
    },
    'deduct_balance': {
        'ar': '💸 خصم رصيد',
        'en': '💸 Deduct Balance'
    },
    'view_transactions': {
        'ar': '💳 عرض المعاملات',
        'en': '💳 View Transactions'
    },
    'export_data': {
        'ar': '📤 تصدير البيانات',
        'en': '📤 Export Data'
    },
    'system_stats': {
        'ar': '📈 إحصائيات النظام',
        'en': '📈 System Statistics'
    },
    'backup_db': {
        'ar': '💾 نسخ احتياطي',
        'en': '💾 Database Backup'
    },
    'group_info': {
        'ar': 'معلومات الجروب',
        'en': 'Group Info'
    },
    'admin_password_prompt': {
        'ar': 'أدخل كلمة مرور الأدمن:',
        'en': 'Enter admin password:'
    },
    'admin_login_success': {
        'ar': '✅ تم تسجيل الدخول بنجاح',
        'en': '✅ Login successful'
    },
    'admin_login_failed': {
        'ar': '❌ كلمة المرور خاطئة',
        'en': '❌ Incorrect password'
    },
    'choose_section': {
        'ar': 'اختر القسم:',
        'en': 'Choose section:'
    }
}

class TranslationManager:
    def __init__(self):
        pass
        
    @lru_cache(maxsize=1000)
    def get_static_text(self, key: str, lang_code: str = 'ar') -> str:
        """Get static translation for common phrases"""
        if key in STATIC_TRANSLATIONS:
            # Try to get the requested language first
            if lang_code in STATIC_TRANSLATIONS[key]:
                return STATIC_TRANSLATIONS[key][lang_code]
            # If not found, try English as fallback
            elif 'en' in STATIC_TRANSLATIONS[key]:
                return STATIC_TRANSLATIONS[key]['en']
            # Last resort: Arabic
            else:
                return STATIC_TRANSLATIONS[key]['ar']
        return key
    
    async def translate_text(self, text: str, target_lang: str = 'ar', source_lang: str = 'auto') -> str:
        """Simple translation for Arabic and English only - no dynamic translation"""
        # Just return the original text - no dynamic translation
        return text
    
    def get_language_name(self, lang_code: str) -> str:
        """Get language name with flag"""
        simple_languages = {
            'ar': '🇸🇦 العربية',
            'en': '🇺🇸 English'
        }
        return simple_languages.get(lang_code, '🇸🇦 العربية')
    
    def get_language_codes(self) -> dict:
        """Get supported language codes (Arabic and English only)"""
        return {
            'ar': '🇸🇦 العربية',
            'en': '🇺🇸 English'
        }

# Global translator instance
translator = TranslationManager()

def t(key: str, lang_code: str = 'ar') -> str:
    """Quick function to get static translations"""
    return translator.get_static_text(key, lang_code)

async def translate(text: str, lang_code: str = 'ar') -> str:
    """Quick function to translate dynamic text"""
    return await translator.translate_text(text, lang_code)