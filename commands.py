"""
Bot Commands System - Quick commands for easy control
"""

from aiogram import Bot
from aiogram.types import BotCommand
from translations import t

async def set_bot_commands(bot: Bot, lang_code: str = 'ar'):
    """Set bot commands menu for easy access"""
    
    commands = [
        BotCommand(command="start", description=t('main_menu', lang_code)),
        BotCommand(command="balance", description=t('my_balance', lang_code)),
        BotCommand(command="help", description=t('help', lang_code)),
        BotCommand(command="language", description=t('choose_language', lang_code)),
        BotCommand(command="admin", description=t('admin_panel', lang_code)),
        BotCommand(command="services", description="📱 " + t('services', lang_code)),
        BotCommand(command="history", description="📋 " + t('history', lang_code)),
        BotCommand(command="support", description="🆘 " + t('support', lang_code)),
        BotCommand(command="cancel", description="❌ " + t('cancel', lang_code)),
        BotCommand(command="chatinfo", description="ℹ️ " + t('group_info', lang_code))
    ]
    
    await bot.set_my_commands(commands)

async def get_text(text: str, lang_code: str = 'ar') -> str:
    """Get translated text - simplified version"""
    translations = {
        'خدمات الأرقام': {
            'ar': 'خدمات الأرقام',
            'en': 'Phone Services', 
            'es': 'Servicios Telefónicos',
            'fr': 'Services Téléphoniques',
            'de': 'Telefondienste',
            'it': 'Servizi Telefonici',
            'pt': 'Serviços Telefônicos',
            'ru': 'Телефонные услуги',
            'zh': '电话服务',
            'ja': '電話サービス',
            'ko': '전화 서비스',
            'tr': 'Telefon Hizmetleri',
            'hi': 'फोन सेवाएं',
            'ur': 'فون سروسز',
            'fa': 'سرویس‌های تلفن',
            'id': 'Layanan Telepon',
            'ms': 'Perkhidmatan Telefon',
            'th': 'บริการโทรศัพท์',
            'vi': 'Dịch Vụ Điện Thoại'
        },
        'سجل الطلبات': {
            'ar': 'سجل الطلبات',
            'en': 'Order History',
            'es': 'Historial de Pedidos',
            'fr': 'Historique des Commandes',
            'de': 'Bestellverlauf',
            'it': 'Cronologia Ordini',
            'pt': 'Histórico de Pedidos',
            'ru': 'История заказов',
            'zh': '订单历史',
            'ja': '注文履歴',
            'ko': '주문 내역',
            'tr': 'Sipariş Geçmişi',
            'hi': 'ऑर्डर इतिहास',
            'ur': 'آرڈر کی تاریخ',
            'fa': 'تاریخچه سفارشات',
            'id': 'Riwayat Pesanan',
            'ms': 'Sejarah Pesanan',
            'th': 'ประวัติการสั่งซื้อ',
            'vi': 'Lịch Sử Đặt Hàng'
        },
        'الدعم الفني': {
            'ar': 'الدعم الفني',
            'en': 'Technical Support',
            'es': 'Soporte Técnico',
            'fr': 'Support Technique',
            'de': 'Technischer Support',
            'it': 'Supporto Tecnico',
            'pt': 'Suporte Técnico',
            'ru': 'Техническая поддержка',
            'zh': '技术支持',
            'ja': 'テクニカルサポート',
            'ko': '기술 지원',
            'tr': 'Teknik Destek',
            'hi': 'तकनीकी सहायता',
            'ur': 'تکنیکی سپورٹ',
            'fa': 'پشتیبانی فنی',
            'id': 'Dukungan Teknis',
            'ms': 'Sokongan Teknikal',
            'th': 'การสนับสนุนทางเทคนิค',
            'vi': 'Hỗ Trợ Kỹ Thuật'
        },
        'إلغاء العملية': {
            'ar': 'إلغاء العملية',
            'en': 'Cancel Operation',
            'es': 'Cancelar Operación',
            'fr': 'Annuler l\'Opération',
            'de': 'Vorgang Abbrechen',
            'it': 'Annulla Operazione',
            'pt': 'Cancelar Operação',
            'ru': 'Отменить операцию',
            'zh': '取消操作',
            'ja': '操作をキャンセル',
            'ko': '작업 취소',
            'tr': 'İşlemi İptal Et',
            'hi': 'ऑपरेशन रद्द करें',
            'ur': 'آپریشن منسوخ کریں',
            'fa': 'لغو عملیات',
            'id': 'Batalkan Operasi',
            'ms': 'Batal Operasi',
            'th': 'ยกเลิกการดำเนินการ',
            'vi': 'Hủy Thao Tác'
        },
        'معلومات الجروب': {
            'ar': 'معلومات الجروب',
            'en': 'Group Info',
            'es': 'Información del Grupo',
            'fr': 'Informations du Groupe',
            'de': 'Gruppeninfo',
            'it': 'Info Gruppo',
            'pt': 'Informações do Grupo',
            'ru': 'Информация о группе',
            'zh': '群组信息',
            'ja': 'グループ情報',
            'ko': '그룹 정보',
            'tr': 'Grup Bilgisi',
            'hi': 'समूह जानकारी',
            'ur': 'گروپ کی معلومات',
            'fa': 'اطلاعات گروه',
            'id': 'Info Grup',
            'ms': 'Maklumat Kumpulan',
            'th': 'ข้อมูลกลุ่ม',
            'vi': 'Thông Tin Nhóm'
        },
        # ترجمة أسماء الخدمات
        'Telegram': {
            'ar': 'تليجرام',
            'en': 'Telegram',
            'es': 'Telegram',
            'fr': 'Telegram',
            'de': 'Telegram',
            'it': 'Telegram',
            'pt': 'Telegram',
            'ru': 'Телеграм',
            'zh': '电报',
            'ja': 'テレグラム',
            'ko': '텔레그램',
            'tr': 'Telegram',
            'hi': 'टेलीग्राम',
            'ur': 'ٹیلی گرام',
            'fa': 'تلگرام',
            'id': 'Telegram',
            'ms': 'Telegram',
            'th': 'Telegram',
            'vi': 'Telegram'
        },
        'Facebook': {
            'ar': 'فيسبوك',
            'en': 'Facebook',
            'es': 'Facebook',
            'fr': 'Facebook',
            'de': 'Facebook',
            'it': 'Facebook',
            'pt': 'Facebook',
            'ru': 'Фейсбук',
            'zh': '脸书',
            'ja': 'フェイスブック',
            'ko': '페이스북',
            'tr': 'Facebook',
            'hi': 'फेसबुक',
            'ur': 'فیس بک',
            'fa': 'فیس‌بوک',
            'id': 'Facebook',
            'ms': 'Facebook',
            'th': 'Facebook',
            'vi': 'Facebook'
        },
        'Instagram': {
            'ar': 'انستقرام',
            'en': 'Instagram',
            'es': 'Instagram',
            'fr': 'Instagram',
            'de': 'Instagram',
            'it': 'Instagram',
            'pt': 'Instagram',
            'ru': 'Инстаграм',
            'zh': 'Instagram',
            'ja': 'インスタグラム',
            'ko': '인스타그램',
            'tr': 'Instagram',
            'hi': 'इंस्टाग्राम',
            'ur': 'انسٹاگرام',
            'fa': 'اینستاگرام',
            'id': 'Instagram',
            'ms': 'Instagram',
            'th': 'Instagram',
            'vi': 'Instagram'
        },
        'Twitter': {
            'ar': 'تويتر',
            'en': 'Twitter',
            'es': 'Twitter',
            'fr': 'Twitter',
            'de': 'Twitter',
            'it': 'Twitter',
            'pt': 'Twitter',
            'ru': 'Твиттер',
            'zh': '推特',
            'ja': 'ツイッター',
            'ko': '트위터',
            'tr': 'Twitter',
            'hi': 'ट्विटर',
            'ur': 'ٹویٹر',
            'fa': 'توییتر',
            'id': 'Twitter',
            'ms': 'Twitter',
            'th': 'Twitter',
            'vi': 'Twitter'
        }
    }
    
    if text in translations:
        # Try to get the requested language first
        if lang_code in translations[text]:
            return translations[text][lang_code]
        # If not found, try English as fallback
        elif 'en' in translations[text]:
            return translations[text]['en']
        # Last resort: Arabic
        else:
            return translations[text]['ar']
    return text