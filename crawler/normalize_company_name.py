import re
import unicodedata

def normalize_text(text: str) -> str:
    """
    Chuẩn hóa chữ: loại dấu, chữ thường, loại ký tự thừa
    """
    text = unicodedata.normalize('NFKC', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()

def remove_brackets(text: str) -> str:
    # Loại bỏ tất cả nội dung trong (), bao gồm dấu ()
    return re.sub(r'\s*\(.*?\)\s*', '', text).strip()

def normalize_company_name(full_name: str) -> str:
    """
    Extract main company name from full name, supporting Vietnamese + English forms
    """
    if not full_name:
        return ""
    
    name = full_name.strip()

    # List các tiền tố phổ biến (Vietnamese + English)
    prefixes = [
        r'công ty trách nhiệm hữu hạn',  # TNHH
        r'công ty tnhh',
        r'công ty cổ phần',              # CP
        r'công ty cp',
        r'công ty',                      # general
        r'company',
        r'corporation',
        r'co',
    ]

    suffixes = [
        r'co\.?,?\s?ltd\.?',
        r'limited',
        r'corporation',
        r'corp\.?',
        r'ltd\.?',
    ]

    # Loại bỏ tiền tố
    for p in prefixes:
        regex = re.compile(r'^' + p + r'\s+', flags=re.IGNORECASE)
        name = regex.sub('', name)
    
    # Loại bỏ hậu tố
    for s in suffixes:
        regex = re.compile(r'\s+' + s + r'$|\,\s*' + s + r'$', flags=re.IGNORECASE)
        name = regex.sub('', name)

    # Loại bỏ ký tự không phải chữ/ số/ dấu cách
    name = re.sub(r'[^0-9a-zA-ZÀ-ỹ\s]', '', name)

    # Chuẩn hóa khoảng trắng và chữ thường
    name = normalize_text(name)

    return remove_brackets(name)
