"""Data Processing Module - Customer data cleaning and validation"""

import re
import pandas as pd
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processes and cleans customer data for WhatsApp campaigns"""
    
    @staticmethod
    def fix_phone_format(phone: str) -> Optional[str]:
        if pd.isna(phone):
            return None
        
        phone = str(phone).strip()
        if not phone.startswith('+'):
            phone = '+' + phone
        
        if not re.match(r'^\+\d{10,15}$', phone):
            return None
        
        return phone
    
    @staticmethod
    def is_french_number(phone: str) -> bool:
        if pd.isna(phone):
            return False
        return str(phone).startswith('+33')
    
    @staticmethod
    def clean_name(name: str) -> Optional[str]:
        if pd.isna(name):
            return None
        
        name = str(name).strip()
        name = re.sub(r'\s*\([^)]*\)', '', name)
        
        patterns_to_remove = [
            r'\s+(nous doit|doit)\s+\d+€.*$',
            r'\s+P\d+.*$',
            r'\s+(ROUTE|LAVAGE|INTE|BAC|portail|clef gardee).*$',
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        name = re.sub(r'\s+', ' ', name).strip()
        
        if len(name) < 2:
            return None
        
        return name
    
    @staticmethod
    def extract_first_name(full_name: str) -> Optional[str]:
        if pd.isna(full_name):
            return None
        
        full_name = str(full_name).strip()
        
        match = re.match(r'^([A-Z][a-zéèêëàâäôöûüçñ-]+)\s+[A-Z]', full_name)
        if match:
            return match.group(1)
        
        match = re.match(r'^[A-Z]+\s+([A-Z][a-zéèêëàâäôöûüçñ-]+)', full_name)
        if match:
            return match.group(1)
        
        first_word = full_name.split()[0] if full_name.split() else full_name
        
        skip_prefixes = ['M.', 'Mme', 'Madame', 'Monsieur', 'Dr', 'Mr']
        if first_word in skip_prefixes:
            words = full_name.split()
            if len(words) > 1:
                first_word = words[1]
        
        return first_word.capitalize()
    
    @staticmethod
    def calculate_quality_score(row: pd.Series) -> int:
        score = 0
        
        if pd.notna(row.get('client_email')) and row['client_email'].strip():
            score += 10
        
        name = str(row.get('client_name', ''))
        
        parasitic_words = ['doit', 'lavage', 'impoli', 'route', 'gardee', 'effectuer', 'portail']
        if not any(word in name.lower() for word in parasitic_words):
            score += 5
        
        if re.match(r'^[A-Z][a-z]+ [A-Z]+', name):
            score += 3
        
        if len(name) < 3 or len(name) > 50:
            score -= 5
        
        return score
    
    @classmethod
    def process_database(cls, df: pd.DataFrame, french_only: bool = True) -> Tuple[pd.DataFrame, dict]:
        logger.info("Starting database processing...")
        
        initial_count = len(df)
        
        df['client_phone'] = df['client_phone'].apply(cls.fix_phone_format)
        df = df[df['client_phone'].notna()].copy()
        
        df['quality_score'] = df.apply(cls.calculate_quality_score, axis=1)
        
        before_dedup = len(df)
        df = df.sort_values('quality_score', ascending=False).drop_duplicates(
            subset='client_phone', keep='first'
        )
        duplicates_removed = before_dedup - len(df)
        
        df['client_name'] = df['client_name'].apply(cls.clean_name)
        df = df[df['client_name'].notna()].copy()
        
        df['first_name'] = df['client_name'].apply(cls.extract_first_name)
        
        if french_only:
            before_filter = len(df)
            df = df[df['client_phone'].apply(cls.is_french_number)].copy()
            foreign_removed = before_filter - len(df)
        else:
            foreign_removed = 0
        
        df = df.drop(columns=['quality_score'])
        
        stats = {
            'initial_count': initial_count,
            'duplicates_removed': duplicates_removed,
            'foreign_numbers_removed': foreign_removed,
            'final_count': len(df),
            'reduction_percentage': ((initial_count - len(df)) / initial_count * 100),
            'has_email_count': df['client_email'].notna().sum(),
            'email_percentage': (df['client_email'].notna().sum() / len(df) * 100),
            'has_first_name_count': df['first_name'].notna().sum(),
            'first_name_percentage': (df['first_name'].notna().sum() / len(df) * 100)
        }
        
        return df, stats
