"""A/B/C Test Splitter Module"""

import pandas as pd
import numpy as np
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


class ABTestSplitter:
    """Splits contacts into A/B/C test groups"""
    
    @staticmethod
    def split_contacts(df: pd.DataFrame, groups: list = ['A', 'B', 'C'], seed: int = 42) -> pd.DataFrame:
        logger.info(f"Splitting {len(df):,} contacts into {len(groups)} groups...")
        
        np.random.seed(seed)
        df_split = df.copy()
        
        df_split['test_group'] = np.random.choice(groups, size=len(df_split), replace=True)
        
        distribution = df_split['test_group'].value_counts().sort_index()
        logger.info("Group distribution:")
        for group, count in distribution.items():
            percentage = (count / len(df_split)) * 100
            logger.info(f"  Group {group}: {count:,} contacts ({percentage:.1f}%)")
        
        return df_split
    
    @staticmethod
    def get_group_statistics(df: pd.DataFrame) -> dict:
        if 'test_group' not in df.columns:
            raise ValueError("DataFrame must have 'test_group' column")
        
        stats = {}
        
        for group in sorted(df['test_group'].unique()):
            group_df = df[df['test_group'] == group]
            
            stats[group] = {
                'total_contacts': len(group_df),
                'percentage': (len(group_df) / len(df)) * 100,
                'has_email': group_df['client_email'].notna().sum(),
                'email_percentage': (group_df['client_email'].notna().sum() / len(group_df)) * 100,
                'has_first_name': group_df['first_name'].notna().sum() if 'first_name' in group_df.columns else 0
            }
        
        return stats
    
    @staticmethod
    def extract_group(df: pd.DataFrame, group: str) -> pd.DataFrame:
        if 'test_group' not in df.columns:
            raise ValueError("DataFrame must have 'test_group' column")
        
        if group not in df['test_group'].unique():
            raise ValueError(f"Group '{group}' not found in data")
        
        return df[df['test_group'] == group].copy()
