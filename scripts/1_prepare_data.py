#!/usr/bin/env python3
"""Data Preparation Script"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_processor import DataProcessor
from src.ab_test_splitter import ABTestSplitter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Prepare customer data for WhatsApp campaign')
    parser.add_argument('--input', default='data/raw_contacts.csv', help='Input CSV file path')
    parser.add_argument('--output-dir', default='outputs', help='Output directory')
    parser.add_argument('--all-countries', action='store_true', help='Keep all countries')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("=" * 70)
    print("🧹 ELIT PARKING - DATA PREPARATION")
    print("=" * 70)
    
    print(f"\n📂 Loading data from: {args.input}")
    df = pd.read_csv(args.input)
    print(f"   ✓ Loaded {len(df):,} raw records")
    
    print("\n🔧 Processing database...")
    df_clean, stats = DataProcessor.process_database(df, french_only=not args.all_countries)
    
    print("\n📊 PROCESSING STATISTICS:")
    print(f"   Initial records      : {stats['initial_count']:,}")
    print(f"   Duplicates removed   : {stats['duplicates_removed']:,}")
    if not args.all_countries:
        print(f"   Foreign numbers removed : {stats['foreign_numbers_removed']:,}")
    print(f"   Final contacts       : {stats['final_count']:,}")
    print(f"   Reduction            : {stats['reduction_percentage']:.1f}%")
    print(f"\n   With email           : {stats['has_email_count']:,} ({stats['email_percentage']:.1f}%)")
    print(f"   With first name      : {stats['has_first_name_count']:,} ({stats['first_name_percentage']:.1f}%)")
    
    print("\n🔀 Splitting into A/B/C test groups...")
    df_final = ABTestSplitter.split_contacts(df_clean)
    
    group_stats = ABTestSplitter.get_group_statistics(df_final)
    print("\n   Group distribution:")
    for group, gstats in sorted(group_stats.items()):
        print(f"   Group {group}: {gstats['total_contacts']:,} contacts ({gstats['percentage']:.1f}%)")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(args.output_dir, f'prepared_contacts_{timestamp}.csv')
    
    print(f"\n💾 Saving prepared data to: {output_file}")
    df_final.to_csv(output_file, index=False)
    print(f"   ✓ Saved {len(df_final):,} contacts")
    
    cost_per_msg = 0.005
    total_cost = stats['final_count'] * cost_per_msg
    
    print("\n💰 COST ESTIMATE:")
    print(f"   Total contacts : {stats['final_count']:,}")
    print(f"   Cost per message : ${cost_per_msg}")
    print(f"   Total campaign cost : ${total_cost:.2f}")
    
    print("\n" + "=" * 70)
    print("✅ DATA PREPARATION COMPLETE!")
    print("=" * 70)
    print(f"\nNext: python scripts/2_send_campaign.py --input {output_file}")


if __name__ == '__main__':
    main()
