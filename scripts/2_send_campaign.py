#!/usr/bin/env python3
"""Campaign Launch Script"""

import os
import sys
from dotenv import load_dotenv

# CHARGER .ENV EN PREMIER !
load_dotenv()

import argparse
import pandas as pd
from datetime import datetime
import logging
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.whatsapp_sender import create_sender_from_env
from config.templates import WhatsAppTemplates

log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'campaign_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def validate_environment():
    required_vars = ['TWILIO_ACCOUNT_SID', 'TWILIO_AUTH_TOKEN', 'TWILIO_WHATSAPP_NUMBER', 'TEMPLATE_A_SID', 'TEMPLATE_B_SID', 'TEMPLATE_C_SID']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logger.error("Missing required environment variables:")
        for var in missing:
            logger.error(f"  - {var}")
        return False
    
    template_validation = WhatsAppTemplates.validate_configuration()
    for template, is_valid in template_validation.items():
        if not is_valid:
            logger.error(f"Template {template} SID is invalid")
            return False
    
    return True


def find_latest_prepared_file(data_dir='outputs'):
    files = [f for f in os.listdir(data_dir) if f.startswith('prepared_contacts_') and f.endswith('.csv')]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join(data_dir, files[0])


def main():
    parser = argparse.ArgumentParser(description='Launch WhatsApp campaign')
    parser.add_argument('--input', help='Input CSV file')
    parser.add_argument('--group', choices=['A', 'B', 'C', 'ALL'], default='ALL')
    parser.add_argument('--test', action='store_true', help='Test mode')
    parser.add_argument('--limit', type=int, default=5, help='Test limit')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("📱 ELIT PARKING - WHATSAPP CAMPAIGN LAUNCHER")
    print("=" * 70)
    
    print("\n🔐 Validating environment...")
    if not validate_environment():
        sys.exit(1)
    print("   ✓ Environment validated")
    
    input_file = args.input or find_latest_prepared_file()
    if not input_file or not os.path.exists(input_file):
        logger.error("No prepared file found")
        sys.exit(1)
    
    print(f"   ✓ Loading: {input_file}")
    df = pd.read_csv(input_file)
    print(f"   ✓ Loaded {len(df):,} contacts")
    
    if args.group != 'ALL':
        df = df[df['test_group'] == args.group].copy()
    
    print("\n📊 CAMPAIGN SUMMARY:")
    print(f"   MODE: {'TEST' if args.test else 'PRODUCTION'}")
    print(f"   Contacts: {len(df):,}")
    
    if not args.test:
        response = input("\nType 'YES' to confirm: ")
        if response != 'YES':
            print("\n❌ Cancelled")
            sys.exit(0)
    
    print("\n📲 Initializing sender...")
    sender = create_sender_from_env()
    
    print("\n🚀 Starting campaign...")
    all_results = {}
    
    groups_to_send = [args.group] if args.group != 'ALL' else ['A', 'B', 'C']
    
    for group in groups_to_send:
        group_df = df[df['test_group'] == group]
        if len(group_df) == 0:
            continue
        
        print(f"\n{'='*70}\n📤 SENDING TO GROUP {group}\n{'='*70}")
        
        template_config = WhatsAppTemplates.get_template_config(group)
        contacts = group_df[['client_phone', 'first_name']].to_dict('records')
        
        results = sender.send_batch(contacts=contacts, template_sid=template_config['sid'], test_mode=args.test, test_limit=args.limit)
        all_results[f'group_{group}'] = results
        
        print(f"\n✓ Group {group}: {results['sent']:,} sent, {results['failed']:,} failed")
    
    results_file = os.path.join('outputs', f'campaign_results_{timestamp}.json')
    with open(results_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print("\n" + "=" * 70)
    print("✅ CAMPAIGN COMPLETE!")
    print("=" * 70)


if __name__ == '__main__':
    main()
