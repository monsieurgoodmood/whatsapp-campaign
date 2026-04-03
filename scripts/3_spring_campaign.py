#!/usr/bin/env python3
"""
Campagne Printemps/Été 2026 - Elit Parking
Envoi WhatsApp aux contacts SANS email (WhatsApp = seul canal disponible)
A/B Test : 1 000 contacts Template A + 1 000 contacts Template B
Tracking complet : date, template reçu, statut
"""

import os
import sys
import pandas as pd
import re
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.whatsapp_sender import create_sender_from_env

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BATCH_SIZE        = 1000                                        # 1 000 par template
CAMPAIGN_NAME     = os.getenv('CAMPAIGN_NAME_ETE', 'printemps_ete_2026')
PROMO_CODE        = os.getenv('PROMO_CODE_ETE', 'PRINTEMPS2026')
PROMO_DISCOUNT    = os.getenv('PROMO_DISCOUNT_ETE', '15')
VALIDITY_DATE     = '31 août 2026'
TEMPLATE_A_SID    = os.getenv('TEMPLATE_ETE_A_SID')            # elit_printemps_offre
TEMPLATE_B_SID    = os.getenv('TEMPLATE_ETE_B_SID')            # elit_printemps_complicite
RAW_DATA_FILE     = 'data/raw_contacts.csv'
LOG_FILE          = 'data/campaign_log.csv'
MIN_DAYS_BETWEEN  = 30                                          # jours minimum entre 2 envois
FALLBACK_NAME     = 'Cher voyageur'                             # si prénom inconnu
# ──────────────────────────────────────────────────────────────────────────────

os.makedirs('logs', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/spring_campaign_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def fix_phone(p):
    if pd.isna(p): return None
    p = str(p).strip()
    if not p.startswith('+'): p = '+' + p
    return p if re.match(r'^\+\d{10,15}$', p) else None

def clean_name(name):
    if pd.isna(name): return None
    name = str(name).strip()
    name = re.sub(r'\s*\([^)]*\)', '', name)
    for pat in [r'\s+(nous doit|doit)\s+\d+€.*$', r'\s+P\d+.*$',
                r'\s+(ROUTE|LAVAGE|INTE|BAC|portail|clef gardee).*$']:
        name = re.sub(pat, '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    return name if len(name) >= 2 else None

def extract_first_name(full_name):
    if pd.isna(full_name): return FALLBACK_NAME
    full_name = str(full_name).strip()
    m = re.match(r'^([A-Z][a-zéèêëàâäôöûüçñ-]+)\s+[A-Z]', full_name)
    if m: return m.group(1)
    m = re.match(r'^[A-Z]+\s+([A-Z][a-zéèêëàâäôöûüçñ-]+)', full_name)
    if m: return m.group(1)
    words = full_name.split()
    if not words: return FALLBACK_NAME
    skip = ['M.', 'Mme', 'Madame', 'Monsieur', 'Dr', 'Mr']
    first = words[1] if words[0] in skip and len(words) > 1 else words[0]
    return first.capitalize()

def sanitize_name(name: str) -> str:
    """Évite les caractères spéciaux qui cassent le JSON Twilio"""
    cleaned = re.sub(r'["\\\n\r\t]', '', str(name)).strip()
    return cleaned if cleaned else FALLBACK_NAME


# ─── LOG ──────────────────────────────────────────────────────────────────────

def load_campaign_log() -> pd.DataFrame:
    if os.path.exists(LOG_FILE):
        df = pd.read_csv(LOG_FILE, dtype=str)
        df['sent_at'] = pd.to_datetime(df['sent_at'])
        logger.info(f"Log chargé : {len(df):,} envois précédents")
        return df
    logger.info("Aucun log existant — premier batch")
    return pd.DataFrame(columns=['client_phone', 'campaign', 'template', 'sent_at', 'status', 'batch_number'])

def save_to_log(results: list, template_id: str, batch_number: int):
    now = datetime.now().isoformat()
    rows = [{
        'client_phone': r['to'],
        'campaign':     CAMPAIGN_NAME,
        'template':     template_id,
        'sent_at':      now,
        'status':       r['status'],
        'batch_number': batch_number
    } for r in results]

    df_new = pd.DataFrame(rows)
    if os.path.exists(LOG_FILE):
        df_existing = pd.read_csv(LOG_FILE, dtype=str)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(LOG_FILE, index=False)
    logger.info(f"Log mis à jour : {len(df_new):,} envois template {template_id} sauvegardés")

def get_next_batch_number(log_df: pd.DataFrame) -> int:
    if log_df.empty: return 1
    campaign_logs = log_df[log_df['campaign'] == CAMPAIGN_NAME]
    if campaign_logs.empty: return 1
    return int(campaign_logs['batch_number'].astype(int).max()) + 1


# ─── DONNÉES ──────────────────────────────────────────────────────────────────

def prepare_contacts(log_df: pd.DataFrame) -> pd.DataFrame:
    """Charge, nettoie et filtre les contacts éligibles"""

    logger.info(f"Chargement : {RAW_DATA_FILE}")
    df = pd.read_csv(RAW_DATA_FILE, dtype=str)
    logger.info(f"Base brute : {len(df):,} contacts")

    # Pipeline nettoyage
    df['client_phone'] = df['client_phone'].apply(fix_phone)
    df = df[df['client_phone'].notna()].copy()
    df = df.drop_duplicates(subset='client_phone', keep='first')
    df = df[df['client_phone'].str.startswith('+33')].copy()
    df['client_name'] = df.get('client_name', df.get('nom', '')).fillna('')
    df = df[df['client_name'].str.len() > 1].copy()
    df['first_name'] = df.get('prenom', df['client_name']).fillna(FALLBACK_NAME).apply(sanitize_name)

    # Filtre SANS email uniquement
    no_email = df['client_email'].isna() | (df['client_email'].str.strip() == '')
    df = df[no_email].copy()
    logger.info(f"Sans email : {len(df):,} contacts")

    # Mobiles FR uniquement (+336 / +337)
    df = df[df['client_phone'].str.match(r'^\+33[67]')].copy()
    logger.info(f"Mobiles FR : {len(df):,} contacts")

    # Exclure déjà contactés récemment
    if not log_df.empty:
        camp_log = log_df[log_df['campaign'] == CAMPAIGN_NAME].copy()
        if not camp_log.empty:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=MIN_DAYS_BETWEEN)
            recent = camp_log[camp_log['sent_at'] > cutoff]['client_phone'].tolist()
            before = len(df)
            df = df[~df['client_phone'].isin(recent)].copy()
            logger.info(f"Exclus (contactés < {MIN_DAYS_BETWEEN}j) : {before - len(df):,} contacts")

    logger.info(f"Éligibles : {len(df):,} contacts")
    return df


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("🌸 ELIT PARKING - CAMPAGNE PRINTEMPS/ÉTÉ 2026")
    print("=" * 70)
    print(f"\n   Code promo    : {PROMO_CODE}")
    print(f"   Réduction     : -{PROMO_DISCOUNT}%")
    print(f"   Validité      : {VALIDITY_DATE}")
    print(f"   Batch A/B     : {BATCH_SIZE:,} contacts chacun ({BATCH_SIZE*2:,} total)")
    print(f"   Cible         : contacts SANS email uniquement")
    print(f"   Fallback nom  : '{FALLBACK_NAME}'")

    # Vérifications SID
    if not TEMPLATE_A_SID or not TEMPLATE_A_SID.startswith('HX'):
        print("\n❌ TEMPLATE_ETE_A_SID manquant ou invalide dans .env !")
        print("   → Crée le template 'elit_printemps_offre' dans Twilio puis ajoute son SID")
        sys.exit(1)
    if not TEMPLATE_B_SID or not TEMPLATE_B_SID.startswith('HX'):
        print("\n❌ TEMPLATE_ETE_B_SID manquant ou invalide dans .env !")
        print("   → Crée le template 'elit_printemps_complicite' dans Twilio puis ajoute son SID")
        sys.exit(1)

    # Charger log
    log_df = load_campaign_log()
    batch_number = get_next_batch_number(log_df)
    print(f"\n   Batch n°      : {batch_number}")

    # Préparer contacts
    df_eligible = prepare_contacts(log_df)

    if len(df_eligible) == 0:
        print("\n✅ Aucun contact éligible disponible.")
        sys.exit(0)

    if len(df_eligible) < BATCH_SIZE * 2:
        print(f"\n⚠️  Seulement {len(df_eligible):,} contacts éligibles (besoin de {BATCH_SIZE*2:,})")

    # Découper A/B
    df_a = df_eligible.iloc[:BATCH_SIZE].copy()
    df_b = df_eligible.iloc[BATCH_SIZE:BATCH_SIZE*2].copy()

    print(f"\n📋 Répartition :")
    print(f"   Template A (elit_printemps_offre)       : {len(df_a):,} contacts")
    print(f"   Template B (elit_printemps_complicite)  : {len(df_b):,} contacts")

    # Aperçu
    print("\n👀 Aperçu Template A (5 premiers) :")
    for _, row in df_a.head(5).iterrows():
        print(f"   {row['first_name']:20} {row['client_phone']}")
    print("\n👀 Aperçu Template B (5 premiers) :")
    for _, row in df_b.head(5).iterrows():
        print(f"   {row['first_name']:20} {row['client_phone']}")

    # Confirmation
    print(f"\n⚠️  Sur le point d'envoyer {len(df_a) + len(df_b):,} messages WhatsApp.")
    print(f"   Template A SID : {TEMPLATE_A_SID}")
    print(f"   Template B SID : {TEMPLATE_B_SID}")
    response = input("\n   Tape 'YES' pour confirmer : ")
    if response.strip() != 'YES':
        print("\n❌ Annulé.")
        sys.exit(0)

    sender = create_sender_from_env()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    all_results = {}

    # ── Envoi Template A ──
    print(f"\n{'='*70}")
    print(f"📤 ENVOI TEMPLATE A — {len(df_a):,} contacts")
    print(f"{'='*70}")
    contacts_a = df_a[['client_phone', 'first_name']].to_dict('records')
    results_a = sender.send_batch(contacts=contacts_a, template_sid=TEMPLATE_A_SID)
    save_to_log(results_a['detailed_results'], 'A', batch_number)
    all_results['template_A'] = {
        'sent': results_a['sent'],
        'failed': results_a['failed'],
        'success_rate': results_a['success_rate']
    }
    print(f"✓ Template A : {results_a['sent']:,} envoyés ({results_a['success_rate']:.1f}%)")

    # ── Envoi Template B ──
    print(f"\n{'='*70}")
    print(f"📤 ENVOI TEMPLATE B — {len(df_b):,} contacts")
    print(f"{'='*70}")
    contacts_b = df_b[['client_phone', 'first_name']].to_dict('records')
    results_b = sender.send_batch(contacts=contacts_b, template_sid=TEMPLATE_B_SID)
    save_to_log(results_b['detailed_results'], 'B', batch_number)
    all_results['template_B'] = {
        'sent': results_b['sent'],
        'failed': results_b['failed'],
        'success_rate': results_b['success_rate']
    }
    print(f"✓ Template B : {results_b['sent']:,} envoyés ({results_b['success_rate']:.1f}%)")

    # ── Résultats JSON ──
    results_file = f'outputs/spring_batch{batch_number}_{timestamp}.json'
    with open(results_file, 'w') as f:
        json.dump({
            'batch_number':  batch_number,
            'campaign':      CAMPAIGN_NAME,
            'promo_code':    PROMO_CODE,
            'sent_at':       timestamp,
            'template_A':    all_results['template_A'],
            'template_B':    all_results['template_B'],
            'total_sent':    results_a['sent'] + results_b['sent'],
            'total_failed':  results_a['failed'] + results_b['failed'],
        }, f, indent=2, ensure_ascii=False)

    # ── Résumé final ──
    total_sent   = results_a['sent'] + results_b['sent']
    total_failed = results_a['failed'] + results_b['failed']
    total        = total_sent + total_failed

    print("\n" + "=" * 70)
    print(f"✅ BATCH {batch_number} TERMINÉ")
    print("=" * 70)
    print(f"   Template A  : {results_a['sent']:,} envoyés ({results_a['success_rate']:.1f}%)")
    print(f"   Template B  : {results_b['sent']:,} envoyés ({results_b['success_rate']:.1f}%)")
    print(f"   Total       : {total_sent:,} / {total:,}")
    print(f"   Log         : {LOG_FILE}")
    print(f"   Résultats   : {results_file}")
    remaining = len(df_eligible) - (BATCH_SIZE * 2)
    if remaining > 0:
        print(f"\n   Contacts restants éligibles : {remaining:,}")


if __name__ == '__main__':
    main()