#!/usr/bin/env python3
"""
Script de filtrage WhatsApp - Campagne Noël 2025
Exclut les contacts Brevo (par email ET téléphone) et génère la liste WhatsApp
"""

import pandas as pd
from datetime import datetime
from pathlib import Path

def clean_phone(phone):
    """Nettoie et normalise un numéro de téléphone"""
    if pd.isna(phone):
        return None
    
    phone = str(phone).strip()
    
    # Retirer tous les espaces, tirets, points
    phone = phone.replace(' ', '').replace('-', '').replace('.', '')
    
    # Si commence par +, garder tel quel
    if phone.startswith('+'):
        return phone
    
    # Si commence par 33, ajouter +
    if phone.startswith('33'):
        return '+' + phone
    
    # Si commence par 0, remplacer par +33
    if phone.startswith('0'):
        return '+33' + phone[1:]
    
    return '+33' + phone

def filter_whatsapp_contacts():
    """Filtre les contacts pour WhatsApp en excluant les emails Brevo"""
    
    print("="*70)
    print("🔧 FILTRAGE CONTACTS WHATSAPP - CAMPAGNE NOËL 2025 ELIT")
    print("="*70)
    
    # Chemins
    CLEANED_FILE = Path("data/cleaned_contacts.csv")
    BREVO_FILE = Path("data/brevo_emails_sent.csv")  # Le fichier uploadé
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_FILE = Path(f"outputs/whatsapp_contacts_{timestamp}.csv")
    
    # 1. Charger la base nettoyée
    print(f"\n📂 ÉTAPE 1 : Chargement de la base nettoyée")
    print(f"   Fichier : {CLEANED_FILE}")
    
    if not CLEANED_FILE.exists():
        print(f"❌ ERREUR : {CLEANED_FILE} non trouvé !")
        return
    
    df_all = pd.read_csv(CLEANED_FILE)
    print(f"   ✅ {len(df_all):,} contacts chargés")
    print(f"   Colonnes : {', '.join(df_all.columns.tolist())}")
    
    # 2. Charger le fichier Brevo
    print(f"\n📧 ÉTAPE 2 : Chargement du fichier Brevo")
    print(f"   Fichier : {BREVO_FILE}")
    
    if not BREVO_FILE.exists():
        print(f"❌ ERREUR : {BREVO_FILE} non trouvé !")
        print(f"\n📝 Place le fichier Brevo ici :")
        print(f"   {BREVO_FILE.absolute()}")
        return
    
    # Lire avec séparateur point-virgule
    df_brevo = pd.read_csv(BREVO_FILE, sep=';')
    print(f"   ✅ {len(df_brevo):,} contacts Brevo chargés")
    print(f"   Colonnes : {', '.join(df_brevo.columns.tolist())}")
    
    # 3. Extraire emails et téléphones Brevo
    print(f"\n🔍 ÉTAPE 3 : Extraction des emails et téléphones Brevo")
    
    # Emails
    if 'EMAIL' in df_brevo.columns:
        df_brevo['email_normalized'] = df_brevo['EMAIL'].str.lower().str.strip()
        emails_sent = set(df_brevo['email_normalized'].dropna())
    else:
        print("⚠️  Colonne EMAIL non trouvée dans Brevo")
        emails_sent = set()
    
    print(f"   ✅ {len(emails_sent):,} emails à exclure")
    
    # Téléphones
    if 'SMS' in df_brevo.columns:
        df_brevo['phone_normalized'] = df_brevo['SMS'].apply(clean_phone)
        phones_sent = set(df_brevo['phone_normalized'].dropna())
    else:
        print("⚠️  Colonne SMS non trouvée dans Brevo")
        phones_sent = set()
    
    print(f"   ✅ {len(phones_sent):,} téléphones à exclure")
    
    # 4. Normaliser la base complète
    print(f"\n🧹 ÉTAPE 4 : Normalisation de la base complète")
    
    df_all['email_normalized'] = df_all['client_email'].str.lower().str.strip()
    df_all['phone_normalized'] = df_all['client_phone'].apply(clean_phone)
    
    print(f"   ✅ Emails et téléphones normalisés")
    
    # 5. Exclure les contacts Brevo
    print(f"\n❌ ÉTAPE 5 : Exclusion des contacts Brevo")
    
    # Marquer ceux qui ont reçu l'email (par email OU téléphone)
    df_all['received_email'] = (
        df_all['email_normalized'].isin(emails_sent) |
        df_all['phone_normalized'].isin(phones_sent)
    )
    
    excluded_count = df_all['received_email'].sum()
    df_filtered = df_all[~df_all['received_email']].copy()
    
    print(f"   ✅ {excluded_count:,} contacts exclus (email OU téléphone)")
    print(f"   📊 Reste : {len(df_filtered):,} contacts")
    
    # 6. Garder seulement téléphones valides
    print(f"\n📱 ÉTAPE 6 : Filtrage téléphones valides")
    
    df_whatsapp = df_filtered[
        (df_filtered['is_valid_phone'] == True) | 
        (df_filtered['is_valid_phone'] == 1) |
        (df_filtered['is_valid_phone'] == '1') |
        (df_filtered['is_valid_phone'] == 'True')
    ].copy()
    
    excluded_no_phone = len(df_filtered) - len(df_whatsapp)
    print(f"   ✅ {excluded_no_phone:,} sans téléphone valide exclus")
    print(f"   📊 Reste : {len(df_whatsapp):,} contacts")
    
    # 7. Dédupliquer par téléphone
    print(f"\n🔄 ÉTAPE 7 : Déduplication par téléphone")
    
    before_dedup = len(df_whatsapp)
    df_whatsapp = df_whatsapp.drop_duplicates(subset=['phone_normalized'])
    duplicates = before_dedup - len(df_whatsapp)
    
    print(f"   ✅ {duplicates:,} doublons retirés")
    print(f"   📊 Final : {len(df_whatsapp):,} contacts WhatsApp uniques")
    
    # 8. Préparer l'export
    print(f"\n📤 ÉTAPE 8 : Préparation de l'export")
    
    df_export = df_whatsapp[[
        'id',
        'phone_normalized',
        'first_name',
        'client_email',
        'quality_score'
    ]].copy()
    
    df_export = df_export.rename(columns={
        'phone_normalized': 'whatsapp_number',
        'first_name': 'firstname'
    })
    
    # Remplacer prénoms vides par "voyageur"
    df_export['firstname'] = df_export['firstname'].fillna('voyageur')
    df_export['firstname'] = df_export['firstname'].replace('', 'voyageur')
    
    print(f"   ✅ Colonnes préparées : {', '.join(df_export.columns.tolist())}")
    
    # 9. Sauvegarder
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    df_export.to_csv(OUTPUT_FILE, index=False)
    
    print(f"   ✅ Fichier sauvegardé : {OUTPUT_FILE}")
    
    # 10. Statistiques finales
    print(f"\n{'='*70}")
    print("📊 STATISTIQUES FINALES")
    print(f"{'='*70}")
    print(f"Base complète (cleaned)     : {len(df_all):>8,} contacts")
    print(f"Contacts Brevo (email)      : {len(df_brevo):>8,} contacts")
    print(f"")
    print(f"Exclus (email/téléphone)    : {excluded_count:>8,} contacts")
    print(f"Sans téléphone valide       : {excluded_no_phone:>8,} contacts")
    print(f"Doublons téléphone          : {duplicates:>8,} contacts")
    print(f"─" * 70)
    print(f"✅ CONTACTS WHATSAPP FINAL  : {len(df_export):>8,} contacts")
    print(f"{'='*70}")
    
    # Taux
    taux_exclusion = excluded_count / len(df_all) * 100
    taux_whatsapp = len(df_export) / len(df_all) * 100
    
    print(f"Taux exclusion Brevo        : {taux_exclusion:>7.1f}%")
    print(f"Taux WhatsApp final         : {taux_whatsapp:>7.1f}%")
    print(f"{'='*70}")
    
    # 11. Aperçu
    print(f"\n👀 APERÇU (5 premiers contacts) :")
    print(df_export.head().to_string(index=False))
    
    # 12. Distribution quality_score
    if 'quality_score' in df_export.columns:
        print(f"\n📊 Distribution Quality Score :")
        print(df_export['quality_score'].value_counts().sort_index().to_string())
    
    print(f"\n{'='*70}")
    print(f"✅ FILTRAGE TERMINÉ !")
    print(f"{'='*70}")
    print(f"📁 Fichier de sortie : {OUTPUT_FILE}")
    print(f"📱 Prêt pour campagne WhatsApp")
    print(f"{'='*70}")
    
    return df_export

if __name__ == "__main__":
    filter_whatsapp_contacts()