# 🎄 Elit Parking - WhatsApp Marketing Campaign

> Professional WhatsApp Business API automation for seasonal marketing campaigns

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## 📋 Overview

Automated WhatsApp marketing system for **Elit Parking** (Lyon Airport), featuring:

- ✅ **A/B/C Testing** with 3 psychological messaging approaches
- ✅ **Smart Name Extraction** for personalized messages  
- ✅ **Data Quality Control** with deduplication and validation
- ✅ **UTM Tracking** for conversion analytics
- ✅ **Rate Limiting** to respect WhatsApp API limits
- ✅ **Test Mode** for safe campaign validation

## 🎯 Campaign Details

**Promotion**: Christmas/New Year 2025 - 15% discount code `NOEL15`  
**Target Audience**: 75,597 unique French customers  
**Platform**: Twilio WhatsApp Business API  
**Validity**: Until January 15, 2026

### A/B/C Test Strategy

| Template | Focus | Psychology |
|----------|-------|------------|
| **A - elit_noel_offre_choc** | Value proposition | Rational decision-making |
| **B - elit_noel_urgence** | Urgency & FOMO | Scarcity principle |
| **C - elit_noel_solution** | Problem-Solution | Pain point resolution |

## 🚀 Quick Start

### Prerequisites
```bash
python3 --version  # Python 3.8+
# Twilio account with WhatsApp Business API
# Meta WhatsApp Business Account (approved)
```

### Installation
```bash
git clone https://github.com/yourusername/elit-whatsapp-campaign.git
cd elit-whatsapp-campaign

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your credentials
```

## 📊 Usage

### 1. Prepare Data
```bash
python scripts/1_prepare_data.py --input data/your_contacts.csv
```

### 2. Test Campaign
```bash
python scripts/2_send_campaign.py --test --limit 5
```

### 3. Launch Campaign
```bash
python scripts/2_send_campaign.py
```

## 📁 Project Structure
```
elit-whatsapp-campaign/
├── README.md
├── config/
│   └── templates.py       # WhatsApp templates
├── src/
│   ├── data_processor.py  # Data cleaning
│   ├── ab_test_splitter.py # A/B/C groups
│   └── whatsapp_sender.py # Twilio API
├── scripts/
│   ├── 1_prepare_data.py
│   └── 2_send_campaign.py
└── data/
    └── sample_data.csv
```

## 🔒 Security

**NEVER commit**:
- `.env` files
- Customer data
- Campaign results

See [SECURITY.md](SECURITY.md) for details.

## 📊 Expected Results

- **Total contacts**: 75,597
- **Cost**: ~$378
- **Duration**: 2-3 hours
- **Target ROI**: >300%

## 📝 License

MIT License - see [LICENSE](LICENSE)

## 👨‍💻 Author

**Arthur Choisnet**  
Data Scientist | Marketing Automation Specialist

- Email: byteberry.analytics@gmail.com
- GitHub: [@monsieurgoodmood](https://github.com/yourusername)
