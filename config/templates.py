"""WhatsApp Message Templates Configuration"""

import os
from typing import Dict, List


class WhatsAppTemplates:
    """Configuration for WhatsApp message templates"""
    
    CAMPAIGN_NAME = os.getenv('CAMPAIGN_NAME', 'noel2025')
    PROMO_CODE = os.getenv('PROMO_CODE', 'NOEL15')
    BASE_URL = os.getenv('CAMPAIGN_URL', 'https://www.elit-parking.fr/')
    
    TEMPLATE_A_SID = os.getenv('TEMPLATE_A_SID')
    TEMPLATE_B_SID = os.getenv('TEMPLATE_B_SID')
    TEMPLATE_C_SID = os.getenv('TEMPLATE_C_SID')
    
    @classmethod
    def get_template_config(cls, template_id: str) -> Dict:
        templates = {
            'A': {
                'sid': cls.TEMPLATE_A_SID,
                'name': 'elit_noel_offre_choc',
                'utm_content': 'templateA',
                'focus': 'Value proposition - Rational decision-making',
            },
            'B': {
                'sid': cls.TEMPLATE_B_SID,
                'name': 'elit_noel_urgence',
                'utm_content': 'templateB',
                'focus': 'Urgency & FOMO - Scarcity principle',
            },
            'C': {
                'sid': cls.TEMPLATE_C_SID,
                'name': 'elit_noel_solution',
                'utm_content': 'templateC',
                'focus': 'Problem-Solution - Pain point resolution',
            }
        }
        
        if template_id not in templates:
            raise ValueError(f"Invalid template_id: {template_id}")
        
        return templates[template_id]
    
    @classmethod
    def get_tracking_url(cls, template_id: str) -> str:
        config = cls.get_template_config(template_id)
        utm_params = (
            f"?utm_source=whatsapp"
            f"&utm_campaign={cls.CAMPAIGN_NAME}"
            f"&utm_content={config['utm_content']}"
        )
        return cls.BASE_URL + utm_params
    
    @classmethod
    def get_all_templates(cls) -> List[str]:
        return ['A', 'B', 'C']
    
    @classmethod
    def validate_configuration(cls) -> Dict[str, bool]:
        return {
            'A': bool(cls.TEMPLATE_A_SID and cls.TEMPLATE_A_SID.startswith('HX')),
            'B': bool(cls.TEMPLATE_B_SID and cls.TEMPLATE_B_SID.startswith('HX')),
            'C': bool(cls.TEMPLATE_C_SID and cls.TEMPLATE_C_SID.startswith('HX')),
        }
