"""WhatsApp Sender Module - Twilio API integration"""

import time
import logging
from typing import Dict, Optional, List
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import os

logger = logging.getLogger(__name__)


class WhatsAppSender:
    """Sends WhatsApp messages via Twilio API"""
    
    def __init__(self, account_sid: str, auth_token: str, whatsapp_number: str, rate_limit: int = 10):
        self.client = Client(account_sid, auth_token)
        self.whatsapp_number = whatsapp_number
        self.rate_limit = rate_limit
        self.min_interval = 1.0 / rate_limit if rate_limit > 0 else 0
        self.last_send_time = 0
        
        self.stats = {'sent': 0, 'failed': 0, 'errors': []}
        
        logger.info(f"WhatsApp Sender initialized (rate: {rate_limit} msg/sec)")
    
    def _enforce_rate_limit(self):
        if self.rate_limit > 0:
            elapsed = time.time() - self.last_send_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
    
    def send_template_message(self, to_number: str, template_sid: str, first_name: str, retry_count: int = 3) -> Dict:
        self._enforce_rate_limit()
        
        result = {
            'to': to_number,
            'first_name': first_name,
            'template_sid': template_sid,
            'status': 'unknown',
            'message_sid': None,
            'error': None
        }
        
        from_whatsapp = f"whatsapp:{self.whatsapp_number}"
        to_whatsapp = f"whatsapp:{to_number}"
        
        for attempt in range(retry_count):
            try:
                message = self.client.messages.create(
                    from_=from_whatsapp,
                    to=to_whatsapp,
                    content_sid=template_sid,
                    content_variables=f'{{"1":"{first_name}"}}'
                )
                
                result['status'] = 'sent'
                result['message_sid'] = message.sid
                self.stats['sent'] += 1
                
                logger.info(f"✓ Sent to {to_number} (SID: {message.sid})")
                
                self.last_send_time = time.time()
                return result
                
            except TwilioRestException as e:
                result['error'] = {'code': e.code, 'message': str(e.msg), 'attempt': attempt + 1}
                
                retryable_codes = [20429, 20003, 20005]
                
                if e.code in retryable_codes and attempt < retry_count - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠ Retry {attempt + 1}/{retry_count} for {to_number}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    result['status'] = 'failed'
                    self.stats['failed'] += 1
                    self.stats['errors'].append(result['error'])
                    logger.error(f"✗ Failed to send to {to_number}: Error {e.code} - {e.msg}")
                    return result
            
            except Exception as e:
                result['status'] = 'failed'
                result['error'] = {'code': 'UNEXPECTED', 'message': str(e), 'attempt': attempt + 1}
                self.stats['failed'] += 1
                self.stats['errors'].append(result['error'])
                logger.error(f"✗ Unexpected error for {to_number}: {e}")
                return result
        
        return result
    
    def send_batch(self, contacts: List[Dict], template_sid: str, test_mode: bool = False, test_limit: int = 5) -> Dict:
        if test_mode:
            logger.warning(f"🧪 TEST MODE: Limiting to {test_limit} messages")
            contacts = contacts[:test_limit]
        
        logger.info(f"Starting batch send: {len(contacts):,} contacts")
        
        results = []
        start_time = time.time()
        
        for i, contact in enumerate(contacts, 1):
            phone = contact.get('client_phone')
            first_name = contact.get('first_name', 'Client')
            
            if not phone:
                logger.warning(f"Skipping contact {i}: No phone number")
                continue
            
            result = self.send_template_message(to_number=phone, template_sid=template_sid, first_name=first_name)
            results.append(result)
            
            if i % 100 == 0:
                logger.info(f"Progress: {i:,}/{len(contacts):,} ({i/len(contacts)*100:.1f}%)")
        
        elapsed_time = time.time() - start_time
        
        summary = {
            'total_attempted': len(results),
            'sent': self.stats['sent'],
            'failed': self.stats['failed'],
            'success_rate': (self.stats['sent'] / len(results) * 100) if results else 0,
            'elapsed_time_seconds': elapsed_time,
            'messages_per_second': len(results) / elapsed_time if elapsed_time > 0 else 0,
            'errors': self.stats['errors'],
            'detailed_results': results
        }
        
        logger.info(f"Batch complete: {summary['sent']:,} sent, {summary['failed']:,} failed ({summary['success_rate']:.1f}% success)")
        
        return summary
    
    def get_stats(self) -> Dict:
        return self.stats.copy()


def create_sender_from_env() -> WhatsAppSender:
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    whatsapp_number = os.getenv('TWILIO_WHATSAPP_NUMBER')
    rate_limit = int(os.getenv('RATE_LIMIT', '10'))
    
    if not all([account_sid, auth_token, whatsapp_number]):
        raise ValueError("Missing required environment variables")
    
    return WhatsAppSender(account_sid=account_sid, auth_token=auth_token, whatsapp_number=whatsapp_number, rate_limit=rate_limit)
