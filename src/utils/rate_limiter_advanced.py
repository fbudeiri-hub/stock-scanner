"""
Advanced rate limiter with provider quota tracking
Ensures compliance with each provider's declared limits
"""

import time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict

class AdvancedRateLimiter:
    """
    Manages rate limiting with provider-specific quotas
    
    Provider Limits (Free Tier):
    - Marketstack: 1,000/month or 100/day
    - Finnhub: 60/minute
    - Twelve Data: 800/day
    - FMP: 250/day
    - Tiingo: 1,000/day
    - Alpha Vantage: 5/minute (STRICTEST)
    - Polygon: Variable (use 500/day conservative)
    """
    
    PROVIDER_LIMITS = {
        'Marketstack': {
            'per_minute': 30,
            'per_day': 100,
            'delay_between_calls': 2.0
        },
        'Finnhub': {
            'per_minute': 50,
            'per_day': 2000,
            'delay_between_calls': 1.2
        },
        'Twelve Data': {
            'per_minute': 60,
            'per_day': 800,
            'delay_between_calls': 1.0
        },
        'FMP': {
            'per_minute': 10,
            'per_day': 250,
            'delay_between_calls': 6.0
        },
        'Tiingo': {
            'per_minute': 100,
            'per_day': 1000,
            'delay_between_calls': 0.6
        },
        'Polygon': {
            'per_minute': 50,
            'per_day': 500,
            'delay_between_calls': 1.2
        },
        'Alpha Vantage': {
            'per_minute': 5,
            'per_day': 500,
            'delay_between_calls': 12.0
        }
    }
    
    def __init__(self):
        self.call_times = defaultdict(list)
        self.daily_count = defaultdict(int)
        self.daily_reset_time = defaultdict(lambda: datetime.now())
        self.last_call_time = defaultdict(float)
        
    def can_call(self, provider_name: str) -> tuple:
        """
        Check if we can make a call to this provider
        Returns: (bool: can_call, str: reason_if_no)
        """
        if provider_name not in self.PROVIDER_LIMITS:
            return True, ""
        
        limits = self.PROVIDER_LIMITS[provider_name]
        now = datetime.now()
        
        # Check daily limit
        if now.date() != self.daily_reset_time[provider_name].date():
            self.daily_count[provider_name] = 0
            self.daily_reset_time[provider_name] = now
        
        if self.daily_count[provider_name] >= limits['per_day']:
            return False, f"Daily quota exceeded ({limits['per_day']}/day)"
        
        # Check minute limit
        cutoff = now - timedelta(minutes=1)
        recent_calls = [t for t in self.call_times[provider_name] if t > cutoff]
        
        if len(recent_calls) >= limits['per_minute']:
            return False, f"Minute quota exceeded ({limits['per_minute']}/min)"
        
        # Check delay requirement
        last_call = self.last_call_time[provider_name]
        time_since_last = time.time() - last_call if last_call else float('inf')
        
        if time_since_last < limits['delay_between_calls']:
            return False, f"Need {limits['delay_between_calls']:.1f}s delay (only {time_since_last:.1f}s since last call)"
        
        return True, ""
    
    def wait_until_ready(self, provider_name: str, max_wait: int = 60) -> bool:
        """
        Wait until we can make a call to this provider
        Returns: True if ready, False if max_wait exceeded
        """
        if provider_name not in self.PROVIDER_LIMITS:
            return True
        
        limits = self.PROVIDER_LIMITS[provider_name]
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            can_call, reason = self.can_call(provider_name)
            if can_call:
                return True
            
            wait_time = limits['delay_between_calls']
            print(f"[RATE LIMIT] {provider_name}: {reason}. Waiting {wait_time:.1f}s...")
            time.sleep(min(wait_time, 1.0))
        
        return False
    
    def record_call(self, provider_name: str):
        """Record a successful API call"""
        now = datetime.now()
        self.call_times[provider_name].append(now)
        self.last_call_time[provider_name] = time.time()
        self.daily_count[provider_name] += 1
        
        # Clean old call times
        cutoff = now - timedelta(days=1)
        self.call_times[provider_name] = [t for t in self.call_times[provider_name] if t > cutoff]
    
    def get_stats(self, provider_name: str) -> Dict:
        """Get quota statistics for a provider"""
        if provider_name not in self.PROVIDER_LIMITS:
            return {}
        
        limits = self.PROVIDER_LIMITS[provider_name]
        now = datetime.now()
        
        # Minute usage
        cutoff = now - timedelta(minutes=1)
        minute_usage = len([t for t in self.call_times[provider_name] if t > cutoff])
        
        # Daily usage
        if now.date() != self.daily_reset_time[provider_name].date():
            daily_usage = 0
        else:
            daily_usage = self.daily_count[provider_name]
        
        return {
            'minute_usage': f"{minute_usage}/{limits['per_minute']}",
            'daily_usage': f"{daily_usage}/{limits['per_day']}",
            'delay_required': f"{limits['delay_between_calls']:.1f}s"
        }
