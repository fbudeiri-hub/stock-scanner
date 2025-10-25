"""
Rate limiter module for API calls
Ensures we stay within provider rate limits
"""

import time
from collections import defaultdict
from datetime import datetime, timedelta

class RateLimiter:
    """
    Manages rate limiting for multiple API providers
    Tracks calls per minute and per day
    """
    
    def __init__(self):
        self.minute_calls = defaultdict(list)
        self.day_calls = defaultdict(int)
        self.wait_until = defaultdict(float)
        
    def wait_if_needed(self, provider, calls_per_min, calls_per_day):
        """Check if we need to wait before making API call"""
        now = datetime.now()
        
        # Clean old calls from tracking
        cutoff = now - timedelta(minutes=1)
        self.minute_calls[provider] = [
            t for t in self.minute_calls[provider] 
            if t > cutoff
        ]
        
        # Check minute limit
        if len(self.minute_calls[provider]) >= calls_per_min:
            sleep_time = 60.1 - (now - self.minute_calls[provider]).total_seconds()
            if sleep_time > 0:
                print(f"[INFO] {provider}: Rate limit. Waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                
    def record_call(self, provider):
        """Record an API call"""
        now = datetime.now()
        self.minute_calls[provider].append(now)
        self.day_calls[provider] += 1
