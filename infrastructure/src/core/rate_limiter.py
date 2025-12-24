"""
Rate limiting and concurrency management for OpenAI API calls.
"""
import time
import threading
from dataclasses import dataclass
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

@dataclass
class RateLimitInfo:
    requests_per_minute: int = 5000  # Default for gpt-4o-mini
    tokens_per_minute: int = 200000  # Default for gpt-4o-mini
    current_requests: int = 0
    current_tokens: int = 0
    window_start: float = 0

class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts concurrency based on API responses."""
    
    def __init__(self):
        self.rate_info = RateLimitInfo()
        self.lock = threading.Lock()
        # Check environment variable for max parallel
        import os
        env_max = os.environ.get('OPENAI_MAX_PARALLEL', '20')
        try:
            self.max_concurrent = int(env_max)
        except:
            self.max_concurrent = 20  # Default if env var is invalid
        logger.info(f"Rate limiter initialized with max_concurrent={self.max_concurrent}")
        self.current_concurrent = 0
        self.error_count = 0
        self.success_count = 0
        
    def can_proceed(self, estimated_tokens: int = 1000) -> bool:
        """Check if we can make a request within rate limits."""
        with self.lock:
            now = time.time()
            # Reset window every minute
            if now - self.rate_info.window_start > 60:
                self.rate_info.current_requests = 0
                self.rate_info.current_tokens = 0
                self.rate_info.window_start = now
            
            # Check if we can make the request
            can_request = (
                self.rate_info.current_requests < self.rate_info.requests_per_minute * 0.95 and
                self.rate_info.current_tokens + estimated_tokens < self.rate_info.tokens_per_minute * 0.95 and
                self.current_concurrent < self.max_concurrent
            )
            
            if can_request:
                self.rate_info.current_requests += 1
                self.rate_info.current_tokens += estimated_tokens
                self.current_concurrent += 1
            
            return can_request
    
    def request_completed(self, success: bool, actual_tokens: int = 0):
        """Record completion of a request and adjust concurrency."""
        with self.lock:
            self.current_concurrent = max(0, self.current_concurrent - 1)
            
            if success:
                self.success_count += 1
                # Gradually increase concurrency on success
                if self.success_count % 10 == 0 and self.max_concurrent < 50:
                    self.max_concurrent += 1
                    logger.info(f"Increased max concurrent to {self.max_concurrent}")
            else:
                self.error_count += 1
                # Aggressively reduce on error
                self.max_concurrent = max(1, self.max_concurrent - 2)
                logger.warning(f"Reduced max concurrent to {self.max_concurrent}")
    
    def wait_for_capacity(self, estimated_tokens: int = 1000):
        """Wait until we have capacity for the request."""
        while not self.can_proceed(estimated_tokens):
            time.sleep(0.1)  # Very short wait for aggressive processing
    
    def configure(self, requests_per_minute: int, tokens_per_minute: int, max_concurrent: int = None):
        """Configure rate limits."""
        with self.lock:
            self.rate_info.requests_per_minute = requests_per_minute
            self.rate_info.tokens_per_minute = tokens_per_minute
            if max_concurrent:
                self.max_concurrent = max_concurrent

class RequestMonitor:
    """Monitor and track API request statistics."""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_tokens_used = 0
        self.avg_response_time = 0.0
        self.request_times = []
        self.start_time = time.time()
    
    def record_request(self, success: bool, tokens: int, response_time: float):
        """Record a completed request."""
        with self.lock:
            self.total_requests += 1
            self.total_tokens_used += tokens
            self.request_times.append(response_time)
            
            if success:
                self.successful_requests += 1
            else:
                self.failed_requests += 1
            
            # Keep only last 100 response times for moving average
            if len(self.request_times) > 100:
                self.request_times.pop(0)
            
            self.avg_response_time = sum(self.request_times) / len(self.request_times)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        with self.lock:
            elapsed = time.time() - self.start_time
            return {
                'total_requests': self.total_requests,
                'success_rate': (self.successful_requests / max(1, self.total_requests)) * 100,
                'tokens_used': self.total_tokens_used,
                'avg_response_time': self.avg_response_time,
                'requests_per_second': self.total_requests / max(1, elapsed),
                'tokens_per_second': self.total_tokens_used / max(1, elapsed),
                'concurrent_requests': 0  # Will be updated by rate limiter
            }
    
    def log_status(self, rate_limiter: AdaptiveRateLimiter = None):
        """Log current status."""
        stats = self.get_stats()
        if rate_limiter:
            stats['concurrent_requests'] = rate_limiter.current_concurrent
        
        logger.info(
            f"API Stats: {stats['total_requests']} reqs, "
            f"{stats['success_rate']:.1f}% success, "
            f"{stats['tokens_used']} tokens, "
            f"{stats['requests_per_second']:.1f} req/s, "
            f"{stats['concurrent_requests']} concurrent"
        )