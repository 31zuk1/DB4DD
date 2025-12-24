"""
OpenAI API client with rate limiting and structured outputs.
"""
import os
import json
import time
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Any
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
from openai import OpenAI, RateLimitError, APIError
from pydantic import BaseModel

from core.rate_limiter import AdaptiveRateLimiter, RequestMonitor

logger = logging.getLogger(__name__)

class APIClient:
    """OpenAI API client with rate limiting and caching."""
    
    def __init__(self, cache_dir: Path, rate_limiter: AdaptiveRateLimiter, monitor: RequestMonitor):
        self.cache_dir = cache_dir
        self.rate_limiter = rate_limiter
        self.monitor = monitor
        
        # Initialize OpenAI client with Cloudflare gateway if configured
        account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        gateway_id = os.getenv('CLOUDFLARE_GATEWAY_ID')
        
        if account_id and gateway_id and account_id != '{account_id}' and gateway_id != '{gateway_id}':
            base_url = f"https://gateway.ai.cloudflare.com/v1/{account_id}/{gateway_id}/openai"
            self.client = OpenAI(base_url=base_url)
            logger.info("Using Cloudflare AI Gateway")
        else:
            self.client = OpenAI()
            logger.info("Using direct OpenAI API")
        
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
    
    @retry(
        wait=wait_random_exponential(min=0.1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((RateLimitError, APIError))
    )
    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 1024, 
             temperature: float = 0.3, cache: bool = True) -> str:
        """Make a chat completion request with rate limiting and caching."""
        # Rate limiting
        estimated_tokens = sum(len(m['content']) // 4 for m in messages) + max_tokens
        self.rate_limiter.wait_for_capacity(estimated_tokens)
        
        # Cache key generation
        key = hashlib.sha256(''.join(m['content'] for m in messages).encode()).hexdigest()
        cache_file = self.cache_dir / f'{key}.json'
        
        if cache and cache_file.exists():
            try:
                self.rate_limiter.request_completed(True, 0)  # Cache hit
                return json.loads(cache_file.read_text(encoding='utf-8'))['content']
            except Exception:
                pass
        
        start_time = time.time()
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
            content = resp.choices[0].message.content.strip()
            response_time = time.time() - start_time
            
            self.rate_limiter.request_completed(True, estimated_tokens)
            self.monitor.record_request(True, estimated_tokens, response_time)
            
            if cache:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(
                    json.dumps({'content': content, 'ts': time.time()}), 
                    encoding='utf-8'
                )
            return content
            
        except Exception as e:
            response_time = time.time() - start_time
            self.rate_limiter.request_completed(False)
            self.monitor.record_request(False, 0, response_time)
            raise e

    @retry(
        wait=wait_random_exponential(min=0.1, max=20),
        stop=stop_after_attempt(7),
        retry=retry_if_exception_type((RateLimitError, APIError))
    )
    def structured_chat(self, messages: List[Dict[str, str]], response_format: BaseModel, 
                       max_tokens: int = 1000, cache: bool = True) -> BaseModel:
        """OpenAI API with structured output using Pydantic models."""
        # Rate limiting
        estimated_tokens = sum(len(m['content']) // 4 for m in messages) + max_tokens
        self.rate_limiter.wait_for_capacity(estimated_tokens)
        
        if cache:
            content_hash = hashlib.md5(json.dumps(messages, sort_keys=True).encode()).hexdigest()
            cache_file = self.cache_dir / f"structured_{content_hash}.json"
            if cache_file.exists():
                try:
                    cached_data = json.loads(cache_file.read_text(encoding='utf-8'))
                    self.rate_limiter.request_completed(True, 0)  # Cache hit
                    return response_format.model_validate(cached_data)
                except Exception:
                    pass
        
        start_time = time.time()
        try:
            response = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=messages,
                response_format=response_format,
                max_tokens=max_tokens
            )
            result = response.choices[0].message.parsed
            response_time = time.time() - start_time
            
            self.rate_limiter.request_completed(True, estimated_tokens)
            self.monitor.record_request(True, estimated_tokens, response_time)
            
            if cache:
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(result.model_dump_json(indent=2), encoding='utf-8')
            
            return result
            
        except Exception as e:
            response_time = time.time() - start_time
            self.rate_limiter.request_completed(False)
            self.monitor.record_request(False, 0, response_time)
            logger.error(f"Structured API call failed: {e}")
            raise e