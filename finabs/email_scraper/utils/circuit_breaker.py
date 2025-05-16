"""
circuit_breaker.py - Circuit breaker pattern
------------------------------------------
Implementation of the circuit breaker pattern for handling failing domains.
"""
import logging
import time
from typing import Dict, Set

from email_scraper.config import CIRCUIT_BREAKER_FAILURE_THRESHOLD, CIRCUIT_BREAKER_RESET_TIMEOUT

class CircuitBreaker:
    """
    Circuit breaker pattern implementation for handling failing domains.
    
    Attributes:
        failure_counts: Dictionary mapping domains to failure counts
        circuit_open: Set of domains with open circuits
        last_failure_time: Dictionary mapping domains to last failure time
        failure_threshold: Number of failures before opening circuit
        reset_timeout: Time in seconds before resetting circuit
    """
    
    def __init__(self, failure_threshold=CIRCUIT_BREAKER_FAILURE_THRESHOLD, reset_timeout=CIRCUIT_BREAKER_RESET_TIMEOUT):
        """
        Initialize the circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            reset_timeout: Time in seconds before resetting circuit
        """
        self.failure_counts: Dict[str, int] = {}
        self.circuit_open: Set[str] = set()
        self.last_failure_time: Dict[str, float] = {}
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.logger = logging.getLogger("email_scraper")
        self.logger.info(f"CircuitBreaker initialized: threshold={failure_threshold}, timeout={reset_timeout}s")

    def record_failure(self, domain: str) -> None:
        """
        Record a failure for a domain.
        
        Args:
            domain: Domain to record failure for
        """
        if not domain:
            return
        
        self.failure_counts[domain] = self.failure_counts.get(domain, 0) + 1
        self.last_failure_time[domain] = time.time()
        self.logger.debug(f"CircuitBreaker: Recorded failure for {domain}. Count: {self.failure_counts[domain]}")
        
        if self.failure_counts[domain] >= self.failure_threshold and domain not in self.circuit_open:
            self.logger.warning(f"Circuit breaker OPENED for domain: {domain}")
            self.circuit_open.add(domain)

    def is_open(self, domain: str) -> bool:
        """
        Check if circuit is open for a domain.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if circuit is open, False otherwise
        """
        if not domain or domain not in self.circuit_open:
            return False
        
        if time.time() - self.last_failure_time.get(domain, 0) > self.reset_timeout:
            self.circuit_open.remove(domain)
            if domain in self.failure_counts:
                del self.failure_counts[domain]
            if domain in self.last_failure_time:
                del self.last_failure_time[domain]
            self.logger.info(f"Circuit breaker RESET for domain: {domain}")
            return False
        
        return True

    def record_success(self, domain: str) -> None:
        """
        Record a success for a domain.
        
        Args:
            domain: Domain to record success for
        """
        if not domain:
            return
        
        if domain in self.failure_counts:
            self.logger.debug(f"CircuitBreaker: Recorded success for {domain}. Resetting failure count.")
            del self.failure_counts[domain]
            if domain in self.last_failure_time:
                del self.last_failure_time[domain]
        
        if domain in self.circuit_open:
            self.circuit_open.remove(domain)
            self.logger.info(f"Circuit breaker CLOSED for domain: {domain} after success")
