"""
Performance Monitor & Load Balancer
Real-time monitoring and adaptive load balancing for optimal throughput.
"""
import time
import logging
from typing import Dict, List
from datetime import datetime, timedelta
from collections import deque
import statistics

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Monitor and track performance metrics in real-time."""
    
    def __init__(self, window_size: int = 60):
        """
        Initialize performance monitor.
        
        Args:
            window_size: Time window in seconds for calculating rates
        """
        self.window_size = window_size
        self.search_times = deque(maxlen=1000)  # Last 1000 search timestamps
        self.match_times = deque(maxlen=500)    # Last 500 match timestamps
        self.error_times = deque(maxlen=100)    # Last 100 error timestamps
        
        self.start_time = time.time()
        self.total_searches = 0
        self.total_matches = 0
        self.total_errors = 0
        
        # Per-worker statistics
        self.worker_stats: Dict[int, Dict] = {}
        
    def record_search(self, worker_id: int):
        """Record a completed search."""
        current_time = time.time()
        self.search_times.append(current_time)
        self.total_searches += 1
        
        if worker_id not in self.worker_stats:
            self.worker_stats[worker_id] = {
                'searches': 0,
                'matches': 0,
                'errors': 0,
                'last_active': current_time
            }
        
        self.worker_stats[worker_id]['searches'] += 1
        self.worker_stats[worker_id]['last_active'] = current_time
    
    def record_match(self, worker_id: int):
        """Record a match found."""
        current_time = time.time()
        self.match_times.append(current_time)
        self.total_matches += 1
        
        if worker_id in self.worker_stats:
            self.worker_stats[worker_id]['matches'] += 1
    
    def record_error(self, worker_id: int):
        """Record an error."""
        current_time = time.time()
        self.error_times.append(current_time)
        self.total_errors += 1
        
        if worker_id in self.worker_stats:
            self.worker_stats[worker_id]['errors'] += 1
    
    def get_search_rate(self) -> float:
        """Get current searches per second."""
        if not self.search_times:
            return 0.0
        
        current_time = time.time()
        cutoff_time = current_time - self.window_size
        
        # Count searches within window
        recent_searches = sum(1 for t in self.search_times if t >= cutoff_time)
        
        if recent_searches == 0:
            return 0.0
        
        # Calculate rate
        time_span = current_time - max(t for t in self.search_times if t >= cutoff_time)
        if time_span > 0:
            return recent_searches / time_span
        return 0.0
    
    def get_match_rate(self) -> float:
        """Get current matches per minute."""
        if not self.match_times:
            return 0.0
        
        current_time = time.time()
        cutoff_time = current_time - 60  # Last minute
        
        recent_matches = sum(1 for t in self.match_times if t >= cutoff_time)
        return recent_matches
    
    def get_error_rate(self) -> float:
        """Get current errors per minute."""
        if not self.error_times:
            return 0.0
        
        current_time = time.time()
        cutoff_time = current_time - 60
        
        recent_errors = sum(1 for t in self.error_times if t >= cutoff_time)
        return recent_errors
    
    def get_overall_stats(self) -> Dict:
        """Get overall statistics."""
        elapsed_time = time.time() - self.start_time
        
        return {
            'total_searches': self.total_searches,
            'total_matches': self.total_matches,
            'total_errors': self.total_errors,
            'elapsed_seconds': elapsed_time,
            'avg_search_rate': self.total_searches / elapsed_time if elapsed_time > 0 else 0,
            'current_search_rate': self.get_search_rate(),
            'match_rate_per_minute': self.get_match_rate(),
            'error_rate_per_minute': self.get_error_rate(),
            'success_rate': (self.total_searches - self.total_errors) / self.total_searches * 100 
                           if self.total_searches > 0 else 0
        }
    
    def get_worker_stats(self) -> Dict[int, Dict]:
        """Get per-worker statistics."""
        return self.worker_stats.copy()
    
    def identify_slow_workers(self, threshold_percentile: float = 25) -> List[int]:
        """
        Identify workers performing below threshold.
        
        Args:
            threshold_percentile: Percentile below which workers are considered slow
            
        Returns:
            List of worker IDs performing below threshold
        """
        if not self.worker_stats:
            return []
        
        # Calculate searches per worker
        worker_searches = {wid: stats['searches'] for wid, stats in self.worker_stats.items()}
        
        if not worker_searches:
            return []
        
        # Find threshold
        search_counts = list(worker_searches.values())
        if len(search_counts) < 2:
            return []
        
        threshold = statistics.quantiles(search_counts, n=100)[threshold_percentile - 1]
        
        # Identify slow workers
        slow_workers = [wid for wid, count in worker_searches.items() if count < threshold]
        
        return slow_workers
    
    def get_eta(self, total_combinations: int) -> str:
        """
        Calculate estimated time to completion.
        
        Args:
            total_combinations: Total number of combinations to process
            
        Returns:
            Human-readable ETA string
        """
        remaining = total_combinations - self.total_searches
        
        if remaining <= 0:
            return "Complete"
        
        current_rate = self.get_search_rate()
        
        if current_rate <= 0:
            return "Unknown"
        
        seconds_remaining = remaining / current_rate
        
        if seconds_remaining < 60:
            return f"{seconds_remaining:.0f} seconds"
        elif seconds_remaining < 3600:
            return f"{seconds_remaining / 60:.1f} minutes"
        elif seconds_remaining < 86400:
            return f"{seconds_remaining / 3600:.1f} hours"
        else:
            return f"{seconds_remaining / 86400:.1f} days"
    
    def print_status_report(self, total_combinations: int = None):
        """Print a comprehensive status report."""
        stats = self.get_overall_stats()
        
        logger.info("=" * 80)
        logger.info("PERFORMANCE STATUS REPORT")
        logger.info("=" * 80)
        logger.info(f"Total Searches:      {stats['total_searches']:,}")
        logger.info(f"Total Matches:       {stats['total_matches']:,}")
        logger.info(f"Total Errors:        {stats['total_errors']:,}")
        logger.info(f"Success Rate:        {stats['success_rate']:.2f}%")
        logger.info("-" * 80)
        logger.info(f"Elapsed Time:        {stats['elapsed_seconds'] / 3600:.2f} hours")
        logger.info(f"Avg Search Rate:     {stats['avg_search_rate']:.2f} searches/sec")
        logger.info(f"Current Rate:        {stats['current_search_rate']:.2f} searches/sec")
        logger.info(f"Match Rate:          {stats['match_rate_per_minute']:.1f} matches/min")
        logger.info(f"Error Rate:          {stats['error_rate_per_minute']:.1f} errors/min")
        
        if total_combinations:
            progress = (stats['total_searches'] / total_combinations) * 100
            eta = self.get_eta(total_combinations)
            logger.info("-" * 80)
            logger.info(f"Progress:            {progress:.2f}% ({stats['total_searches']:,}/{total_combinations:,})")
            logger.info(f"ETA:                 {eta}")
        
        # Worker breakdown
        logger.info("-" * 80)
        logger.info("Worker Performance:")
        for wid in sorted(self.worker_stats.keys()):
            wstats = self.worker_stats[wid]
            logger.info(f"  Worker {wid:2d}: {wstats['searches']:6,} searches, "
                       f"{wstats['matches']:4,} matches, {wstats['errors']:3,} errors")
        
        logger.info("=" * 80)


class AdaptiveLoadBalancer:
    """Dynamically adjust load based on performance metrics."""
    
    def __init__(self, initial_delay: float = 0.3):
        """
        Initialize adaptive load balancer.
        
        Args:
            initial_delay: Initial delay between requests
        """
        self.current_delay = initial_delay
        self.min_delay = 0.1
        self.max_delay = 2.0
        
        self.error_threshold = 5  # Errors per minute before throttling
        self.success_threshold = 2  # Minutes of success before speeding up
        
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # Adjust at most once per minute
        
    def should_throttle(self, error_rate: float) -> bool:
        """Determine if we should throttle based on error rate."""
        return error_rate > self.error_threshold
    
    def should_speed_up(self, error_rate: float) -> bool:
        """Determine if we can speed up."""
        current_time = time.time()
        time_since_adjustment = current_time - self.last_adjustment
        
        return (error_rate < 1.0 and 
                time_since_adjustment > self.adjustment_interval * self.success_threshold)
    
    def adjust_delay(self, monitor: PerformanceMonitor) -> float:
        """
        Adjust delay based on current performance.
        
        Args:
            monitor: PerformanceMonitor instance
            
        Returns:
            Recommended delay in seconds
        """
        error_rate = monitor.get_error_rate()
        current_time = time.time()
        
        # Don't adjust too frequently
        if current_time - self.last_adjustment < self.adjustment_interval:
            return self.current_delay
        
        old_delay = self.current_delay
        
        if self.should_throttle(error_rate):
            # Increase delay by 50%
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
            logger.warning(f"High error rate detected ({error_rate:.1f}/min). "
                          f"Throttling: {old_delay:.2f}s -> {self.current_delay:.2f}s")
            self.last_adjustment = current_time
            
        elif self.should_speed_up(error_rate):
            # Decrease delay by 20%
            self.current_delay = max(self.current_delay * 0.8, self.min_delay)
            logger.info(f"Performance good. Speeding up: {old_delay:.2f}s -> {self.current_delay:.2f}s")
            self.last_adjustment = current_time
        
        return self.current_delay
    
    def get_recommended_workers(self, current_workers: int, monitor: PerformanceMonitor) -> int:
        """
        Recommend number of workers based on performance.
        
        Args:
            current_workers: Current number of active workers
            monitor: PerformanceMonitor instance
            
        Returns:
            Recommended number of workers
        """
        error_rate = monitor.get_error_rate()
        search_rate = monitor.get_search_rate()
        
        # If high error rate, reduce workers
        if error_rate > self.error_threshold * 2:
            return max(current_workers - 2, 4)
        
        # If low utilization and low errors, can add workers
        if error_rate < 1.0 and search_rate > 0:
            # Very good performance, can try adding workers
            return min(current_workers + 2, 32)
        
        return current_workers
