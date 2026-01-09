"""
High-Performance Multithreading Worker
Uses threading for concurrent parallelism with shared memory access.
Designed for massive scale (11,000+ users).
"""
import threading
from threading import Thread, Lock, Event
from queue import Queue, Empty
import time
import logging
import os
import sys
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from pathlib import Path
import random

from browser_automation import BrowserAutomation
from result_validator import ResultValidator
from checkpoint_manager import CheckpointManager
from excel_handler import ExcelHandler

# Configure logging for threading
def setup_worker_logging(worker_id: int):
    """Setup logging for each worker thread."""
    logger = logging.getLogger(f'Worker-{worker_id}')
    logger.setLevel(logging.INFO)
    
    # File handler for this worker
    Path('logs').mkdir(exist_ok=True)
    fh = logging.FileHandler(f'logs/worker_{worker_id}.log')
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


class HighPerformanceWorker:
    """
    High-performance multithreading worker with advanced optimizations:
    - Concurrent parallelism via threading (shared memory access)
    - Browser connection pooling and reuse
    - Minimal latency with aggressive batching
    - Smart load balancing across workers
    - Memory-efficient result handling
    """
    
    def __init__(self, num_threads: int = 16, headless: bool = True,
                 min_delay: float = 0.3, max_delay: float = 0.6,
                 pause_every_n: int = 500, pause_duration: int = 5,
                 output_dir: str = "./data/results",
                 checkpoint_interval: int = 5000):
        """
        Initialize high-performance multithreading worker - SAFE CONFIGURATION.
        
        Args:
            num_threads: Number of parallel threads (recommend 8-16)
            headless: Run browsers in headless mode (TRUE for max performance)
            min_delay: Minimum delay between searches (SAFE: 0.3s)
            max_delay: Maximum delay between searches (SAFE: 0.6s)
            pause_every_n: Pause every N searches (SAFE: 500)
            pause_duration: Duration of pause (SAFE: 5s)
            output_dir: Directory for output files
            checkpoint_interval: Save checkpoint every N searches (SAFE: 5000)
        """
        self.num_threads = num_threads
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        self.output_dir = output_dir
        self.checkpoint_interval = checkpoint_interval
        
        # Threading components (shared memory)
        self.results_list = []  # Shared results storage
        self.processed_count = 0  # Counter
        self.match_count = 0  # Match counter
        self.count_lock = Lock()  # Lock for counters
        self.results_lock = Lock()  # Lock for results list
        
        # Performance tracking
        self.worker_stats = {}  # Per-worker statistics
        self.stats_lock = Lock()
        
        self.excel_handler = ExcelHandler(output_dir=output_dir)
        self.result_validator = ResultValidator()
        
        # Output file tracking
        self.output_files = {}
        
        # Batch writing configuration
        self.BATCH_SIZE = 300  # Safe batch size
        self.BATCH_TIMEOUT = 300  # Write every 5 minutes
        
    def worker_thread(self, worker_id: int, task_queue: Queue, result_queue: Queue,
                      person_data: Dict, stop_event: Event):
        """
        Worker thread that runs concurrently with shared memory access.
        """
        logger = setup_worker_logging(worker_id)
        logger.info(f"Worker thread {worker_id} starting (Thread ID: {threading.get_ident()})")
        
        browser = None
        local_search_count = 0
        local_match_count = 0
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10  # Safe error threshold
        
        # Stagger worker initialization to avoid thundering herd
        initial_delay = (worker_id - 1) * 0.5
        if initial_delay > 0:
            time.sleep(initial_delay)
        
        try:
            # Initialize browser for this process
            browser = BrowserAutomation(
                headless=self.headless,
                min_delay=self.min_delay,
                max_delay=self.max_delay,
                pause_every_n=self.pause_every_n,
                pause_duration=self.pause_duration
            )
            browser.start_browser()
            logger.info(f"Worker {worker_id}: Browser initialized successfully")
            
            start_time = time.time()
            
            # Process tasks from queue
            while not stop_event.is_set():
                try:
                    # Get task with timeout
                    try:
                        task = task_queue.get(timeout=2)
                    except Empty:
                        # Queue empty or timeout
                        if task_queue.empty():
                            break
                        continue
                    
                    if task is None:  # Poison pill
                        break
                    
                    combo_idx, day, month, state, year = task
                    
                    # Perform search
                    try:
                        html_content = browser.search_curp(
                            first_name=person_data['first_name'],
                            last_name_1=person_data['last_name_1'],
                            last_name_2=person_data['last_name_2'],
                            gender=person_data['gender'],
                            day=day,
                            month=month,
                            state=state,
                            year=year
                        )
                        
                        consecutive_errors = 0
                        
                        # Validate result
                        validation_result = self.result_validator.validate_result(html_content, state)
                        
                        if validation_result['found'] and validation_result['valid']:
                            # Match found!
                            local_match_count += 1
                            
                            match_data = {
                                'person_id': person_data['person_id'],
                                'first_name': person_data['first_name'],
                                'last_name_1': person_data['last_name_1'],
                                'last_name_2': person_data['last_name_2'],
                                'gender': person_data['gender'],
                                'curp': validation_result['curp'],
                                'birth_date': validation_result['birth_date'],
                                'birth_state': state,
                                'worker_id': worker_id,
                                'timestamp': datetime.now().isoformat()
                            }
                            
                            # Put result in queue for writer thread to handle
                            result_queue.put(('match', match_data))
                            
                            with self.count_lock:
                                self.match_count += 1
                            
                            logger.info(f"Worker {worker_id}: MATCH #{local_match_count} - "
                                      f"CURP {validation_result['curp']} "
                                      f"({day:02d}/{month:02d}/{year}, {state})")
                        
                        local_search_count += 1
                        
                        # Update global counter
                        with self.count_lock:
                            self.processed_count += 1
                            current_count = self.processed_count
                        
                        # Periodic progress logging
                        if local_search_count % 50 == 0:
                            elapsed = time.time() - start_time
                            rate = local_search_count / elapsed if elapsed > 0 else 0
                            logger.info(f"Worker {worker_id}: {local_search_count} searches, "
                                      f"{local_match_count} matches, {rate:.2f} searches/sec")
                        
                        task_queue.task_done()
                        
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"Worker {worker_id}: Search error (combo {combo_idx}): {e}")
                        
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                            logger.warning(f"Worker {worker_id}: Too many errors, restarting browser...")
                            try:
                                browser.close_browser()
                                time.sleep(5)
                                browser = BrowserAutomation(
                                    headless=self.headless,
                                    min_delay=self.min_delay,
                                    max_delay=self.max_delay,
                                    pause_every_n=self.pause_every_n,
                                    pause_duration=self.pause_duration
                                )
                                browser.start_browser()
                                consecutive_errors = 0
                                logger.info(f"Worker {worker_id}: Browser restarted")
                            except Exception as restart_error:
                                logger.error(f"Worker {worker_id}: Failed to restart browser: {restart_error}")
                                break
                        
                        task_queue.task_done()
                        continue
                
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Loop error: {e}")
                    break
            
            # Report final statistics
            elapsed_total = time.time() - start_time
            final_rate = local_search_count / elapsed_total if elapsed_total > 0 else 0
            
            with self.stats_lock:
                self.worker_stats[worker_id] = {
                    'searches': local_search_count,
                    'matches': local_match_count,
                    'elapsed': elapsed_total,
                    'rate': final_rate
                }
            
            logger.info(f"Worker {worker_id} completed: {local_search_count} searches, "
                       f"{local_match_count} matches, {final_rate:.2f} searches/sec")
        
        except Exception as e:
            logger.error(f"Worker {worker_id} fatal error: {e}")
        
        finally:
            if browser:
                try:
                    browser.close_browser()
                except:
                    pass
            logger.info(f"Worker {worker_id} shutdown complete")
    
    def result_writer_thread(self, result_queue: Queue, stop_event: Event, 
                             checkpoint_manager: CheckpointManager,
                             total_combinations: int):
        """
        Dedicated thread for writing results and checkpoints.
        This prevents write operations from blocking worker threads.
        """
        logger = setup_worker_logging(0)  # Writer thread
        logger.info("Result writer thread started")
        
        all_results = []
        match_buffer = []
        last_write_time = time.time()
        last_checkpoint_time = time.time()
        
        try:
            while not stop_event.is_set() or not result_queue.empty():
                try:
                    # Get result with timeout
                    try:
                        result = result_queue.get(timeout=1)
                    except Empty:
                        # Check if we should flush based on timeout
                        if time.time() - last_write_time >= self.BATCH_TIMEOUT and match_buffer:
                            self._write_batch(match_buffer, all_results, logger)
                            match_buffer.clear()
                            last_write_time = time.time()
                        continue
                    
                    if result is None:  # Poison pill
                        break
                    
                    msg_type, data = result
                    
                    if msg_type == 'match':
                        all_results.append(data)
                        match_buffer.append(data)
                        
                        # Flush if buffer is full
                        if len(match_buffer) >= self.BATCH_SIZE:
                            self._write_batch(match_buffer, all_results, logger)
                            match_buffer.clear()
                            last_write_time = time.time()
                    
                    # Periodic checkpoint saving
                    if time.time() - last_checkpoint_time >= 300:  # Every 5 minutes
                        with self.count_lock:
                            current_count = self.processed_count
                        
                        if all_results:
                            last_result = all_results[-1]
                            checkpoint_manager.save_checkpoint(
                                person_id=last_result['person_id'],
                                person_name=f"{last_result['first_name']} {last_result['last_name_1']}",
                                combination_index=current_count,
                                day=0, month=0, state='', year=0,  # Not tracking individual combo
                                matches=all_results.copy(),
                                total_processed=current_count,
                                total_combinations=total_combinations
                            )
                            logger.info(f"Checkpoint saved: {current_count}/{total_combinations} "
                                      f"({current_count/total_combinations*100:.1f}%)")
                        
                        last_checkpoint_time = time.time()
                    
                    result_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Writer process error: {e}")
            
            # Final flush
            if match_buffer:
                self._write_batch(match_buffer, all_results, logger)
            
            logger.info(f"Result writer completed: {len(all_results)} total matches written")
            
        except Exception as e:
            logger.error(f"Writer process fatal error: {e}")
    
    def _write_batch(self, match_buffer: List[Dict], all_results: List[Dict], logger):
        """Write a batch of matches to Excel."""
        try:
            # Group by person
            by_person = {}
            for match in match_buffer:
                pid = match['person_id']
                if pid not in by_person:
                    by_person[pid] = []
                by_person[pid].append(match)
            
            # Write each person's file
            for person_id, matches in by_person.items():
                if person_id not in self.output_files:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"curp_results_person_{person_id}_{timestamp}.xlsx"
                    self.output_files[person_id] = filename
                
                filename = self.output_files[person_id]
                
                # Get all matches for this person
                person_matches = [r for r in all_results if r.get('person_id') == person_id]
                
                # Create summary
                if matches:
                    first_match = matches[0]
                    summary = [{
                        'person_id': person_id,
                        'first_name': first_match['first_name'],
                        'last_name_1': first_match['last_name_1'],
                        'last_name_2': first_match['last_name_2'],
                        'total_matches': len(person_matches)
                    }]
                else:
                    summary = []
                
                self.excel_handler.write_results(person_matches, summary, filename)
                logger.info(f"Batch written: Person {person_id}, {len(person_matches)} matches -> {filename}")
        
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
    
    def process_person_multiprocess(self, person_data: Dict, combinations: List[Tuple],
                                   total_combinations: int, checkpoint_manager: CheckpointManager,
                                   start_index: int = 0):
        """
        Process person using multithreading for concurrent performance.
        Returns all matches found.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"Processing person {person_data['person_id']} with {self.num_threads} threads")
        
        # Create queues
        task_queue = Queue(maxsize=self.num_threads * 100)  # Bounded queue
        result_queue = Queue()
        
        # Create stop event
        stop_event = Event()
        
        # Reset counters
        with self.count_lock:
            self.processed_count = start_index
            self.match_count = 0
        
        # Start result writer thread
        writer_thread = Thread(
            target=self.result_writer_thread,
            args=(result_queue, stop_event, checkpoint_manager, total_combinations),
            daemon=False
        )
        writer_thread.start()
        
        # Start worker threads
        threads = []
        for worker_id in range(1, self.num_threads + 1):
            t = Thread(
                target=self.worker_thread,
                args=(worker_id, task_queue, result_queue, person_data, stop_event),
                daemon=False
            )
            t.start()
            threads.append(t)
        
        logger.info(f"Started {self.num_threads} worker threads + 1 writer thread")
        
        # Feed tasks to queue
        start_time = time.time()
        queued_count = 0
        
        try:
            for idx, combo in enumerate(combinations):
                if idx < start_index:
                    continue
                
                day, month, state, year = combo
                task_queue.put((idx, day, month, state, year))
                queued_count += 1
                
                # Periodic progress
                if queued_count % 1000 == 0 and queued_count > 0:
                    with self.count_lock:
                        completed = self.processed_count
                    progress_pct = (completed / total_combinations * 100) if total_combinations > 0 else 0
                    logger.info(f"Progress: {completed:,}/{total_combinations:,} ({progress_pct:.1f}%) - Queued {queued_count:,} tasks")
            
            logger.info(f"All {queued_count} tasks queued")
            
            # Send poison pills to stop workers
            for _ in range(self.num_threads):
                task_queue.put(None)
            
            # Wait for all threads to complete
            for t in threads:
                t.join()
            
            # Stop writer thread
            result_queue.put(None)
            writer_thread.join(timeout=60)
            
            elapsed_time = time.time() - start_time
            
            # Collect statistics
            with self.stats_lock:
                total_searches = sum(stats.get('searches', 0) for stats in self.worker_stats.values())
                total_matches = sum(stats.get('matches', 0) for stats in self.worker_stats.values())
            overall_rate = total_searches / elapsed_time if elapsed_time > 0 else 0
            
            logger.info("=" * 80)
            logger.info(f"Person {person_data['person_id']} completed in {elapsed_time:.1f} seconds")
            logger.info(f"Total searches: {total_searches}")
            logger.info(f"Total matches: {total_matches}")
            logger.info(f"Overall rate: {overall_rate:.2f} searches/second")
            logger.info(f"Per-worker average: {overall_rate/self.num_threads:.2f} searches/sec")
            logger.info("=" * 80)
            
        except KeyboardInterrupt:
            logger.warning("Interrupted by user, stopping workers...")
            stop_event.set()
            
            # Wait for all threads to finish
            for t in threads:
                t.join(timeout=5)
            
            writer_thread.join(timeout=5)
        
        except Exception as e:
            logger.error(f"Error in multithreading execution: {e}")
            stop_event.set()
            
            # Threads will exit gracefully
            for t in threads:
                t.join(timeout=5)
            writer_thread.join(timeout=5)
