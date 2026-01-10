"""
High-Performance Multithreading Worker - OPTIMIZED VERSION
Uses efficient queue-based work distribution with all workers pulling from a single queue.
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


def setup_worker_logging(worker_id: int):
    """Setup logging for each worker thread."""
    logger = logging.getLogger(f'Worker-{worker_id}')
    logger.setLevel(logging.INFO)
    
    Path('logs').mkdir(exist_ok=True)
    fh = logging.FileHandler(f'logs/worker_{worker_id}.log')
    fh.setLevel(logging.INFO)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)
    
    return logger


class HighPerformanceWorker:
    """
    OPTIMIZED High-performance multithreading worker:
    - Single shared queue for perfect load balancing
    - Workers pull tasks as fast as they can process
    - No pre-allocation = faster workers get more work
    - Progress-based checkpointing (even with 0 matches)
    - Batch writing with timeout-based flush
    """
    
    def __init__(self, num_threads: int = 4, headless: bool = True,
                 min_delay: float = 0.3, max_delay: float = 0.6,
                 pause_every_n: int = 100, pause_duration: int = 10,
                 output_dir: str = "./data/results",
                 checkpoint_interval: int = 1000):
        self.num_threads = num_threads
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        self.output_dir = output_dir
        self.checkpoint_interval = checkpoint_interval
        
        # Thread-safe counters
        self.processed_count = 0
        self.match_count = 0
        self.count_lock = Lock()
        
        # Per-worker statistics
        self.worker_stats = {}
        self.stats_lock = Lock()
        
        self.excel_handler = ExcelHandler(output_dir=output_dir)
        self.result_validator = ResultValidator()
        
        # Output file tracking
        self.output_files = {}
        
        # Batch writing configuration
        self.BATCH_SIZE = 100  # Write after 100 matches
        self.BATCH_TIMEOUT = 120  # Write every 2 minutes regardless
        self.CHECKPOINT_INTERVAL = 60  # Checkpoint every 60 seconds
    
    def worker_thread(self, worker_id: int, task_queue: Queue, result_queue: Queue,
                      person_data: Dict, stop_event: Event, all_tasks_queued: Event,
                      match_found_event: Event):
        """
        OPTIMIZED worker thread - pulls tasks as fast as it can process them.
        """
        logger = setup_worker_logging(worker_id)
        logger.info(f"Worker {worker_id} starting")
        
        browser = None
        local_search_count = 0
        local_match_count = 0
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 5
        
        # Stagger initialization slightly
        time.sleep(worker_id * 0.3)
        
        try:
            browser = BrowserAutomation(
                headless=self.headless,
                min_delay=self.min_delay,
                max_delay=self.max_delay,
                pause_every_n=self.pause_every_n,
                pause_duration=self.pause_duration
            )
            browser.start_browser()
            logger.info(f"Worker {worker_id}: Browser ready, processing tasks...")
            
            start_time = time.time()
            last_log_time = start_time
            
            while not stop_event.is_set() and not match_found_event.is_set():
                try:
                    # Try to get a task
                    try:
                        task = task_queue.get(timeout=1)
                    except Empty:
                        # Queue empty - check if all tasks have been queued
                        if all_tasks_queued.is_set() and task_queue.empty():
                            logger.info(f"Worker {worker_id}: All tasks completed")
                            break
                        continue
                    
                    if task is None:  # Poison pill
                        task_queue.task_done()
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
                        local_search_count += 1
                        
                        # Update global counter
                        with self.count_lock:
                            self.processed_count += 1
                            current_total = self.processed_count
                        
                        # Validate result
                        result = self.result_validator.validate_result(html_content, state)
                        
                        if result['found'] and result['valid']:
                            local_match_count += 1
                            
                            match_data = {
                                'person_id': person_data['person_id'],
                                'first_name': person_data['first_name'],
                                'last_name_1': person_data['last_name_1'],
                                'last_name_2': person_data['last_name_2'],
                                'gender': person_data['gender'],
                                'curp': result['curp'],
                                'birth_date': result['birth_date'],
                                'birth_state': state,
                                'worker_id': worker_id,
                                'timestamp': datetime.now().isoformat()
                            }
                            
                            result_queue.put(('match', match_data))
                            
                            with self.count_lock:
                                self.match_count += 1
                            
                            # EARLY STOP: Signal all workers to stop
                            match_found_event.set()
                            logger.info(f"Worker {worker_id}: CURP FOUND! Signaling all workers to stop.")
                            
                            logger.info(f"Worker {worker_id}: MATCH! CURP={result['curp']} "
                                       f"({day:02d}/{month:02d}/{year}, {state})")
                        
                        # Progress report every 10 seconds
                        if time.time() - last_log_time > 10:
                            elapsed = time.time() - start_time
                            rate = local_search_count / elapsed if elapsed > 0 else 0
                            logger.info(f"Worker {worker_id}: {local_search_count} done, "
                                       f"{local_match_count} matches, {rate:.2f}/sec")
                            last_log_time = time.time()
                        
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"Worker {worker_id}: Search error: {e}")
                        
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                            logger.warning(f"Worker {worker_id}: Too many errors, restarting browser...")
                            try:
                                browser.close_browser()
                                time.sleep(2)
                                browser = BrowserAutomation(
                                    headless=self.headless,
                                    min_delay=self.min_delay,
                                    max_delay=self.max_delay
                                )
                                browser.start_browser()
                                consecutive_errors = 0
                            except:
                                break
                    
                    finally:
                        task_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Loop error: {e}")
            
            # Final stats
            elapsed = time.time() - start_time
            rate = local_search_count / elapsed if elapsed > 0 else 0
            
            with self.stats_lock:
                self.worker_stats[worker_id] = {
                    'searches': local_search_count,
                    'matches': local_match_count,
                    'elapsed': elapsed,
                    'rate': rate
                }
            
            logger.info(f"Worker {worker_id} DONE: {local_search_count} searches, "
                       f"{local_match_count} matches, {rate:.2f}/sec")
        
        except Exception as e:
            logger.error(f"Worker {worker_id} fatal error: {e}")
        
        finally:
            if browser:
                try:
                    browser.close_browser()
                except:
                    pass
    
    def result_writer_thread(self, result_queue: Queue, stop_event: Event,
                             checkpoint_manager: CheckpointManager,
                             person_data: Dict, total_combinations: int):
        """
        OPTIMIZED result writer with progress-based checkpointing.
        """
        logger = setup_worker_logging(0)
        logger.info("Writer thread started")
        
        all_results = []
        match_buffer = []
        last_write_time = time.time()
        last_checkpoint_time = time.time()
        
        try:
            while not stop_event.is_set() or not result_queue.empty():
                try:
                    try:
                        result = result_queue.get(timeout=1)
                    except Empty:
                        # Time-based flush
                        current_time = time.time()
                        
                        if current_time - last_write_time >= self.BATCH_TIMEOUT and match_buffer:
                            self._write_batch(match_buffer, all_results, logger)
                            match_buffer.clear()
                            last_write_time = current_time
                        
                        # Progress-based checkpoint (even with 0 matches)
                        if current_time - last_checkpoint_time >= self.CHECKPOINT_INTERVAL:
                            with self.count_lock:
                                current_count = self.processed_count
                            
                            checkpoint_manager.save_checkpoint(
                                person_id=person_data['person_id'],
                                person_name=f"{person_data['first_name']} {person_data['last_name_1']}",
                                combination_index=current_count,
                                day=0, month=0, state='', year=0,
                                matches=all_results.copy(),
                                total_processed=current_count,
                                total_combinations=total_combinations
                            )
                            
                            if current_count > 0:
                                pct = current_count / total_combinations * 100
                                logger.info(f"Checkpoint: {current_count}/{total_combinations} ({pct:.1f}%)")
                            
                            last_checkpoint_time = current_time
                        
                        continue
                    
                    if result is None:
                        break
                    
                    msg_type, data = result
                    
                    if msg_type == 'match':
                        all_results.append(data)
                        match_buffer.append(data)
                        
                        # Write when buffer is full
                        if len(match_buffer) >= self.BATCH_SIZE:
                            self._write_batch(match_buffer, all_results, logger)
                            match_buffer.clear()
                            last_write_time = time.time()
                    
                    result_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Writer error: {e}")
            
            # Final flush
            if match_buffer:
                self._write_batch(match_buffer, all_results, logger)
            
            # Final checkpoint
            with self.count_lock:
                final_count = self.processed_count
            
            checkpoint_manager.save_checkpoint(
                person_id=person_data['person_id'],
                person_name=f"{person_data['first_name']} {person_data['last_name_1']}",
                combination_index=final_count,
                day=0, month=0, state='', year=0,
                matches=all_results.copy(),
                total_processed=final_count,
                total_combinations=total_combinations
            )
            
            logger.info(f"Writer completed: {len(all_results)} matches saved")
        
        except Exception as e:
            logger.error(f"Writer fatal error: {e}")
    
    def _write_batch(self, match_buffer: List[Dict], all_results: List[Dict], logger):
        """Write matches to Excel file."""
        if not match_buffer:
            return
        
        try:
            by_person = {}
            for match in match_buffer:
                pid = match['person_id']
                if pid not in by_person:
                    by_person[pid] = []
                by_person[pid].append(match)
            
            for person_id, matches in by_person.items():
                if person_id not in self.output_files:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"curp_results_person_{person_id}_{timestamp}.xlsx"
                    self.output_files[person_id] = filename
                
                filename = self.output_files[person_id]
                person_matches = [r for r in all_results if r.get('person_id') == person_id]
                
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
                logger.info(f"Saved {len(person_matches)} matches -> {filename}")
        
        except Exception as e:
            logger.error(f"Write error: {e}")
    
    def process_person_multiprocess(self, person_data: Dict, combinations: List[Tuple],
                                   total_combinations: int, checkpoint_manager: CheckpointManager,
                                   start_index: int = 0):
        """
        OPTIMIZED processing with perfect work distribution.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"Processing person {person_data['person_id']} with {self.num_threads} workers")
        logger.info(f"Total combinations: {total_combinations}, starting from index {start_index}")
        
        # Unbounded queue for maximum throughput
        task_queue = Queue()
        result_queue = Queue()
        
        stop_event = Event()
        all_tasks_queued = Event()
        match_found_event = Event()  # NEW: Early stop when CURP found
        
        # Reset counters
        with self.count_lock:
            self.processed_count = start_index
            self.match_count = 0
        
        # Start writer thread
        writer_thread = Thread(
            target=self.result_writer_thread,
            args=(result_queue, stop_event, checkpoint_manager, person_data, total_combinations),
            daemon=False
        )
        writer_thread.start()
        
        # Start worker threads
        workers = []
        for worker_id in range(1, self.num_threads + 1):
            t = Thread(
                target=self.worker_thread,
                args=(worker_id, task_queue, result_queue, person_data, stop_event, 
                      all_tasks_queued, match_found_event),  # Pass match_found_event
                daemon=False
            )
            t.start()
            workers.append(t)
        
        logger.info(f"Started {self.num_threads} workers + 1 writer")
        
        start_time = time.time()
        
        try:
            # Queue all tasks immediately (fast workers will grab more)
            queued = 0
            for idx, combo in enumerate(combinations):
                if idx < start_index:
                    continue
                day, month, state, year = combo
                task_queue.put((idx, day, month, state, year))
                queued += 1
            
            logger.info(f"Queued {queued} tasks")
            all_tasks_queued.set()
            
            # Wait for all tasks to complete
            task_queue.join()
            
            # Stop workers
            for _ in range(self.num_threads):
                task_queue.put(None)
            
            for t in workers:
                t.join(timeout=30)
            
            # Stop writer
            stop_event.set()
            result_queue.put(None)
            writer_thread.join(timeout=30)
            
            elapsed = time.time() - start_time
            
            # Final statistics
            with self.stats_lock:
                total_searches = sum(s.get('searches', 0) for s in self.worker_stats.values())
                total_matches = sum(s.get('matches', 0) for s in self.worker_stats.values())
            
            overall_rate = total_searches / elapsed if elapsed > 0 else 0
            
            logger.info("=" * 60)
            logger.info(f"COMPLETED in {elapsed:.1f}s")
            logger.info(f"Total searches: {total_searches}")
            logger.info(f"Total matches: {total_matches}")
            logger.info(f"Rate: {overall_rate:.2f} searches/sec")
            
            # Show per-worker distribution
            logger.info("Worker distribution:")
            for wid, stats in sorted(self.worker_stats.items()):
                pct = stats['searches'] / total_searches * 100 if total_searches > 0 else 0
                logger.info(f"  Worker {wid}: {stats['searches']} searches ({pct:.1f}%), "
                           f"{stats['matches']} matches, {stats['rate']:.2f}/sec")
            logger.info("=" * 60)
        
        except KeyboardInterrupt:
            logger.warning("Interrupted! Saving progress...")
            stop_event.set()
            for t in workers:
                t.join(timeout=5)
            writer_thread.join(timeout=5)
        
        except Exception as e:
            logger.error(f"Error: {e}")
            stop_event.set()
            for t in workers:
                t.join(timeout=5)
            writer_thread.join(timeout=5)
