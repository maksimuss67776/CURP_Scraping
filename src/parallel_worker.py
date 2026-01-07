"""
Parallel Worker
Manages multiple browser instances for parallel CURP searches.
OPTIMIZED VERSION - Batched Excel writes, smarter checkpointing
"""
import threading
import time
import logging
from typing import List, Dict, Iterator, Tuple
from queue import Queue
from pathlib import Path
from datetime import datetime

from browser_automation import BrowserAutomation
from result_validator import ResultValidator
from combination_generator import CombinationGenerator
from checkpoint_manager import CheckpointManager
from excel_handler import ExcelHandler

logger = logging.getLogger(__name__)


class ParallelWorker:
    """Manages parallel browser instances for CURP searches - OPTIMIZED."""
    
    def __init__(self, num_workers: int = 8, headless: bool = False,
                 min_delay: float = 0.5, max_delay: float = 1.0,
                 pause_every_n: int = 150, pause_duration: int = 10,
                 output_dir: str = "./data/results"):
        """
        Initialize parallel worker - OPTIMIZED DEFAULTS.
        
        Args:
            num_workers: Number of parallel browser instances (increased from 5)
            headless: Run browsers in headless mode
            min_delay: Minimum delay between searches (reduced from 1.0)
            max_delay: Maximum delay between searches (reduced from 2.0)
            pause_every_n: Pause every N searches (increased from 75)
            pause_duration: Duration of pause (reduced from 15)
            output_dir: Directory for output Excel files
        """
        self.num_workers = num_workers
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        self.output_dir = output_dir
        
        self.result_validator = ResultValidator()
        self.results_lock = threading.Lock()
        self.checkpoint_lock = threading.Lock()
        self.processed_count_lock = threading.Lock()
        self.excel_lock = threading.Lock()
        
        # Initialize Excel handler
        self.excel_handler = ExcelHandler(output_dir=output_dir)
        
        # Track output file per person
        self.output_files = {}  # person_id -> filename
        
        # OPTIMIZATION: Batch matches for Excel writing
        self.match_buffer = []  # Buffer for batch writes
        self.match_buffer_lock = threading.Lock()
        self.last_excel_write = time.time()
        self.EXCEL_BATCH_SIZE = 100  # Write every 100 matches (increased)
        self.EXCEL_BATCH_TIMEOUT = 180  # Or every 3 minutes (increased)
        
        # OPTIMIZATION: Smarter checkpoint interval
        self.CHECKPOINT_INTERVAL = 2000  # Further increased from 1000
        
    def worker_thread(self, worker_id: int, combinations_queue: Queue,
                    first_name: str, last_name_1: str, last_name_2: str,
                    gender: str, person_id: int, person_name: str,
                    total_combinations: int, checkpoint_manager: CheckpointManager,
                    all_results: List[Dict], processed_count: Dict,
                    stop_event: threading.Event):
        """
        Worker thread that processes combinations from the queue - OPTIMIZED.
        """
        browser_automation = None
        
        try:
            # Add worker-specific delay to further stagger connections
            initial_delay = (worker_id - 1) * 1.5  # 0s, 1.5s, 3s, 4.5s, etc.
            if initial_delay > 0:
                logger.info(f"Worker {worker_id}: Waiting {initial_delay:.1f}s before starting...")
                time.sleep(initial_delay)
            
            # Initialize browser for this worker with optimized settings
            browser_automation = BrowserAutomation(
                headless=self.headless,
                min_delay=self.min_delay,
                max_delay=self.max_delay,
                pause_every_n=self.pause_every_n,
                pause_duration=self.pause_duration
            )
            
            browser_automation.start_browser()
            logger.info(f"Worker {worker_id}: Browser started successfully")
            
            worker_search_count = 0
            consecutive_errors = 0
            MAX_CONSECUTIVE_ERRORS = 5
            
            while not stop_event.is_set():
                try:
                    # Get combination from queue (with timeout)
                    try:
                        combo_data = combinations_queue.get(timeout=1)
                    except:
                        # Queue empty, check if we should continue
                        if combinations_queue.empty():
                            break
                        continue
                    
                    combo_idx, day, month, state, year = combo_data
                    
                    # Perform search
                    try:
                        html_content = browser_automation.search_curp(
                            first_name=first_name,
                            last_name_1=last_name_1,
                            last_name_2=last_name_2,
                            gender=gender,
                            day=day,
                            month=month,
                            state=state,
                            year=year
                        )
                        
                        consecutive_errors = 0  # Reset on success
                        
                        # Validate result
                        validation_result = self.result_validator.validate_result(html_content, state)
                        
                        if validation_result['found'] and validation_result['valid']:
                            # Match found!
                            with self.results_lock:
                                person_match_count = len([r for r in all_results if r.get('person_id') == person_id])
                            
                            match_data = {
                                'person_id': person_id,
                                'first_name': first_name,
                                'last_name_1': last_name_1,
                                'last_name_2': last_name_2,
                                'gender': gender,
                                'curp': validation_result['curp'],
                                'birth_date': validation_result['birth_date'],
                                'birth_state': state,
                                'match_number': person_match_count + 1
                            }
                            
                            with self.results_lock:
                                all_results.append(match_data)
                            
                            logger.info(f"Worker {worker_id}: MATCH FOUND! Person {person_id}: "
                                      f"CURP {validation_result['curp']} ({day:02d}/{month:02d}/{year}, {state})")
                            
                            # OPTIMIZATION: Add to batch buffer instead of immediate write
                            self._add_to_match_buffer(person_id, match_data, all_results)
                        
                        worker_search_count += 1
                        
                        # Update processed count
                        with self.processed_count_lock:
                            processed_count['count'] = processed_count.get('count', 0) + 1
                            current_count = processed_count['count']
                        
                        # OPTIMIZATION: Save checkpoint less frequently
                        if current_count % self.CHECKPOINT_INTERVAL == 0:
                            with self.checkpoint_lock:
                                checkpoint_manager.save_checkpoint(
                                    person_id=person_id,
                                    person_name=person_name,
                                    combination_index=combo_idx,
                                    day=day,
                                    month=month,
                                    state=state,
                                    year=year,
                                    matches=all_results.copy(),
                                    total_processed=current_count,
                                    total_combinations=total_combinations
                                )
                            logger.info(f"Checkpoint saved. Progress: {current_count}/{total_combinations} "
                                      f"({current_count/total_combinations*100:.2f}%)")
                        
                        # Log progress periodically (every 1000 for performance)
                        if current_count % 1000 == 0:
                            elapsed_rate = worker_search_count / max(1, time.time() - getattr(self, '_start_time', time.time()))
                            logger.info(f"Progress: {current_count}/{total_combinations} "
                                      f"({current_count/total_combinations*100:.2f}%) - "
                                      f"Worker {worker_id}: {elapsed_rate:.1f} searches/sec")
                        
                        # Mark task as done
                        combinations_queue.task_done()
                        
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"Worker {worker_id}: Error processing combination "
                                   f"(day={day}, month={month}, state={state}, year={year}): {e}")
                        
                        # If too many consecutive errors, take a break
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                            logger.warning(f"Worker {worker_id}: {MAX_CONSECUTIVE_ERRORS} consecutive errors, "
                                         f"pausing for 30 seconds...")
                            time.sleep(30)
                            consecutive_errors = 0
                        
                        combinations_queue.task_done()
                        continue
                
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error in worker loop: {e}")
                    break
            
            logger.info(f"Worker {worker_id}: Completed {worker_search_count} searches")
        
        except Exception as e:
            logger.error(f"Worker {worker_id}: Fatal error: {e}")
        
        finally:
            if browser_automation:
                browser_automation.close_browser()
                logger.info(f"Worker {worker_id}: Browser closed")
    
    def _add_to_match_buffer(self, person_id: int, match_data: Dict, all_results: List[Dict]):
        """
        Add match to buffer and flush if needed - OPTIMIZED batching.
        """
        with self.match_buffer_lock:
            self.match_buffer.append({
                'person_id': person_id,
                'match_data': match_data,
                'all_results_snapshot': None  # Don't snapshot every time
            })
            
            current_time = time.time()
            should_flush = (
                len(self.match_buffer) >= self.EXCEL_BATCH_SIZE or
                (current_time - self.last_excel_write) >= self.EXCEL_BATCH_TIMEOUT
            )
            
            if should_flush and self.match_buffer:
                self._flush_match_buffer(all_results)
    
    def _flush_match_buffer(self, all_results: List[Dict]):
        """
        Flush match buffer to Excel - called with lock held.
        """
        if not self.match_buffer:
            return
            
        try:
            with self.excel_lock:
                # Group matches by person_id
                by_person = {}
                for item in self.match_buffer:
                    pid = item['person_id']
                    if pid not in by_person:
                        by_person[pid] = []
                    by_person[pid].append(item['match_data'])
                
                # Write each person's matches
                for person_id, matches in by_person.items():
                    # Get or create output filename for this person
                    if person_id not in self.output_files:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"curp_results_person_{person_id}_{timestamp}.xlsx"
                        self.output_files[person_id] = filename
                    
                    filename = self.output_files[person_id]
                    
                    # Get all matches for this person from main results
                    with self.results_lock:
                        person_matches = [r for r in all_results if r.get('person_id') == person_id]
                    
                    # Create summary for this person
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
                    
                    # Save to Excel
                    self.excel_handler.write_results(person_matches, summary, filename)
                    logger.info(f"Batch saved to {filename} (Person {person_id}, {len(person_matches)} total matches)")
                
                self.match_buffer.clear()
                self.last_excel_write = time.time()
        
        except Exception as e:
            logger.error(f"Error flushing match buffer: {e}")
    
    def process_person_parallel(self, person_data: Dict, combinations: Iterator[Tuple[int, int, str, int]],
                              total_combinations: int, checkpoint_manager: CheckpointManager,
                              all_results: List[Dict], start_index: int = 0):
        """
        Process a person's combinations using parallel workers - OPTIMIZED.
        """
        first_name = person_data['first_name']
        last_name_1 = person_data['last_name_1']
        last_name_2 = person_data['last_name_2']
        gender = person_data['gender']
        person_id = person_data['person_id']
        person_name = f"{first_name} {last_name_1} {last_name_2}"
        
        # Create queue for combinations
        combinations_queue = Queue()
        
        # Skip to start_index if resuming
        combo_idx = 0
        queued_count = 0
        for combo in combinations:
            if combo_idx < start_index:
                combo_idx += 1
                continue
            
            day, month, state, year = combo
            combinations_queue.put((combo_idx, day, month, state, year))
            combo_idx += 1
            queued_count += 1
        
        logger.info(f"Queued {queued_count} combinations for parallel processing")
        
        # Shared state
        processed_count = {'count': start_index}
        stop_event = threading.Event()
        self._start_time = time.time()
        
        # Create and start worker threads
        threads = []
        for worker_id in range(1, self.num_workers + 1):
            thread = threading.Thread(
                target=self.worker_thread,
                args=(worker_id, combinations_queue, first_name, last_name_1,
                     last_name_2, gender, person_id, person_name,
                     total_combinations, checkpoint_manager, all_results,
                     processed_count, stop_event),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            # CRITICAL: Stagger browser startups to avoid overwhelming server
            # Each browser needs time to fully initialize before next starts
            time.sleep(2.0)
        
        logger.info(f"Started {self.num_workers} worker threads")
        
        # Wait for all combinations to be processed
        try:
            combinations_queue.join()
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Stopping workers...")
            stop_event.set()
            # Wait a bit for threads to finish current work
            time.sleep(2)
        
        # Flush any remaining matches in buffer
        with self.match_buffer_lock:
            self._flush_match_buffer(all_results)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5)
        
        elapsed_time = time.time() - self._start_time
        logger.info(f"Completed parallel processing for person {person_id} in {elapsed_time:.1f} seconds")
        logger.info(f"Average rate: {queued_count / max(1, elapsed_time):.2f} searches/second")
