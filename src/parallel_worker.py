"""
Parallel Worker
Manages multiple browser instances for parallel CURP searches.
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
    """Manages parallel browser instances for CURP searches."""
    
    def __init__(self, num_workers: int = 5, headless: bool = False,
                 min_delay: float = 1.0, max_delay: float = 2.0,
                 pause_every_n: int = 75, pause_duration: int = 15,
                 output_dir: str = "./data/results"):
        """
        Initialize parallel worker.
        
        Args:
            num_workers: Number of parallel browser instances
            headless: Run browsers in headless mode
            min_delay: Minimum delay between searches (seconds)
            max_delay: Maximum delay between searches (seconds)
            pause_every_n: Pause every N searches
            pause_duration: Duration of pause (seconds)
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
        
    def worker_thread(self, worker_id: int, combinations_queue: Queue,
                    first_name: str, last_name_1: str, last_name_2: str,
                    gender: str, person_id: int, person_name: str,
                    total_combinations: int, checkpoint_manager: CheckpointManager,
                    all_results: List[Dict], processed_count: Dict,
                    stop_event: threading.Event):
        """
        Worker thread that processes combinations from the queue.
        
        Args:
            worker_id: Unique ID for this worker
            combinations_queue: Queue of combinations to process
            first_name: First name
            last_name_1: First last name
            last_name_2: Second last name
            gender: Gender
            person_id: Person ID
            person_name: Person name for logging
            total_combinations: Total number of combinations
            checkpoint_manager: Checkpoint manager instance
            all_results: Shared list for results
            processed_count: Shared dict for processed count
            stop_event: Event to signal stop
        """
        browser_automation = None
        
        try:
            # Initialize browser for this worker
            browser_automation = BrowserAutomation(
                headless=self.headless,
                min_delay=self.min_delay,
                max_delay=self.max_delay,
                pause_every_n=self.pause_every_n,
                pause_duration=self.pause_duration
            )
            
            browser_automation.start_browser()
            logger.info(f"Worker {worker_id}: Browser started")
            
            worker_search_count = 0
            
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
                            
                            # Immediately save to Excel file
                            self._save_match_immediately(person_id, match_data, all_results)
                        
                        worker_search_count += 1
                        
                        # Update processed count
                        with self.processed_count_lock:
                            processed_count['count'] = processed_count.get('count', 0) + 1
                            current_count = processed_count['count']
                        
                        # Save checkpoint periodically (every 100 combinations across all workers)
                        if current_count % 100 == 0:
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
                        
                        # Log progress periodically
                        if current_count % 1000 == 0:
                            logger.info(f"Progress: {current_count}/{total_combinations} "
                                      f"({current_count/total_combinations*100:.2f}%)")
                        
                        # Mark task as done
                        combinations_queue.task_done()
                        
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Error processing combination "
                                   f"(day={day}, month={month}, state={state}, year={year}): {e}")
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
    
    def _save_match_immediately(self, person_id: int, match_data: Dict, all_results: List[Dict]):
        """
        Immediately save a match to Excel file (thread-safe).
        
        Args:
            person_id: Person ID
            match_data: Match data dictionary
            all_results: All results list
        """
        try:
            with self.excel_lock:
                # Get or create output filename for this person
                if person_id not in self.output_files:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"curp_results_person_{person_id}_{timestamp}.xlsx"
                    self.output_files[person_id] = filename
                
                filename = self.output_files[person_id]
                
                # Get all matches for this person
                person_matches = [r for r in all_results if r.get('person_id') == person_id]
                
                # Create summary for this person
                summary = [{
                    'person_id': person_id,
                    'first_name': match_data['first_name'],
                    'last_name_1': match_data['last_name_1'],
                    'last_name_2': match_data['last_name_2'],
                    'total_matches': len(person_matches)
                }]
                
                # Save to Excel (this will create or append)
                self.excel_handler.write_results(person_matches, summary, filename)
                
                logger.info(f"Match saved immediately to {filename} (Person {person_id}, Match #{len(person_matches)})")
        
        except Exception as e:
            logger.error(f"Error saving match immediately: {e}")
    
    def process_person_parallel(self, person_data: Dict, combinations: Iterator[Tuple[int, int, str, int]],
                              total_combinations: int, checkpoint_manager: CheckpointManager,
                              all_results: List[Dict], start_index: int = 0):
        """
        Process a person's combinations using parallel workers.
        
        Args:
            person_data: Dictionary with person information
            combinations: Iterator of (day, month, state, year) tuples
            total_combinations: Total number of combinations
            checkpoint_manager: Checkpoint manager
            all_results: List to store results
            start_index: Starting combination index (for resume)
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
        for combo in combinations:
            if combo_idx < start_index:
                combo_idx += 1
                continue
            
            day, month, state, year = combo
            combinations_queue.put((combo_idx, day, month, state, year))
            combo_idx += 1
        
        logger.info(f"Queued {combinations_queue.qsize()} combinations for parallel processing")
        
        # Shared state
        processed_count = {'count': start_index}
        stop_event = threading.Event()
        
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
            # Stagger thread starts slightly to avoid simultaneous requests
            time.sleep(0.5)
        
        logger.info(f"Started {self.num_workers} worker threads")
        
        # Wait for all combinations to be processed
        try:
            combinations_queue.join()
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Stopping workers...")
            stop_event.set()
            # Wait a bit for threads to finish current work
            time.sleep(2)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5)
        
        logger.info(f"Completed parallel processing for person {person_id}")

