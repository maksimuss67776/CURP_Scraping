"""
Main Orchestrator - OPTIMIZED VERSION
Combines OLD reliability + NEW stealth features
Single multithreading implementation
"""
import json
import sys
import logging
import os
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from excel_handler import ExcelHandler
from combination_generator import CombinationGenerator
from result_validator import ResultValidator
from checkpoint_manager import CheckpointManager
from multiprocess_worker import HighPerformanceWorker

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/curp_automation_main.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def load_config(config_path: str = "./config/settings.json") -> Dict:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise


def main():
    """Main execution function - OPTIMIZED MULTITHREADING."""
    
    try:
        # Load configuration
        config = load_config()
        
        year_start = config['year_range']['start']
        year_end = config['year_range']['end']
        min_delay = config['delays']['min_seconds']
        max_delay = config['delays']['max_seconds']
        pause_every_n = config['pause_every_n']
        pause_duration = config['pause_duration']
        headless = config['browser']['headless']
        output_dir = config['output_dir']
        checkpoint_dir = config.get('checkpoint_dir', './checkpoints')
        input_dir = config.get('input_dir', './data')
        num_threads = config.get('num_processes', 4)
        checkpoint_interval = config.get('checkpoint_interval', 5000)
        
        logger.info("=" * 80)
        logger.info("CURP SCRAPING - OPTIMIZED MULTITHREADING + STEALTH")
        logger.info("=" * 80)
        logger.info(f"Threads:             {num_threads} parallel workers")
        logger.info(f"Headless Mode:       {headless}")
        logger.info(f"Delay Range:         {min_delay}s - {max_delay}s")
        logger.info(f"Year Range:          {year_start} - {year_end}")
        logger.info("=" * 80)
        
        # Initialize components
        excel_handler = ExcelHandler(input_dir=input_dir, output_dir=output_dir)
        checkpoint_manager = CheckpointManager(checkpoint_dir=checkpoint_dir)
        
        # Check for existing checkpoint
        checkpoint = checkpoint_manager.load_checkpoint()
        resume_from_checkpoint = checkpoint is not None
        
        if resume_from_checkpoint:
            logger.info("Checkpoint found. Resuming from last position...")
            start_person_id = checkpoint['person_id']
            start_combination_index = checkpoint['combination_index']
            existing_matches = checkpoint['matches']
            logger.info(f"Resuming from person ID {start_person_id}, combination index {start_combination_index}")
        else:
            start_person_id = None
            start_combination_index = 0
            existing_matches = []
            logger.info("Starting fresh search...")
        
        # Get input file
        if len(sys.argv) > 1:
            input_filename = sys.argv[1]
        else:
            input_filename = "input_file.xlsx"
            template_path = Path(input_dir) / "input_template.xlsx"
            input_path = Path(input_dir) / input_filename
            if not input_path.exists() and not template_path.exists():
                logger.info("Creating input template...")
                excel_handler.create_template()
                logger.info(f"Template created at {template_path}. Please fill it with your data and run again.")
                return
        
        # Read input Excel
        logger.info(f"Reading input file: {input_filename}")
        input_df = excel_handler.read_input(input_filename)
        logger.info(f"Loaded {len(input_df)} person(s) from input file")
        
        # Calculate total combinations
        combination_generator_temp = CombinationGenerator(year_start, year_end)
        total_combinations_per_person = combination_generator_temp.get_total_count()
        total_persons = len(input_df)
        total_all_combinations = total_combinations_per_person * total_persons
        
        logger.info(f"Total combinations: {total_all_combinations:,} ({total_combinations_per_person:,} per person)")
        logger.info("")
        
        # Prepare results storage
        all_results: List[Dict] = existing_matches.copy()
        summary_data: List[Dict] = []
        
        # Initialize high-performance multithreading worker
        hp_worker = HighPerformanceWorker(
            num_threads=num_threads,
            headless=headless,
            min_delay=min_delay,
            max_delay=max_delay,
            pause_every_n=pause_every_n,
            pause_duration=pause_duration,
            output_dir=output_dir,
            checkpoint_interval=checkpoint_interval
        )
        
        logger.info(f"Initialized worker with {num_threads} threads")
        logger.info("Starting processing...")
        logger.info("")
        
        try:
            # Process each person
            for idx, row in input_df.iterrows():
                person_id = row['person_id']
                first_name = row['first_name']
                last_name_1 = row['last_name_1']
                last_name_2 = row['last_name_2']
                gender = row['gender']
                
                person_name = f"{first_name} {last_name_1} {last_name_2}"
                
                # Skip if resuming and haven't reached this person yet
                if resume_from_checkpoint and person_id < start_person_id:
                    person_matches = [m for m in existing_matches if m.get('person_id') == person_id]
                    summary_data.append({
                        'person_id': person_id,
                        'first_name': first_name,
                        'last_name_1': last_name_1,
                        'last_name_2': last_name_2,
                        'total_matches': len(person_matches)
                    })
                    continue
                
                # Generate combinations
                combination_generator = CombinationGenerator(year_start, year_end)
                combinations_list = list(combination_generator.generate_combinations())
                total_combinations = len(combinations_list)
                
                # Determine start index
                if resume_from_checkpoint and person_id == start_person_id:
                    logger.info(f"Resuming person {person_id}: {person_name}")
                    start_combo_index = start_combination_index
                    logger.info(f"Resuming from combination index {start_combo_index}")
                else:
                    logger.info(f"Processing person {person_id}/{total_persons}: {person_name}")
                    start_combo_index = 0
                
                logger.info(f"Total combinations for this person: {total_combinations:,}")
                logger.info("")
                
                # Get matches count before processing
                person_matches_before = len([r for r in all_results if r.get('person_id') == person_id])
                
                # Process using multiprocessing
                person_start_time = datetime.now()
                
                hp_worker.process_person_multiprocess(
                    person_data={
                        'person_id': person_id,
                        'first_name': first_name,
                        'last_name_1': last_name_1,
                        'last_name_2': last_name_2,
                        'gender': gender
                    },
                    combinations=combinations_list,
                    total_combinations=total_combinations,
                    checkpoint_manager=checkpoint_manager,
                    start_index=start_combo_index
                )
                
                person_end_time = datetime.now()
                person_elapsed = (person_end_time - person_start_time).total_seconds()
                
                # Update results
                person_matches_after = len([r for r in all_results if r.get('person_id') == person_id])
                person_matches_count = person_matches_after - person_matches_before
                
                # Add person summary
                summary_data.append({
                    'person_id': person_id,
                    'first_name': first_name,
                    'last_name_1': last_name_1,
                    'last_name_2': last_name_2,
                    'total_matches': person_matches_count
                })
                
                logger.info("")
                logger.info(f"Completed person {person_id} in {person_elapsed:.1f} seconds")
                logger.info(f"Found {person_matches_count} match(es)")
                logger.info("")
            
            # Generate final summary
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_filename = f"curp_summary_{timestamp}.xlsx"
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("PROCESSING COMPLETE!")
            logger.info("=" * 80)
            logger.info(f"Total persons processed: {total_persons}")
            logger.info(f"Total matches found: {sum(s['total_matches'] for s in summary_data)}")
            logger.info(f"Summary file: {summary_filename}")
            logger.info("=" * 80)
            
            # Write summary
            if summary_data:
                excel_handler.write_results([], summary_data, summary_filename)
            
            # Clear checkpoint
            checkpoint_manager.clear_checkpoint()
            
            logger.info("All processing completed successfully!")
            
        except KeyboardInterrupt:
            logger.warning("")
            logger.warning("=" * 80)
            logger.warning("Process interrupted by user!")
            logger.warning("Checkpoint has been saved. You can resume by running the script again.")
            logger.warning("=" * 80)
        
        except Exception as e:
            logger.error(f"Error during processing: {e}", exc_info=True)
            raise
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
