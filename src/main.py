"""
Main Orchestrator
Main script that coordinates all components to perform CURP searches.
OPTIMIZED VERSION - Better logging, progress tracking
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
from browser_automation import BrowserAutomation
from result_validator import ResultValidator
from checkpoint_manager import CheckpointManager
from parallel_worker import ParallelWorker


# Create logs directory if it doesn't exist
Path('logs').mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/curp_automation.log'),
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


def estimate_time(total_combinations: int, num_workers: int, avg_seconds_per_search: float = 2.0) -> str:
    """Estimate total time for completion."""
    total_seconds = (total_combinations / num_workers) * avg_seconds_per_search
    hours = total_seconds / 3600
    if hours > 24:
        days = hours / 24
        return f"{days:.1f} days"
    elif hours > 1:
        return f"{hours:.1f} hours"
    else:
        return f"{total_seconds / 60:.1f} minutes"


def main():
    """Main execution function."""
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
        num_workers = config.get('num_workers', 8)  # Default increased to 8
        
        # Log optimization settings
        logger.info("=" * 60)
        logger.info("CURP SCRAPING - OPTIMIZED VERSION")
        logger.info("=" * 60)
        logger.info(f"Workers: {num_workers}")
        logger.info(f"Delay: {min_delay}-{max_delay}s")
        logger.info(f"Pause: every {pause_every_n} searches for {pause_duration}s")
        logger.info(f"Year range: {year_start}-{year_end}")
        logger.info("=" * 60)
        
        # Initialize components
        excel_handler = ExcelHandler(input_dir=input_dir, output_dir=output_dir)
        checkpoint_manager = CheckpointManager(checkpoint_dir=checkpoint_dir)
        result_validator = ResultValidator()
        
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
        
        # Get input file from command line or use default
        if len(sys.argv) > 1:
            input_filename = sys.argv[1]
        else:
            input_filename = "input_file.xlsx"
            # Create template if it doesn't exist
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
        
        # Calculate total combinations for estimation
        combination_generator_temp = CombinationGenerator(year_start, year_end)
        total_combinations_per_person = combination_generator_temp.get_total_count()
        total_persons = len(input_df)
        total_all_combinations = total_combinations_per_person * total_persons
        
        estimated_time = estimate_time(total_all_combinations, num_workers)
        logger.info(f"Total combinations: {total_all_combinations:,} ({total_combinations_per_person:,} per person)")
        logger.info(f"Estimated completion time: {estimated_time}")
        
        # Prepare results storage
        all_results: List[Dict] = existing_matches.copy()
        summary_data: List[Dict] = []
        
        # Initialize parallel worker for multi-instance processing
        parallel_worker = ParallelWorker(
            num_workers=num_workers,
            headless=headless,
            min_delay=min_delay,
            max_delay=max_delay,
            pause_every_n=pause_every_n,
            pause_duration=pause_duration,
            output_dir=output_dir
        )
        logger.info(f"Initialized parallel worker with {num_workers} browser instances")
        
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
                    # Include matches from this person in summary
                    person_matches = [m for m in existing_matches if m.get('person_id') == person_id]
                    summary_data.append({
                        'person_id': person_id,
                        'first_name': first_name,
                        'last_name_1': last_name_1,
                        'last_name_2': last_name_2,
                        'total_matches': len(person_matches)
                    })
                    continue
                
                # Skip if resuming and this is the person we're resuming from
                combination_generator = CombinationGenerator(year_start, year_end)
                total_combinations = combination_generator.get_total_count()
                
                if resume_from_checkpoint and person_id == start_person_id:
                    logger.info(f"Resuming person {person_id}: {person_name}")
                    start_combination_index = start_combination_index
                    logger.info(f"Resuming from combination index {start_combination_index}")
                else:
                    logger.info(f"Processing person {person_id}/{total_persons}: {person_name}")
                    start_combination_index = 0
                
                logger.info(f"Total combinations for this person: {total_combinations:,}")
                
                # Get person matches count before processing
                person_matches_before = len([r for r in all_results if r.get('person_id') == person_id])
                
                # Determine start index for this person
                if resume_from_checkpoint and person_id == start_person_id:
                    start_combo_index = start_combination_index
                else:
                    start_combo_index = 0
                
                # Process using parallel workers
                parallel_worker.process_person_parallel(
                    person_data={
                        'person_id': person_id,
                        'first_name': first_name,
                        'last_name_1': last_name_1,
                        'last_name_2': last_name_2,
                        'gender': gender
                    },
                    combinations=combination_generator.generate_combinations(),
                    total_combinations=total_combinations,
                    checkpoint_manager=checkpoint_manager,
                    all_results=all_results,
                    start_index=start_combo_index
                )
                
                # Count matches found for this person
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
                
                logger.info(f"Completed person {person_id}: {person_matches_count} match(es) found")
            
            # Generate output Excel
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"curp_results_{timestamp}.xlsx"
            
            logger.info(f"Writing final results to Excel: {output_filename}")
            excel_handler.write_results(all_results, summary_data, output_filename)
            
            # Clear checkpoint on successful completion
            checkpoint_manager.clear_checkpoint()
            logger.info("=" * 60)
            logger.info("Search completed successfully!")
            logger.info(f"Total matches found: {len(all_results)}")
            logger.info("=" * 60)
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user. Checkpoint will be saved by workers.")
            # Workers handle their own checkpoint saving, so we just log
            logger.info("You can resume later by running the script again.")
        
        finally:
            # Parallel workers handle their own browser cleanup
            logger.info("All workers completed")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
