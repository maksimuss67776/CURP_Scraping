# CURP Automation Tool - Multithreading Edition

Automated tool for searching Mexican CURP (Clave Única de Registro de Población) on the official government portal (gob.mx/curp/) using high-performance multithreaded browser automation.

## Overview

This tool reads personal data from an Excel file, generates combinations of birth dates, months, states, and years, and performs concurrent searches on the official CURP portal to find valid CURP matches. Results are saved to Excel files with found CURPs, birth dates, states, and summary statistics.

**Key Features:**
- **Multithreading**: Run 2-10 parallel browser instances for faster searching
- **Selenium WebDriver**: Reliable browser automation with stealth features
- **Smart Rate Limiting**: Configurable delays to avoid detection while maximizing speed
- **Checkpoint System**: Automatic progress saving and resume capability
- **Batch Processing**: Efficient result writing in batches

## Features

- **Excel Input/Output**: Read input data from Excel and export results to Excel format
- **Combination Generation**: Automatically generates all combinations of dates (1-31), months (1-12), 33 states/options, and configurable year ranges
- **Multithreaded Browser Automation**: Run multiple Selenium browsers in parallel with shared queue
- **Checkpoint System**: Saves progress every 5000 searches and allows resuming interrupted sessions
- **Result Validation**: Validates and extracts CURP information from search results
- **Smart Rate Limiting**: Configurable delays (0.3-0.6 seconds) with adaptive throttling
- **Error Handling**: Robust error handling for network issues, timeouts, and browser crashes
- **Performance Monitoring**: Real-time statistics on search rate and matches found
- **Stealth Features**: Anti-detection measures to avoid blocking

## Requirements

- Python 3.9 or higher
- Windows, macOS, or Linux
- Google Chrome browser installed
- At least 4GB RAM (8GB recommended for multiple threads)
- Stable internet connection

## Installation

1. **Clone or download this repository**

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   
   # On Windows:
   venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

**Required packages** (automatically installed with requirements.txt):
- `selenium>=4.15.0` - Browser automation
- `webdriver-manager>=4.0.0` - Automatic ChromeDriver management
- `openpyxl>=3.1.0` - Excel file handling
- `pandas>=2.0.0` - Data manipulation
- `undetected-chromedriver>=3.5.0` (optional) - Enhanced stealth features

**Optional but recommended:**
```bash
pip install undetected-chromedriver
```

## Configuration

Edit [`config/settings.json`](config/settings.json) to configure:

- **Year Range**: Set `start` and `end` for the birth year range to search (e.g., 1990-2000)
- **Delays**: Adjust `min_seconds` (0.3) and `max_seconds` (0.6) for delays between searches
- **Pause Settings**: Configure `pause_every_n` (500) and `pause_duration` (5s)
- **Browser Mode**: Set `headless` to `true` (faster, invisible) or `false` (visible, easier debugging)
- **Workers**: Set `num_processes` (2-10) for number of parallel browser threads
- **Checkpoint**: Configure `checkpoint_interval` (5000) for progress save frequency
- **Batch Size**: Set `batch_size` (300) for result writing batch size
- **Paths**: Configure `output_dir`, `input_dir`, and `checkpoint_dir`

Example configuration:
```json
{
  "year_range": {
    "start": 1990,
    "end": 2000,
    "comment": "10-year range for typical searches"
  },
  "delays": {
    "min_seconds": 0.3,
    "max_seconds": 0.6,
    "comment": "SAFE: Balanced delays to avoid detection"
  },
  "pause_every_n": 500,
  "pause_duration": 5,
  "browser": {
    "headless": false,
    "comment": "Set to true for maximum performance (3-5x faster)"
  },
  "num_processes": 2,
  "comment_workers": "Number of parallel browser threads (2-10)",
  "use_multiprocessing": true,
  "checkpoint_interval": 5000,
  "batch_size": 300,
  "output_dir": "./data/results",
  "input_dir": "./data",
  "checkpoint_dir": "./checkpoints"
}
```

**Performance Tuning:**
- Start with 2 threads to test stability
- Increase to 3-4 if searches are stable
- More threads = faster but higher risk of detection/blocking
- Headless mode is 3-5x faster but may be blocked by some sites

## Usage

### 1. Prepare Input Excel File

The input Excel file should have the following columns:
- `first_name`: First name(s) (can be one or two names)
- `last_name_1`: First last name
- `last_name_2`: Second last name
- `gender`: Gender (`H` for Hombre/Male or `M` for Mujer/Female)

Example:
| first_name | last_name_1 | last_name_2 | gender |
|------------|-------------|-------------|--------|
| Eduardo    | Basich      | Muguiro     | H      |
| María      | González    | López       | M      |

**Note**: If no input file exists, the script will create a template file (`input_template.xlsx`) that you can fill with your data.

### 2. Run the Script

**Using the multithreading edition (recommended):**
```bash
python src/main_multiprocess.py input_file.xlsx
```

**Using the single-threaded version (slower but more stable):**
```bash
python src/main.py input_file.xlsx
```

If no filename is provided, it will look for `input_file.xlsx` in the current directory or `data` directory.

Example:
```bash
python src/main_multiprocess.py my_data.xlsx
```

**On first run:**
- Chrome will be downloaded automatically if needed (via webdriver-manager)
- Multiple browser windows will open (one per thread)
- You'll see initialization logs and progress updates

### 3. Monitor Progress

The script will:
- Display progress in the console with thread statistics
- Show initialization logs for each browser thread
- Log activities to `logs/curp_automation_main.log` and per-worker logs
- Save checkpoints periodically (every 5000 searches)
- Show match notifications when CURPs are found with worker ID
- Display performance metrics (searches/second per worker)

**Example output:**
```
2026-01-09 18:39:08 - INFO - ================================================================================
2026-01-09 18:39:08 - INFO - CURP SCRAPING - HIGH PERFORMANCE MULTITHREADING EDITION
2026-01-09 18:39:08 - INFO - Threads:             2 parallel workers
2026-01-09 18:39:08 - INFO - Headless Mode:       False
2026-01-09 18:39:08 - INFO - Delay Range:         0.3s - 0.6s (OPTIMIZED)
2026-01-09 18:39:08 - INFO - ================================================================================
2026-01-09 18:40:18 - Worker-1 - INFO - Worker 1: Browser initialized successfully
2026-01-09 18:40:26 - Worker-2 - INFO - Worker 2: Browser initialized successfully
2026-01-09 18:41:30 - Worker-1 - INFO - Worker 1: MATCH #1 - CURP BAED900815HTLSXD01
```

### 4. View Results

Results are saved to the `data/results` directory with timestamps:
- `curp_results_person_{ID}_{timestamp}.xlsx` - Individual person results

The Excel file contains two sheets:

**Results Sheet**: All found matches with:
- Person ID, name, gender
- Found CURP
- Birth date
- Birth state
- Worker ID (which thread found it)
- Timestamp

**Summary Sheet**: Summary per person with:
- Person ID, name
- Total matches found

**Batch Writing**: Results are written in batches of 300 matches for efficiency.

### 5. Resume Interrupted Search

If the script is interrupted (Ctrl+C) or crashes:
- A checkpoint is automatically saved
- Simply run the script again with the same input file
- It will automatically resume from the last position
- To start fresh, delete the checkpoint file in the `checkpoints` directory

## Performance Expectations

**With Multithreading (2-10 workers):**
- **Search Rate**: Approximately 2-5 searches per second per worker
  - 2 workers: ~4-10 searches/second total
  - 10 workers: ~20-50 searches/second total
- **Time per Person**: For a 10-year range (130,944 combinations):
  - 2 workers: ~6-18 hours per person
  - 4 workers: ~3-9 hours per person
  - 10 workers: ~1.2-4 hours per person

**Performance Factors:**
- Website response time (gob.mx can be slow: 30-60s initialization)
- Configured delays (0.3-0.6s is safe)
- Number of worker threads (2-10 recommended)
- Headless mode (3-5x faster than visible browsers)
- Your internet connection speed
- System RAM (each browser uses ~500MB-1GB)

**Important**: Performance is limited by website speed. Even with many workers, you may be bottlenecked by how fast gob.mx/curp responds.

**Estimated Times:**
- 2 persons, 10-year range: ~12-24 hours (2 workers)
- 10 persons, 10-year range: ~60-120 hours (2 workers)
- Scale linearly with more workers (up to website limits)

## Testing Strategy

Before running full searches, test with:
1. **1 person, 1 year, 1 state**: Verify the automation works
2. **1 person, 1 year, all states**: Test state selection
3. **1 person, 10 years, 1 state**: Test year range
4. **Gradually scale up**: Only run full searches after confirming everything works

## States Supported

The tool searches all 33 options:
- 32 Mexican states
- "Nacido en el extranjero" (Born abroad)

Complete list: Aguascalientes, Baja California, Baja California Sur, Campeche, Chiapas, Chihuahua, Coahuila, Colima, Durango, Guanajuato, Guerrero, Hidalgo, Jalisco, Michoacán, Morelos, Nayarit, Nuevo León, Oaxaca, Puebla, Querétaro, Quintana Roo, San Luis Potosí, Sinaloa, Sonora, Tabasco, Tamaulipas, Tlaxcala, Veracruz, Yucatán, Zacatecas, Ciudad de México, Nacido en el extranjero

## Troubleshooting

### Slow Website / Timeouts
- The gob.mx/curp website can be very slow (60-90 seconds to load)
- Reduce number of workers if you see frequent timeouts
- Check logs for "Timed out receiving message from renderer"
- Try disabling headless mode to see what's happening

### Browser Initialization Fails
- Ensure Google Chrome is installed on your system
- ChromeDriver will be downloaded automatically by webdriver-manager
- Check internet connection
- Try reducing the number of workers

### Form Not Loading
- The website may have changed its structure
- Run with headless=false to visually debug
- Check [`browser_automation.py`](src/browser_automation.py) for form selectors
- Website might be blocking automated access - try fewer workers

### Connection Reset / Network Errors
- Website may be rate limiting - reduce workers or increase delays
- Check your internet connection
- Try adding longer pauses between searches
- The script will retry automatically (3 attempts)

### High Memory Usage
- Each browser uses 500MB-1GB RAM
- Reduce `num_processes` if system is struggling
- Close other applications
- 2 workers recommended for 4GB RAM, 4-6 for 8GB RAM

### Script Crashes
- Check `logs/` directory for error details
- Checkpoint is saved automatically - just restart
- Try single-threaded version if multithreading is unstable

## Important Notes

⚠️ **Legal and Ethical Considerations**:
- This tool is for legitimate use cases only
- Respect the website's terms of service
- Do not abuse the system with excessive requests
- The rate limiting is intentionally conservative to be respectful

⚠️ **Technical Limitations**:
- The website (gob.mx/curp) can be very slow (60-90s initialization per browser)
- Performance is limited by website response time, not the script
- The website may change structure, breaking automation
- There is no official API, so this relies on browser automation
- Results depend on data available on the portal
- Website may implement rate limiting or blocking

⚠️ **No Guarantees**:
- Success depends on website availability and structure
- Anti-automation measures may be implemented at any time
- Use at your own risk

## Recent Updates (January 2026)

- ✅ Migrated from Playwright to Selenium WebDriver
- ✅ Implemented multithreading for parallel execution
- ✅ Added Windows compatibility (fixed pickle errors)
- ✅ Enhanced stealth features to avoid detection
- ✅ Improved error handling and retry logic
- ✅ Added per-worker logging
- ✅ Optimized timeouts for slow website responses
- ✅ Batch result writing for efficiency

---

**Version**: 2.0.0 (Multithreading Edition)  
**Last Updated**: January 2026
**Python Version**: 3.9+
**Status**: Active Development

```
CURP_Scraping/
├── src/
│   ├── __init__.py
│   ├── excel_handler.py          # Excel I/O operations
│   ├── combination_generator.py  # Generate date/state/year combos
│   ├── browser_automation.py     # Selenium automation with stealth
│   ├── result_validator.py       # Validate and extract CURPs
│   ├── checkpoint_manager.py     # Save/resume progress
│   ├── state_codes.py            # State code mappings
│   ├── multiprocess_worker.py    # Multithreading worker management
│   ├── performance_monitor.py    # Performance tracking
│   ├── main.py                   # Single-threaded orchestrator
│   └── main_multiprocess.py      # Multi-threaded orchestrator (recommended)
├── data/
│   ├── input_file.xlsx           # Your input data
│   └── results/                  # Output directory (auto-created)
├── config/
│   └── settings.json             # Configuration
├── logs/                         # Log files (auto-created)
│   ├── curp_automation_main.log  # Main process logs
│   └── worker_*.log              # Per-worker thread logs
├── checkpoints/                  # Checkpoint files (auto-created)
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── .gitignore
```

## License

This project is provided as-is for educational and legitimate use purposes.

## Support

For issues or questions:
1. Check the logs in `logs/curp_automation_main.log` and `logs/worker_*.log`
2. Review the configuration in [`config/settings.json`](config/settings.json)
3. Verify your input Excel file format
4. Test with 1-2 workers first before scaling up
5. Try single-threaded version ([`main.py`](src/main.py)) if multithreading has issues
6. Check if website structure has changed

**Common Solutions:**
- Slow performance → Website is slow, not the script
- Timeouts → Reduce workers, increase timeouts in code
- Browser won't start → Install Chrome, check internet connection
- High memory → Reduce num_processes in settings.json

---

**Version**: 2.0.0 (Multithreading Edition)  
**Last Updated**: January 2026
**Python Version**: 3.9+
**Status**: Active Development

