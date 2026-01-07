# CURP Automation Tool

Automated tool for searching Mexican CURP (Clave Única de Registro de Población) on the official government portal (gob.mx/curp/) using controlled browser automation.

## Overview

This tool reads personal data from an Excel file, generates combinations of birth dates, months, states, and years, and performs controlled searches on the official CURP portal to find valid CURP matches. Results are saved to an Excel file with found CURPs, birth dates, states, and summary statistics.

## Features

- **Excel Input/Output**: Read input data from Excel and export results to Excel format
- **Combination Generation**: Automatically generates all combinations of dates (1-31), months (1-12), 33 states/options, and configurable year ranges
- **Controlled Browser Automation**: Uses Playwright for reliable browser automation with rate limiting
- **Checkpoint System**: Saves progress and allows resuming interrupted searches
- **Result Validation**: Validates and extracts CURP information from search results
- **Rate Limiting**: Configurable delays (2-5 seconds) with randomization to avoid detection
- **Error Handling**: Robust error handling for network issues, CAPTCHAs, and browser crashes

## Requirements

- Python 3.9 or higher
- Windows, macOS, or Linux

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

4. **Install Playwright browsers:**
   ```bash
   playwright install chromium
   ```

## Configuration

Edit `config/settings.json` to configure:

- **Year Range**: Set `start_year` and `end_year` for the birth year range to search
- **Delays**: Adjust `min_seconds` and `max_seconds` for delays between searches
- **Pause Settings**: Configure `pause_every_n` (pause frequency) and `pause_duration` (pause length in seconds)
- **Browser Mode**: Set `headless` to `true` or `false` (false shows browser window)
- **Paths**: Configure `output_dir`, `input_dir`, and `checkpoint_dir`

Example configuration:
```json
{
  "year_range": {
    "start": 1950,
    "end": 1960
  },
  "delays": {
    "min_seconds": 2,
    "max_seconds": 5
  },
  "pause_every_n": 50,
  "pause_duration": 30,
  "browser": {
    "headless": false
  },
  "output_dir": "./data/results",
  "input_dir": "./data",
  "checkpoint_dir": "./checkpoints"
}
```

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

```bash
python src/main.py [input_filename.xlsx]
```

If no filename is provided, it will look for `input.xlsx` in the `data` directory.

Example:
```bash
python src/main.py my_data.xlsx
```

### 3. Monitor Progress

The script will:
- Display progress in the console
- Log activities to `logs/curp_automation.log`
- Save checkpoints periodically (every 100 combinations)
- Show match notifications when CURPs are found

### 4. View Results

Results are saved to the `data/results` directory with a timestamp:
- `curp_results_YYYYMMDD_HHMMSS.xlsx`

The Excel file contains two sheets:

**Results Sheet**: All found matches with:
- Person ID, name, gender
- Found CURP
- Birth date
- Birth state
- Match number

**Summary Sheet**: Summary per person with:
- Person ID, name
- Total matches found

### 5. Resume Interrupted Search

If the script is interrupted (Ctrl+C) or crashes:
- A checkpoint is automatically saved
- Simply run the script again with the same input file
- It will automatically resume from the last position
- To start fresh, delete the checkpoint file in the `checkpoints` directory

## Performance Expectations

- **Search Rate**: Approximately 12-30 searches per minute (depending on configured delays)
- **Time per Person**: For a 10-year range:
  - Total combinations: ~122,760 per person
  - Estimated time: ~68-170 hours per person (at 2-5 second delays)
  
**Important**: This is intentionally slow to avoid getting blocked by the portal. Adjust delays at your own risk.

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

### CAPTCHA Detected
- If CAPTCHA appears, the script will pause and prompt you to solve it manually
- Press Enter after solving to continue

### Browser Issues
- If browser fails to start, ensure Playwright browsers are installed: `playwright install chromium`
- Try running with `headless: false` to see what's happening

### Form Field Issues
- The website structure may change. If searches fail, check the form field selectors in `src/browser_automation.py`
- You may need to inspect the website and update the selectors

### Network Errors
- Check your internet connection
- The script will retry, but persistent failures may require manual intervention

## Important Notes

⚠️ **Legal and Ethical Considerations**:
- This tool is for legitimate use cases only
- Respect the website's terms of service
- Do not abuse the system with excessive requests
- The rate limiting is intentionally conservative

⚠️ **No Guarantees**:
- The website structure may change, breaking the automation
- There is no official API, so this relies on web scraping
- Results depend on data available on the portal

## Project Structure

```
CURP-scraping/
├── src/
│   ├── __init__.py
│   ├── excel_handler.py          # Excel I/O operations
│   ├── combination_generator.py  # Generate date/state/year combos
│   ├── browser_automation.py     # Playwright automation
│   ├── result_validator.py       # Validate and extract CURPs
│   ├── checkpoint_manager.py     # Save/resume progress
│   └── main.py                   # Main orchestrator
├── data/
│   ├── input_template.xlsx       # Input template
│   └── results/                  # Output directory
├── config/
│   └── settings.json             # Configuration
├── logs/                         # Log files
├── checkpoints/                  # Checkpoint files
├── requirements.txt
├── README.md
└── .gitignore
```

## License

This project is provided as-is for educational and legitimate use purposes.

## Support

For issues or questions:
1. Check the logs in `logs/curp_automation.log`
2. Review the configuration in `config/settings.json`
3. Verify your input Excel file format
4. Test with a small subset first

---

**Version**: 1.0.0  
**Last Updated**: 2024

