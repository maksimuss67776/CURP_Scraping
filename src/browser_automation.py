"""
Browser Automation
Handles browser automation using Playwright to interact with the CURP portal.
"""
import time
import random
from typing import Optional, Dict
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
from state_codes import get_state_code


class BrowserAutomation:
    """Handle browser automation for CURP searches."""
    
    def __init__(self, headless: bool = False, min_delay: float = 2.0, 
                 max_delay: float = 5.0, pause_every_n: int = 50, 
                 pause_duration: int = 30):
        """
        Initialize browser automation.
        
        Args:
            headless: Run browser in headless mode
            min_delay: Minimum delay between searches (seconds)
            max_delay: Maximum delay between searches (seconds)
            pause_every_n: Pause every N searches
            pause_duration: Duration of pause (seconds)
        """
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        self.search_count = 0
        self.url = "https://www.gob.mx/curp/"
    
    def start_browser(self):
        """Start browser and navigate to CURP page."""
        self.playwright = sync_playwright().start()
        
        # Launch browser
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # Create context with realistic settings
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Create page
        self.page = self.context.new_page()
        
        # Navigate to CURP page
        try:
            # Use 'load' instead of 'networkidle' for faster loading
            # Increase timeout to 60 seconds
            self.page.goto(self.url, wait_until='load', timeout=60000)
            time.sleep(2)  # Wait for page to fully load
            
            # Click on "Datos Personales" tab to access the form
            try:
                # Wait for the tab to be available
                self.page.wait_for_selector('a[href="#tab-02"]', timeout=15000)
                # Click the "Datos Personales" tab
                self.page.click('a[href="#tab-02"]')
                time.sleep(1)  # Wait for tab to switch
            except Exception as e:
                print(f"Warning: Could not click 'Datos Personales' tab: {e}")
        except Exception as e:
            print(f"Error navigating to {self.url}: {e}")
            raise
    
    def close_browser(self):
        """Close browser and cleanup."""
        if self.page:
            try:
                self.page.close()
            except:
                pass
        if self.context:
            try:
                self.context.close()
            except:
                pass
        if self.browser:
            try:
                self.browser.close()
            except:
                pass
        if self.playwright:
            try:
                self.playwright.stop()
            except:
                pass
    
    def _random_delay(self):
        """Apply random delay between searches."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
    
    def _close_modal_if_present(self):
        """Close the error modal if it appears (no match found)."""
        if not self.page:
            return
        
        try:
            # Check for the modal close button
            close_button = self.page.query_selector('button[data-dismiss="modal"]')
            if close_button:
                close_button.click()
                time.sleep(0.5)  # Wait for modal to close
        except:
            pass
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> str:
        """
        Search for CURP with given parameters.
        
        Args:
            first_name: First name(s)
            last_name_1: First last name
            last_name_2: Second last name
            gender: Gender (H or M)
            day: Day of birth (1-31)
            month: Month of birth (1-12)
            state: State name
            year: Year of birth
            
        Returns:
            HTML content of the result page
        """
        if not self.page:
            raise RuntimeError("Browser not started. Call start_browser() first.")
        
        try:
            # Navigate to the CURP page (refresh to reset form)
            # Use 'load' instead of 'networkidle' for faster loading
            self.page.goto(self.url, wait_until='load', timeout=60000)
            time.sleep(1)
            
            # Click on "Datos Personales" tab to access the form
            try:
                self.page.wait_for_selector('a[href="#tab-02"]', timeout=10000)
                self.page.click('a[href="#tab-02"]')
                time.sleep(0.5)  # Wait for tab to switch
            except Exception as e:
                print(f"Warning: Could not click 'Datos Personales' tab: {e}")
            
            # Fill form fields using the actual IDs from the website
            
            # First name (nombres)
            self.page.fill('input#nombre', first_name, timeout=5000)
            
            # First last name (primerApellido)
            self.page.fill('input#primerApellido', last_name_1, timeout=5000)
            
            # Second last name (segundoApellido)
            self.page.fill('input#segundoApellido', last_name_2, timeout=5000)
            
            # Day - format as "01", "02", etc.
            day_str = str(day).zfill(2)
            self.page.select_option('select#diaNacimiento', day_str, timeout=5000)
            
            # Month - format as "01", "02", etc.
            month_str = str(month).zfill(2)
            self.page.select_option('select#mesNacimiento', month_str, timeout=5000)
            
            # Year
            year_str = str(year)
            self.page.fill('input#selectedYear', year_str, timeout=5000)
            
            # Gender (sexo) - values: "H", "M", or "X"
            gender_value = "H" if gender.upper() == "H" else "M"
            self.page.select_option('select#sexo', gender_value, timeout=5000)
            
            # State (claveEntidad) - convert state name to code
            state_code = get_state_code(state)
            self.page.select_option('select#claveEntidad', state_code, timeout=5000)
            
            # Submit form - look for submit button
            time.sleep(0.5)  # Small delay before submit
            # Try to find and click submit button
            try:
                # Look for common submit button patterns
                submit_button = self.page.query_selector('button[type="submit"]')
                if not submit_button:
                    submit_button = self.page.query_selector('input[type="submit"]')
                if not submit_button:
                    submit_button = self.page.query_selector('button:has-text("Buscar")')
                if not submit_button:
                    submit_button = self.page.query_selector('button:has-text("Consultar")')
                
                if submit_button:
                    submit_button.click()
                else:
                    # Try pressing Enter on the form
                    self.page.keyboard.press('Enter')
            except Exception as e:
                print(f"Warning: Could not find submit button, trying Enter key: {e}")
                self.page.keyboard.press('Enter')
            
            # Wait for results (either modal or results table)
            time.sleep(3)  # Give page time to render
            # Wait for either the modal or results table to appear
            try:
                # Wait for either error modal or results table
                self.page.wait_for_selector('button[data-dismiss="modal"], table, #dwnldLnk', timeout=15000)
            except:
                # If neither appears, just continue with current page state
                pass
            
            # Check if modal appeared (no match) and close it
            self._close_modal_if_present()
            
            # Get page content
            content = self.page.content()
            
            # Increment search count
            self.search_count += 1
            
            # Apply delay after search
            self._random_delay()
            
            # Pause every N searches
            if self.search_count % self.pause_every_n == 0:
                print(f"Pausing for {self.pause_duration} seconds after {self.search_count} searches...")
                time.sleep(self.pause_duration)
            
            return content
            
        except Exception as e:
            print(f"Error during search: {e}")
            # Return empty content on error
            return ""
    
    def __enter__(self):
        """Context manager entry."""
        self.start_browser()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_browser()

