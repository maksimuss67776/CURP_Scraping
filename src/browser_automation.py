"""
Browser Automation
Handles browser automation using Playwright to interact with the CURP portal.
OPTIMIZED VERSION - Eliminates page reloads, uses smart waits
"""
import time
import random
from typing import Optional, Dict
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
from state_codes import get_state_code


class BrowserAutomation:
    """Handle browser automation for CURP searches."""
    
    def __init__(self, headless: bool = False, min_delay: float = 0.5, 
                 max_delay: float = 1.0, pause_every_n: int = 150, 
                 pause_duration: int = 10):
        """
        Initialize browser automation.
        
        Args:
            headless: Run browser in headless mode
            min_delay: Minimum delay between searches (seconds) - OPTIMIZED
            max_delay: Maximum delay between searches (seconds) - OPTIMIZED
            pause_every_n: Pause every N searches - OPTIMIZED (was 50)
            pause_duration: Duration of pause (seconds) - OPTIMIZED (was 30)
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
        self._form_ready = False  # Track if form is already loaded
    
    def start_browser(self):
        """Start browser and navigate to CURP page."""
        self.playwright = sync_playwright().start()
        
        # Launch browser with optimized settings
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',  # Don't load images - faster
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ]
        )
        
        # Create context with realistic settings
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Create page with optimized settings
        self.page = self.context.new_page()
        
        # Block unnecessary resources for faster loading
        self.page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2}", lambda route: route.abort())
        
        # Navigate to CURP page ONCE
        self._navigate_to_form()
    
    def _navigate_to_form(self):
        """Navigate to form page - called once at start and only if needed."""
        if not self.page:
            return
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use 'domcontentloaded' for faster initial load
                self.page.goto(self.url, wait_until='domcontentloaded', timeout=60000)
                
                # Wait for the tab to be available and click it
                self.page.wait_for_selector('a[href="#tab-02"]', timeout=15000)
                self.page.click('a[href="#tab-02"]')
                
                # Wait for form fields to be ready
                self.page.wait_for_selector('input#nombre', timeout=10000)
                
                self._form_ready = True
                return
                
            except Exception as e:
                print(f"Error navigating to {self.url} (attempt {attempt + 1}/{max_retries}): {e}")
                self._form_ready = False
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4 seconds
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
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
        """Apply random delay between searches - OPTIMIZED for speed while avoiding detection."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
    
    def _clear_form(self):
        """Clear form fields efficiently without page reload."""
        if not self.page:
            return
        
        try:
            # Clear all text inputs at once using JavaScript - much faster
            self.page.evaluate("""
                () => {
                    const fields = ['nombre', 'primerApellido', 'segundoApellido', 'selectedYear'];
                    fields.forEach(id => {
                        const el = document.getElementById(id);
                        if (el) el.value = '';
                    });
                }
            """)
        except:
            pass
    
    def _close_modal_if_present(self):
        """Close the error modal if it appears (no match found) - OPTIMIZED."""
        if not self.page:
            return
        
        try:
            # Use JavaScript for faster modal closing - check and close in one operation
            self.page.evaluate("""
                () => {
                    const closeBtn = document.querySelector('button[data-dismiss="modal"]');
                    if (closeBtn && closeBtn.offsetParent !== null) closeBtn.click();
                }
            """, timeout=1000)
        except:
            pass
    
    def _ensure_form_ready(self):
        """Ensure form is ready, reload only if necessary."""
        if not self._form_ready or not self.page:
            self._navigate_to_form()
            return
        
        # Quick check if we're still on the right page
        try:
            # Check if form element exists
            form_exists = self.page.query_selector('input#nombre')
            if not form_exists:
                self._navigate_to_form()
        except:
            self._navigate_to_form()
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> str:
        """
        Search for CURP with given parameters - OPTIMIZED VERSION.
        No page reload - clears form and refills instead.
        
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
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Ensure form is ready (no page reload if already ready)
                self._ensure_form_ready()
                
                # Close any open modal from previous search
                self._close_modal_if_present()
                
                # Clear form efficiently
                self._clear_form()
                
                # Fill form fields using JavaScript for maximum speed
                day_str = str(day).zfill(2)
                month_str = str(month).zfill(2)
                year_str = str(year)
                gender_value = "H" if gender.upper() == "H" else "M"
                state_code = get_state_code(state)
                
                # Use JavaScript to fill all fields at once - MUCH faster
                self.page.evaluate(f"""
                    () => {{
                        document.getElementById('nombre').value = '{first_name}';
                        document.getElementById('primerApellido').value = '{last_name_1}';
                        document.getElementById('segundoApellido').value = '{last_name_2}';
                        document.getElementById('selectedYear').value = '{year_str}';
                        
                        // Set select values
                        document.getElementById('diaNacimiento').value = '{day_str}';
                        document.getElementById('mesNacimiento').value = '{month_str}';
                        document.getElementById('sexo').value = '{gender_value}';
                        document.getElementById('claveEntidad').value = '{state_code}';
                        
                        // Trigger change events for selects
                        ['diaNacimiento', 'mesNacimiento', 'sexo', 'claveEntidad'].forEach(id => {{
                            const el = document.getElementById(id);
                            if (el) el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        }});
                    }}
                """)
                
                # Submit form using JavaScript
                try:
                    self.page.evaluate("""
                        () => {
                            const submitBtn = document.querySelector('button[type="submit"]') || 
                                             document.querySelector('input[type="submit"]') ||
                                             document.querySelector('button.btn-primary');
                            if (submitBtn) submitBtn.click();
                        }
                    """)
                except:
                    # Fallback
                    self.page.keyboard.press('Enter')
                
                # OPTIMIZED: Wait for response with smart timeout
                try:
                    # Wait for either error modal OR results table - whichever comes first
                    self.page.wait_for_selector(
                        'button[data-dismiss="modal"], #dwnldLnk, table.table',
                        timeout=8000
                    )
                except:
                    # If timeout, still try to get content
                    pass
                
                # Close modal if present (no match)
                self._close_modal_if_present()
                
                # Get page content
                content = self.page.content()
                
                # Increment search count
                self.search_count += 1
                
                # Apply delay after search (respects rate limiting)
                self._random_delay()
                
                # Periodic pause to avoid detection - OPTIMIZED frequency
                if self.search_count % self.pause_every_n == 0:
                    print(f"Pausing for {self.pause_duration} seconds after {self.search_count} searches...")
                    time.sleep(self.pause_duration)
                
                return content
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error during search (attempt {attempt + 1}/{max_retries}): {e}")
                    # Mark form as not ready to trigger reload on retry
                    self._form_ready = False
                    time.sleep(1)  # Brief pause before retry
                else:
                    print(f"Error during search (final attempt): {e}")
                    self._form_ready = False
                    return ""
    
    def __enter__(self):
        """Context manager entry."""
        self.start_browser()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_browser()
