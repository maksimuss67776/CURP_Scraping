"""
Browser Automation - OPTIMIZED VERSION
Combines OLD reliability + NEW stealth
Fixes rate limiting and loading issues
"""
import time
import random
import threading
from typing import Optional, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, WebDriverException
from state_codes import get_state_code

# Try undetected-chromedriver for stealth
try:
    import undetected_chromedriver as uc
    USE_UNDETECTED = True
except ImportError:
    USE_UNDETECTED = False

# Try webdriver_manager
try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WEBDRIVER_MANAGER = True
except ImportError:
    USE_WEBDRIVER_MANAGER = False

# Global cached chromedriver path
_chromedriver_path = None
_chromedriver_lock = threading.Lock()


def get_chromedriver_path():
    """Get chromedriver path with thread-safe caching."""
    global _chromedriver_path
    
    if _chromedriver_path is not None:
        return _chromedriver_path
    
    with _chromedriver_lock:
        if _chromedriver_path is not None:
            return _chromedriver_path
        
        if USE_WEBDRIVER_MANAGER:
            _chromedriver_path = ChromeDriverManager().install()
        else:
            _chromedriver_path = None
        
        return _chromedriver_path


class BrowserAutomation:
    """Handle browser automation - OPTIMIZED for rate limiting."""
    
    def __init__(self, headless: bool = False, min_delay: float = 1.0, 
                 max_delay: float = 2.0, pause_every_n: int = 100, 
                 pause_duration: int = 10):
        """
        Initialize browser automation - OPTIMIZED.
        
        Args:
            headless: Run browser in headless mode
            min_delay: Minimum delay between searches (1.0s recommended)
            max_delay: Maximum delay between searches (2.0s recommended)
            pause_every_n: Pause every N searches (100 recommended)
            pause_duration: Duration of pause (10s recommended)
        """
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        
        self.driver: Optional[webdriver.Chrome] = None
        self.search_count = 0
        self.url = "https://www.gob.mx/curp/"
        self._form_ready = False
    
    def _get_random_user_agent(self) -> str:
        """Get a random realistic user agent."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        return random.choice(user_agents)
    
    def start_browser(self):
        """Start browser with stealth settings."""
        # Stagger initialization
        time.sleep(random.uniform(2.0, 4.0))
        
        if USE_UNDETECTED:
            self._start_undetected_browser()
        else:
            self._start_standard_browser()
        
        # Set timeouts - INCREASED for slow website
        self.driver.set_page_load_timeout(120)  # 2 minutes
        self.driver.implicitly_wait(0)
        
        # Navigate to CURP page
        self._navigate_to_form()
    
    def _start_undetected_browser(self):
        """Start browser using undetected-chromedriver."""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Stealth + VPS optimization
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'--user-agent={self._get_random_user_agent()}')
        
        self.driver = uc.Chrome(options=options, use_subprocess=True)
        self._inject_stealth_scripts()
    
    def _start_standard_browser(self):
        """Start browser using standard Selenium."""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Stealth + VPS optimization
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'--user-agent={self._get_random_user_agent()}')
        
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        prefs = {
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
        }
        options.add_experimental_option('prefs', prefs)
        
        # Page load strategy - wait for full load
        options.page_load_strategy = 'normal'  # Changed from 'eager' to 'normal'
        
        driver_path = get_chromedriver_path()
        if driver_path:
            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
        else:
            self.driver = webdriver.Chrome(options=options)
        
        self._inject_stealth_scripts()
    
    def _inject_stealth_scripts(self):
        """Inject JavaScript to hide automation."""
        if not self.driver:
            return
        
        stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['es-MX', 'es', 'en']});
        window.chrome = {runtime: {}, loadTimes: function() {}, csi: function() {}, app: {}};
        """
        
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_js})
        except:
            try:
                self.driver.execute_script(stealth_js)
            except:
                pass
    
    def _navigate_to_form(self):
        """Navigate to form page - OPTIMIZED with better waiting."""
        if not self.driver:
            return
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[Browser] Navigating to {self.url} (attempt {attempt + 1}/{max_retries})...")
                
                # Navigate with longer timeout
                self.driver.get(self.url)
                print(f"[Browser] Page loaded, waiting for ready state...")
                
                # Wait for page to be fully ready - INCREASED timeout
                WebDriverWait(self.driver, 90).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                print(f"[Browser] Page ready, waiting for tab link...")
                
                # Wait longer before interacting
                time.sleep(random.uniform(2.0, 4.0))
                
                # Wait for the tab link - INCREASED timeout
                tab_link = WebDriverWait(self.driver, 60).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="#tab-02"]'))
                )
                print(f"[Browser] Tab link found, clicking...")
                
                # Scroll and click
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", tab_link)
                time.sleep(random.uniform(0.5, 1.0))
                
                self.driver.execute_script("arguments[0].click();", tab_link)
                print(f"[Browser] Tab clicked, waiting for form...")
                
                # Wait for form to be visible - INCREASED timeout
                time.sleep(random.uniform(1.0, 2.0))
                
                WebDriverWait(self.driver, 60).until(
                    lambda d: d.find_element(By.ID, "nombre").is_displayed()
                )
                
                self._form_ready = True
                print(f"[Browser] Form ready! Browser initialization complete.")
                return
                
            except Exception as e:
                print(f"Error navigating (attempt {attempt + 1}/{max_retries}): {e}")
                self._form_ready = False
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
    
    def close_browser(self):
        """Close browser and cleanup."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def _random_delay(self):
        """Apply random delay - OPTIMIZED to avoid rate limiting."""
        base_delay = random.uniform(self.min_delay, self.max_delay)
        
        # Add occasional longer pause (10% chance)
        if random.random() < 0.10:
            base_delay += random.uniform(2.0, 5.0)
        
        time.sleep(base_delay)
    
    def _close_modal_if_present(self):
        """Close error modal if present."""
        if not self.driver:
            return
        
        try:
            time.sleep(0.5)
            close_btns = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-dismiss="modal"], button.btn-default')
            for btn in close_btns:
                try:
                    if btn.is_displayed():
                        btn.click()
                        time.sleep(0.5)
                        break
                except:
                    continue
        except:
            pass
    
    def _ensure_form_ready(self):
        """Ensure form is ready - navigate back if needed."""
        if not self._form_ready or not self.driver:
            self._navigate_to_form()
            return
        
        try:
            # Check if form is still visible
            nombre_field = self.driver.find_element(By.ID, "nombre")
            if not nombre_field.is_displayed():
                # Navigate back to form
                self._navigate_to_form()
        except:
            self._navigate_to_form()
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> str:
        """
        Search for CURP - OPTIMIZED to handle rate limiting.
        
        Returns:
            HTML content of the result page
        """
        if not self.driver:
            raise RuntimeError("Browser not started. Call start_browser() first.")
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Ensure form is ready
                self._ensure_form_ready()
                
                # Close any modal
                self._close_modal_if_present()
                
                # Clear form
                try:
                    self.driver.find_element(By.ID, "nombre").clear()
                    self.driver.find_element(By.ID, "primerApellido").clear()
                    self.driver.find_element(By.ID, "segundoApellido").clear()
                    self.driver.find_element(By.ID, "selectedYear").clear()
                    time.sleep(0.3)
                except:
                    pass
                
                # Fill form fields
                day_str = str(day).zfill(2)
                month_str = str(month).zfill(2)
                year_str = str(year)
                gender_value = "H" if gender.upper() == "H" else "M"
                state_code = get_state_code(state)
                
                # Fill with small delays
                self.driver.find_element(By.ID, "nombre").send_keys(first_name)
                time.sleep(0.2)
                self.driver.find_element(By.ID, "primerApellido").send_keys(last_name_1)
                time.sleep(0.2)
                self.driver.find_element(By.ID, "segundoApellido").send_keys(last_name_2)
                time.sleep(0.2)
                
                Select(self.driver.find_element(By.ID, "diaNacimiento")).select_by_value(day_str)
                time.sleep(0.2)
                Select(self.driver.find_element(By.ID, "mesNacimiento")).select_by_value(month_str)
                time.sleep(0.2)
                self.driver.find_element(By.ID, "selectedYear").send_keys(year_str)
                time.sleep(0.2)
                Select(self.driver.find_element(By.ID, "sexo")).select_by_value(gender_value)
                time.sleep(0.2)
                Select(self.driver.find_element(By.ID, "claveEntidad")).select_by_value(state_code)
                time.sleep(0.5)
                
                # Submit form
                try:
                    submit_btn = self.driver.find_element(By.ID, "searchButton")
                    submit_btn.click()
                except:
                    self.driver.execute_script("document.getElementById('searchButton').click();")
                
                # CRITICAL FIX: Wait for loading spinner to disappear
                # This is what was causing the issue in the image
                try:
                    # Wait for spinner to appear first (if it does)
                    time.sleep(1.0)
                    
                    # Then wait for it to disappear - INCREASED timeout to 30s
                    WebDriverWait(self.driver, 30).until_not(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '.spinner, .loading, [class*="spin"], [class*="load"]'))
                    )
                except:
                    # If no spinner found, just wait a bit
                    time.sleep(2.0)
                
                # Wait for response - INCREASED timeout
                try:
                    WebDriverWait(self.driver, 20).until(
                        lambda d: d.execute_script("""
                            return document.querySelector('button[data-dismiss="modal"]') !== null ||
                                   document.getElementById('dwnldLnk') !== null ||
                                   document.querySelector('table.table') !== null;
                        """)
                    )
                except TimeoutException:
                    # Timeout is okay, check content anyway
                    pass
                
                # Wait a bit more for content to stabilize
                time.sleep(1.0)
                
                # Close modal if present
                self._close_modal_if_present()
                
                # Get page content
                content = self.driver.page_source
                
                # Increment search count
                self.search_count += 1
                
                # Apply delay
                self._random_delay()
                
                # Periodic pause
                if self.search_count % self.pause_every_n == 0:
                    pause_time = self.pause_duration + random.uniform(-2, 3)
                    print(f"Pausing for {pause_time:.0f} seconds after {self.search_count} searches...")
                    time.sleep(pause_time)
                
                return content
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error during search (attempt {attempt + 1}/{max_retries}): {e}")
                    self._form_ready = False
                    time.sleep(3)
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
