"""
Browser Automation - ROBUST VERSION with Rate Limit Detection
"""
import time
import random
import threading
import logging
from typing import Optional, Dict, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
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

# Logger
logger = logging.getLogger(__name__)


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


class SearchResult:
    """Result of a CURP search."""
    SUCCESS = "success"
    NOT_FOUND = "not_found"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    BROWSER_CRASHED = "browser_crashed"


class BrowserAutomation:
    """Handle browser automation - ROBUST version with rate limit detection."""
    
    def __init__(self, headless: bool = False, min_delay: float = 0.3, 
                 max_delay: float = 0.6, pause_every_n: int = 100, 
                 pause_duration: int = 10, worker_id: int = 0):
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.pause_every_n = pause_every_n
        self.pause_duration = pause_duration
        self.worker_id = worker_id
        
        self.driver: Optional[webdriver.Chrome] = None
        self.search_count = 0
        self.url = "https://www.gob.mx/curp/"
        self._form_ready = False
        self.consecutive_errors = 0
        self.rate_limit_count = 0
    
    def _log(self, msg: str):
        """Log message with worker ID."""
        logger.info(f"[Worker-{self.worker_id}] {msg}")
    
    def _get_random_user_agent(self) -> str:
        """Get a random realistic user agent."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        ]
        return random.choice(user_agents)
    
    def is_browser_alive(self) -> bool:
        """Check if browser is still responsive."""
        if not self.driver:
            return False
        try:
            _ = self.driver.current_url
            return True
        except:
            return False
    
    def start_browser(self) -> bool:
        """Start browser with stealth settings. Returns True if successful."""
        try:
            # Stagger initialization
            time.sleep(random.uniform(0.5, 2.0))
            
            if USE_UNDETECTED:
                self._start_undetected_browser()
            else:
                self._start_standard_browser()
            
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(0)
            
            # Navigate to CURP page
            self._navigate_to_form()
            return True
        except Exception as e:
            self._log(f"Failed to start browser: {e}")
            return False
    
    def _start_undetected_browser(self):
        """Start browser using undetected-chromedriver."""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'--user-agent={self._get_random_user_agent()}')
        options.add_argument('--disable-extensions')
        
        self.driver = uc.Chrome(options=options, use_subprocess=True)
        self._inject_stealth_scripts()
    
    def _start_standard_browser(self):
        """Start browser using standard Selenium."""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
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
        options.page_load_strategy = 'eager'
        
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
        """Navigate to form page."""
        if not self.driver:
            return
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[Browser] Navigating to {self.url} (attempt {attempt + 1}/{max_retries})...")
                self.driver.get(self.url)
                print(f"[Browser] Page loaded, waiting for ready state...")
                
                WebDriverWait(self.driver, 30).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                print(f"[Browser] Page ready, waiting for tab link...")
                
                time.sleep(random.uniform(0.5, 1.0))
                
                tab_link = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="#tab-02"]'))
                )
                print(f"[Browser] Tab link found, clicking...")
                
                self.driver.execute_script("arguments[0].click();", tab_link)
                print(f"[Browser] Tab clicked, waiting for form...")
                
                time.sleep(0.5)
                
                WebDriverWait(self.driver, 20).until(
                    lambda d: d.find_element(By.ID, "nombre").is_displayed()
                )
                
                self._form_ready = True
                print(f"[Browser] Form ready! Browser initialization complete.")
                return
                
            except Exception as e:
                print(f"Error navigating (attempt {attempt + 1}/{max_retries}): {e}")
                self._form_ready = False
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
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
            self._form_ready = False
    
    def restart_browser(self) -> bool:
        """Restart the browser. Returns True if successful."""
        self._log("Restarting browser...")
        self.close_browser()
        time.sleep(2)
        return self.start_browser()
    
    def _random_delay(self):
        """Apply random delay."""
        base_delay = random.uniform(self.min_delay, self.max_delay)
        if random.random() < 0.05:
            base_delay += random.uniform(1.0, 2.0)
        time.sleep(base_delay)
    
    def _close_modal_if_present(self):
        """Close error modal if present."""
        if not self.driver:
            return
        
        try:
            self.driver.execute_script("""
                var btns = document.querySelectorAll('button[data-dismiss="modal"], button.btn-default');
                for (var i = 0; i < btns.length; i++) {
                    if (btns[i].offsetParent !== null) {
                        btns[i].click();
                        break;
                    }
                }
            """)
        except:
            pass
    
    def _ensure_form_ready(self):
        """Ensure form is ready - click tab if needed."""
        if not self._form_ready or not self.driver:
            self._navigate_to_form()
            return
        
        try:
            nombre_visible = self.driver.execute_script(
                "var el = document.getElementById('nombre'); return el && el.offsetParent !== null;"
            )
            if not nombre_visible:
                self.driver.execute_script("""
                    var tab = document.querySelector('a[href="#tab-02"]');
                    if (tab) tab.click();
                """)
                time.sleep(0.5)
                nombre_visible = self.driver.execute_script(
                    "var el = document.getElementById('nombre'); return el && el.offsetParent !== null;"
                )
                if not nombre_visible:
                    self._navigate_to_form()
        except:
            self._navigate_to_form()
    
    def _detect_rate_limit(self, html_content: str) -> bool:
        """Detect if we're being rate limited."""
        rate_limit_indicators = [
            'rate limit',
            'too many requests',
            'intente más tarde',
            'servicio no disponible',
            'service unavailable',
            '429',
            'blocked',
            'access denied',
        ]
        
        html_lower = html_content.lower()
        for indicator in rate_limit_indicators:
            if indicator in html_lower:
                self._log(f"RATE LIMIT DETECTED: Found '{indicator}' in response")
                return True
        
        # Also check if response is suspiciously short
        if len(html_content) < 1000:
            self._log(f"WARNING: Very short response ({len(html_content)} bytes) - possible rate limit")
            return True
        
        return False
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> Tuple[str, str]:
        """
        Search for CURP with robust error handling.
        
        Returns:
            Tuple of (status, html_content) where status is one of:
            - SearchResult.SUCCESS: Search completed, check html for result
            - SearchResult.RATE_LIMITED: Rate limited, should wait before retry
            - SearchResult.BROWSER_CRASHED: Browser crashed, needs restart
            - SearchResult.ERROR: Other error
        """
        if not self.driver or not self.is_browser_alive():
            return (SearchResult.BROWSER_CRASHED, "")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Ensure form is ready
                self._ensure_form_ready()
                self._close_modal_if_present()
                
                # Prepare values
                day_str = str(day).zfill(2)
                month_str = str(month).zfill(2)
                year_str = str(year)
                gender_value = "H" if gender.upper() == "H" else "M"
                state_code = get_state_code(state)
                
                # Fill form using Selenium
                nombre = self.driver.find_element(By.ID, "nombre")
                nombre.clear()
                nombre.send_keys(first_name)
                
                apellido1 = self.driver.find_element(By.ID, "primerApellido")
                apellido1.clear()
                apellido1.send_keys(last_name_1)
                
                apellido2 = self.driver.find_element(By.ID, "segundoApellido")
                apellido2.clear()
                apellido2.send_keys(last_name_2)
                
                Select(self.driver.find_element(By.ID, "diaNacimiento")).select_by_value(day_str)
                Select(self.driver.find_element(By.ID, "mesNacimiento")).select_by_value(month_str)
                
                anio = self.driver.find_element(By.ID, "selectedYear")
                anio.clear()
                anio.send_keys(year_str)
                
                Select(self.driver.find_element(By.ID, "sexo")).select_by_value(gender_value)
                Select(self.driver.find_element(By.ID, "claveEntidad")).select_by_value(state_code)
                
                # Small delay for form validation
                time.sleep(0.3)
                
                # Click search button
                search_btn = self.driver.find_element(By.ID, "searchButton")
                search_btn.click()
                
                # Wait for result or error
                try:
                    WebDriverWait(self.driver, 15).until(
                        lambda d: d.execute_script("""
                            if (document.getElementById('dwnldLnk') !== null) return true;
                            var tables = document.querySelectorAll('table');
                            for (var i = 0; i < tables.length; i++) {
                                if (tables[i].innerHTML.indexOf('CURP:') > -1) return true;
                            }
                            if (document.body.innerHTML.indexOf('los datos ingresados no son correctos') > -1) return true;
                            var modal = document.querySelector('.modal.in, .modal.show');
                            if (modal && modal.querySelector('.modal-body')) return true;
                            return false;
                        """)
                    )
                except TimeoutException:
                    pass
                
                time.sleep(0.3)
                self._close_modal_if_present()
                
                content = self.driver.page_source
                self.search_count += 1
                
                # Check for rate limiting
                if self._detect_rate_limit(content):
                    self.rate_limit_count += 1
                    self._log(f"Rate limit #{self.rate_limit_count} - waiting 30 seconds...")
                    time.sleep(30)
                    if self.rate_limit_count >= 3:
                        self._log("Too many rate limits - waiting 2 minutes...")
                        time.sleep(120)
                        self.rate_limit_count = 0
                    return (SearchResult.RATE_LIMITED, content)
                
                # Reset error counters on success
                self.consecutive_errors = 0
                
                # Apply delay
                self._random_delay()
                
                # Periodic pause
                if self.search_count % self.pause_every_n == 0:
                    pause_time = self.pause_duration + random.uniform(-2, 3)
                    self._log(f"Pausing for {pause_time:.0f} seconds after {self.search_count} searches...")
                    time.sleep(pause_time)
                
                return (SearchResult.SUCCESS, content)
                
            except (WebDriverException, StaleElementReferenceException) as e:
                self.consecutive_errors += 1
                self._log(f"Browser error (attempt {attempt + 1}/{max_retries}): {e}")
                
                if self.consecutive_errors >= 5 or not self.is_browser_alive():
                    return (SearchResult.BROWSER_CRASHED, "")
                
                self._form_ready = False
                time.sleep(2)
                
            except Exception as e:
                self.consecutive_errors += 1
                self._log(f"Error during search (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    self._form_ready = False
                    time.sleep(2)
        
        return (SearchResult.ERROR, "")
    
    def __enter__(self):
        self.start_browser()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()
