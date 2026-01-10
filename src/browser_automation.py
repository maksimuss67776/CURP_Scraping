"""
Browser Automation - FIXED VERSION
Reliable form filling with proper Ember.js event handling.
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
    """Handle browser automation - RELIABLE version."""
    
    def __init__(self, headless: bool = False, min_delay: float = 0.3, 
                 max_delay: float = 0.6, pause_every_n: int = 100, 
                 pause_duration: int = 10):
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
        ]
        return random.choice(user_agents)
    
    def start_browser(self):
        """Start browser with stealth settings."""
        time.sleep(random.uniform(0.5, 1.5))
        
        if USE_UNDETECTED:
            self._start_undetected_browser()
        else:
            self._start_standard_browser()
        
        self.driver.set_page_load_timeout(60)
        self.driver.implicitly_wait(0)
        self._navigate_to_form()
    
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
                # Click tab to go back to form
                self.driver.execute_script("""
                    var tab = document.querySelector('a[href="#tab-02"]');
                    if (tab) tab.click();
                """)
                time.sleep(0.5)
                # Verify form is visible
                nombre_visible = self.driver.execute_script(
                    "var el = document.getElementById('nombre'); return el && el.offsetParent !== null;"
                )
                if not nombre_visible:
                    self._navigate_to_form()
        except:
            self._navigate_to_form()
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> str:
        """Search for CURP using Selenium (reliable method)."""
        if not self.driver:
            raise RuntimeError("Browser not started. Call start_browser() first.")
        
        max_retries = 2
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
                
                # Fill form using Selenium (reliable)
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
                
                self._random_delay()
                
                if self.search_count % self.pause_every_n == 0:
                    pause_time = self.pause_duration + random.uniform(-2, 3)
                    print(f"Pausing for {pause_time:.0f} seconds after {self.search_count} searches...")
                    time.sleep(pause_time)
                
                return content
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error during search (attempt {attempt + 1}/{max_retries}): {e}")
                    self._form_ready = False
                    time.sleep(1)
                else:
                    print(f"Error during search (final attempt): {e}")
                    self._form_ready = False
                    return ""
    
    def __enter__(self):
        self.start_browser()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()
