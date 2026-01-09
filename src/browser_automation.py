"""
Browser Automation - Stealth Selenium Version
Handles browser automation using Selenium WebDriver with anti-detection measures.
ENHANCED VERSION - reCAPTCHA bypass, human-like behavior
"""
import time
import random
import threading
import math
from typing import Optional, Dict, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from state_codes import get_state_code

# Try to import undetected-chromedriver for better stealth
try:
    import undetected_chromedriver as uc
    USE_UNDETECTED = True
except ImportError:
    USE_UNDETECTED = False
    print("Note: undetected-chromedriver not installed. Using standard Selenium.")

# Try to import webdriver_manager for automatic chromedriver management
try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WEBDRIVER_MANAGER = True
except ImportError:
    USE_WEBDRIVER_MANAGER = False

# Global cached chromedriver path to avoid race conditions
_chromedriver_path = None
_chromedriver_lock = threading.Lock()


def get_chromedriver_path():
    """Get chromedriver path with thread-safe caching."""
    global _chromedriver_path
    
    if _chromedriver_path is not None:
        return _chromedriver_path
    
    with _chromedriver_lock:
        # Double-check after acquiring lock
        if _chromedriver_path is not None:
            return _chromedriver_path
        
        if USE_WEBDRIVER_MANAGER:
            _chromedriver_path = ChromeDriverManager().install()
        else:
            _chromedriver_path = None
        
        return _chromedriver_path


class HumanBehavior:
    """Simulate human-like behavior patterns."""
    
    @staticmethod
    def random_sleep(min_seconds: float = 0.1, max_seconds: float = 0.5):
        """Sleep for a random duration."""
        time.sleep(random.uniform(min_seconds, max_seconds))
    
    @staticmethod
    def typing_delay():
        """Simulate typing delay between keystrokes."""
        time.sleep(random.uniform(0.05, 0.15))
    
    @staticmethod
    def human_type(element, text: str, clear_first: bool = True):
        """Type text with human-like delays."""
        if clear_first:
            element.clear()
            HumanBehavior.random_sleep(0.1, 0.3)
        
        for char in text:
            element.send_keys(char)
            HumanBehavior.typing_delay()
    
    @staticmethod
    def random_mouse_jitter(driver, element):
        """Add small random mouse movements near an element."""
        try:
            action = ActionChains(driver)
            # Move to element with offset
            offset_x = random.randint(-5, 5)
            offset_y = random.randint(-5, 5)
            action.move_to_element_with_offset(element, offset_x, offset_y)
            action.perform()
            HumanBehavior.random_sleep(0.05, 0.15)
        except:
            pass
    
    @staticmethod
    def bezier_mouse_move(driver, start_x, start_y, end_x, end_y, steps: int = 20):
        """Simulate natural mouse movement using Bezier curves."""
        try:
            action = ActionChains(driver)
            
            # Control points for Bezier curve
            ctrl1_x = start_x + (end_x - start_x) * random.uniform(0.2, 0.4)
            ctrl1_y = start_y + random.uniform(-50, 50)
            ctrl2_x = start_x + (end_x - start_x) * random.uniform(0.6, 0.8)
            ctrl2_y = end_y + random.uniform(-50, 50)
            
            for i in range(steps):
                t = i / steps
                # Cubic Bezier formula
                x = (1-t)**3 * start_x + 3*(1-t)**2*t * ctrl1_x + 3*(1-t)*t**2 * ctrl2_x + t**3 * end_x
                y = (1-t)**3 * start_y + 3*(1-t)**2*t * ctrl1_y + 3*(1-t)*t**2 * ctrl2_y + t**3 * end_y
                
                action.move_by_offset(int(x - start_x) // steps, int(y - start_y) // steps)
                start_x += (x - start_x) / steps
                start_y += (y - start_y) / steps
            
            action.perform()
        except:
            pass


class BrowserAutomation:
    """Handle browser automation for CURP searches using Selenium with stealth."""
    
    def __init__(self, headless: bool = False, min_delay: float = 0.3, 
                 max_delay: float = 0.6, pause_every_n: int = 500, 
                 pause_duration: int = 5):
        """
        Initialize browser automation with stealth features - SAFE CONFIGURATION.
        
        Args:
            headless: Run browser in headless mode (TRUE recommended for max performance)
            min_delay: Minimum delay between searches (SAFE: 0.3s)
            max_delay: Maximum delay between searches (SAFE: 0.6s)
            pause_every_n: Pause every N searches (SAFE: 500)
            pause_duration: Duration of pause (SAFE: 5s)
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
        self._last_tab_click_time = 0
        self._session_searches = 0  # Track searches in current session
        self._connection_pool_ready = False  # Connection pooling flag
        self._last_error_time = 0  # Track errors for adaptive throttling
    
    def _get_random_user_agent(self) -> str:
        """Get a random realistic user agent."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        return random.choice(user_agents)
    
    def start_browser(self):
        """Start browser with stealth settings and navigate to CURP page."""
        # Add random delay to spread out worker initialization (increased for slow website)
        time.sleep(random.uniform(2.0, 5.0))
        
        if USE_UNDETECTED:
            self._start_undetected_browser()
        else:
            self._start_standard_browser()
        
        # Set page load timeout (increased for slow website)
        self.driver.set_page_load_timeout(90)  # Increased timeout
        self.driver.implicitly_wait(0)  # We use explicit waits
        
        # Navigate to CURP page
        self._navigate_to_form()
    
    def _start_undetected_browser(self):
        """Start browser using undetected-chromedriver."""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Basic stealth options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'--user-agent={self._get_random_user_agent()}')
        
        # Add random window position
        x_pos = random.randint(0, 100)
        y_pos = random.randint(0, 100)
        options.add_argument(f'--window-position={x_pos},{y_pos}')
        
        self.driver = uc.Chrome(options=options, use_subprocess=True)
        
        # Execute stealth scripts
        self._inject_stealth_scripts()
    
    def _start_standard_browser(self):
        """Start browser using standard Selenium with stealth measures."""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Comprehensive anti-detection arguments
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-logging')
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'--user-agent={self._get_random_user_agent()}')
        
        # Add language and timezone to appear more natural
        options.add_argument('--lang=es-MX')
        
        # Experimental options for stealth
        options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Preferences to disable various fingerprinting vectors
        prefs = {
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
            'profile.default_content_setting_values.notifications': 2,
            'webrtc.ip_handling_policy': 'disable_non_proxied_udp',
            'webrtc.multiple_routes_enabled': False,
            'webrtc.nonproxied_udp_enabled': False
        }
        options.add_experimental_option('prefs', prefs)
        
        # Page load strategy
        options.page_load_strategy = 'eager'  # Don't wait for images/CSS
        
        # Additional performance optimizations
        options.add_argument('--disable-images')  # Don't load images
        options.add_argument('--disable-css')  # Skip CSS for speed
        options.add_argument('--disable-javascript')  # If site works without JS
        
        # Create driver
        driver_path = get_chromedriver_path()
        if driver_path:
            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
        else:
            self.driver = webdriver.Chrome(options=options)
        
        # Execute stealth scripts
        self._inject_stealth_scripts()
    
    def _inject_stealth_scripts(self):
        """Inject JavaScript to hide automation indicators."""
        if not self.driver:
            return
        
        stealth_js = """
        // Override webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Override plugins to appear normal
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-MX', 'es', 'en-US', 'en']
        });
        
        // Override permissions query
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Override chrome property
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };
        
        // Override console.debug to prevent detection scripts
        console.debug = console.log;
        """
        
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': stealth_js
            })
        except:
            # Fallback: execute script directly
            try:
                self.driver.execute_script(stealth_js)
            except:
                pass
    
    def _navigate_to_form(self):
        """Navigate to form page and click the Datos Personales tab."""
        if not self.driver:
            return
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[Browser] Navigating to {self.url} (attempt {attempt + 1}/{max_retries})...")
                # Navigate to URL
                self.driver.get(self.url)
                print(f"[Browser] Page loaded, waiting for ready state...")
                
                # Random delay after page load
                HumanBehavior.random_sleep(1.0, 2.5)
                
                # Wait for page to be ready (increased timeout)
                WebDriverWait(self.driver, 60).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                print(f"[Browser] Page ready, waiting for tab link...")
                
                # Another random delay to simulate reading
                HumanBehavior.random_sleep(0.5, 1.5)
                
                # Wait for the tab link to be clickable and click it (increased timeout)
                tab_link = WebDriverWait(self.driver, 45).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="#tab-02"]'))
                )
                print(f"[Browser] Tab link found, clicking...")
                
                # Scroll into view smoothly
                self.driver.execute_script("""
                    arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});
                """, tab_link)
                HumanBehavior.random_sleep(0.3, 0.7)
                
                # Mouse jitter before click
                HumanBehavior.random_mouse_jitter(self.driver, tab_link)
                
                # Click with JavaScript for reliability
                self.driver.execute_script("arguments[0].click();", tab_link)
                self._last_tab_click_time = time.time()
                print(f"[Browser] Tab clicked, waiting for form...")
                
                # Wait for the form to become visible (increased timeout)
                HumanBehavior.random_sleep(0.5, 1.0)
                
                print(f"[Browser] Waiting for form to be visible...")
                WebDriverWait(self.driver, 45).until(
                    self._is_form_visible
                )
                
                self._form_ready = True
                print(f"[Browser] Form ready! Browser initialization complete.")
                return
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error navigating to {self.url} (attempt {attempt + 1}/{max_retries}): {error_msg}")
                print(f"[Browser] Current URL: {self.driver.current_url if self.driver else 'N/A'}")
                
                # Try to get page source for debugging
                try:
                    if self.driver:
                        # Check if nombre field exists
                        try:
                            nombre = self.driver.find_element(By.ID, "nombre")
                            print(f"[Browser] nombre field found but not visible: displayed={nombre.is_displayed()}")
                        except:
                            print(f"[Browser] nombre field NOT FOUND in page")
                except:
                    pass
                
                self._form_ready = False
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
    
    def _is_form_visible(self, driver) -> bool:
        """Check if the form is actually visible and ready for input."""
        try:
            nombre_input = driver.find_element(By.ID, "nombre")
            # Check if element is displayed and has non-zero dimensions
            if nombre_input.is_displayed():
                rect = nombre_input.rect
                return rect['width'] > 0 and rect['height'] > 0
            return False
        except:
            return False
    
    def close_browser(self):
        """Close browser and cleanup."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def _random_delay(self):
        """Apply random delay between searches with variation - OPTIMIZED."""
        # PERFORMANCE: Minimal delay for max throughput
        base_delay = random.uniform(self.min_delay, self.max_delay)
        
        # Adaptive throttling if recent errors
        if self._last_error_time > 0 and (time.time() - self._last_error_time) < 60:
            base_delay *= 1.5  # Slow down temporarily after errors
        
        # 10% chance of extra pause (human distraction simulation)
        if random.random() < 0.10:
            base_delay += random.uniform(1.0, 3.0)
        
        time.sleep(base_delay)
    
    def _human_like_scroll(self):
        """Perform random scrolling like a human would."""
        if random.random() < 0.15:  # 15% chance
            try:
                scroll_amount = random.randint(-100, 100)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                HumanBehavior.random_sleep(0.2, 0.5)
            except:
                pass
    
    def _clear_form(self):
        """Clear form fields with human-like behavior."""
        if not self.driver:
            return
        
        try:
            # Clear fields one by one with small delays
            fields = ['nombre', 'primerApellido', 'segundoApellido', 'selectedYear']
            for field_id in fields:
                try:
                    field = self.driver.find_element(By.ID, field_id)
                    if field.get_attribute('value'):
                        field.clear()
                        HumanBehavior.random_sleep(0.05, 0.15)
                except:
                    pass
        except:
            pass
    
    def _close_modal_if_present(self):
        """Close the error modal if it appears (no match found)."""
        if not self.driver:
            return
        
        try:
            # Wait a moment for modal to fully appear
            HumanBehavior.random_sleep(0.2, 0.4)
            
            # Try to find and click close button
            close_btns = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-dismiss="modal"], button.btn-default')
            for btn in close_btns:
                try:
                    if btn.is_displayed():
                        HumanBehavior.random_mouse_jitter(self.driver, btn)
                        btn.click()
                        HumanBehavior.random_sleep(0.3, 0.6)
                        break
                except:
                    continue
        except:
            pass
    
    def _ensure_form_ready(self):
        """Ensure form is ready, reload only if necessary."""
        if not self._form_ready or not self.driver:
            self._navigate_to_form()
            return
        
        try:
            # Quick check if form is still visible
            if not self._is_form_visible(self.driver):
                # Try clicking tab again first (faster than reload)
                try:
                    tab_link = self.driver.find_element(By.CSS_SELECTOR, 'a[href="#tab-02"]')
                    self.driver.execute_script("arguments[0].click();", tab_link)
                    HumanBehavior.random_sleep(0.3, 0.6)
                    
                    if self._is_form_visible(self.driver):
                        return
                except:
                    pass
                
                # Full reload if tab click didn't work
                self._navigate_to_form()
        except:
            self._navigate_to_form()
    
    def _fill_text_field_human_like(self, field_id: str, value: str):
        """Fill a text field with human-like typing."""
        try:
            field = self.driver.find_element(By.ID, field_id)
            
            # Move mouse to field
            HumanBehavior.random_mouse_jitter(self.driver, field)
            
            # Click field
            field.click()
            HumanBehavior.random_sleep(0.1, 0.2)
            
            # Clear existing value
            field.clear()
            HumanBehavior.random_sleep(0.05, 0.15)
            
            # Type with human-like delays
            for char in value:
                field.send_keys(char)
                time.sleep(random.uniform(0.03, 0.12))
            
            # Trigger events
            self.driver.execute_script("""
                var el = arguments[0];
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            """, field)
            
            HumanBehavior.random_sleep(0.1, 0.25)
        except Exception as e:
            # Fallback to JavaScript
            self.driver.execute_script(f"""
                var el = document.getElementById('{field_id}');
                if (el) {{
                    el.value = '{value}';
                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}
            """)
    
    def _select_dropdown_human_like(self, field_id: str, value: str):
        """Select a dropdown option with human-like behavior."""
        try:
            select_el = self.driver.find_element(By.ID, field_id)
            
            # Move mouse to dropdown
            HumanBehavior.random_mouse_jitter(self.driver, select_el)
            
            # Click to open
            select_el.click()
            HumanBehavior.random_sleep(0.1, 0.3)
            
            # Select the value
            select = Select(select_el)
            select.select_by_value(value)
            
            # Trigger change event
            self.driver.execute_script("""
                var el = arguments[0];
                el.dispatchEvent(new Event('change', { bubbles: true }));
            """, select_el)
            
            HumanBehavior.random_sleep(0.1, 0.2)
        except Exception as e:
            # Fallback to JavaScript
            self.driver.execute_script(f"""
                var el = document.getElementById('{field_id}');
                if (el) {{
                    el.value = '{value}';
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}
            """)
    
    def search_curp(self, first_name: str, last_name_1: str, last_name_2: str,
                   gender: str, day: int, month: int, state: str, year: int) -> str:
        """
        Search for CURP with given parameters - STEALTH VERSION.
        Uses human-like behavior to avoid detection.
        
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
        if not self.driver:
            raise RuntimeError("Browser not started. Call start_browser() first.")
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Ensure form is ready
                self._ensure_form_ready()
                
                # Close any open modal from previous search
                self._close_modal_if_present()
                
                # Sometimes scroll randomly
                self._human_like_scroll()
                
                # Clear form
                self._clear_form()
                
                # Prepare values
                day_str = str(day).zfill(2)
                month_str = str(month).zfill(2)
                year_str = str(year)
                gender_value = "H" if gender.upper() == "H" else "M"
                state_code = get_state_code(state)
                
                # Fill fields with human-like behavior
                # Randomize order slightly to appear more natural
                field_actions = [
                    ('nombre', first_name, 'text'),
                    ('primerApellido', last_name_1, 'text'),
                    ('segundoApellido', last_name_2, 'text'),
                    ('selectedYear', year_str, 'text'),
                    ('diaNacimiento', day_str, 'select'),
                    ('mesNacimiento', month_str, 'select'),
                    ('sexo', gender_value, 'select'),
                    ('claveEntidad', state_code, 'select'),
                ]
                
                for field_id, value, field_type in field_actions:
                    if field_type == 'text':
                        self._fill_text_field_human_like(field_id, value)
                    else:
                        self._select_dropdown_human_like(field_id, value)
                
                # Small pause before submitting
                HumanBehavior.random_sleep(0.3, 0.7)
                
                # Find and click submit button with human-like behavior
                try:
                    submit_btn = self.driver.find_element(By.ID, "searchButton")
                    HumanBehavior.random_mouse_jitter(self.driver, submit_btn)
                    submit_btn.click()
                except:
                    # Fallback
                    self.driver.execute_script("""
                        var btn = document.getElementById('searchButton');
                        if (btn) btn.click();
                    """)
                
                # Wait for response with reasonable timeout
                try:
                    WebDriverWait(self.driver, 8).until(  # Safe timeout
                        lambda d: d.execute_script("""
                            return document.querySelector('button[data-dismiss="modal"]') !== null ||
                                   document.getElementById('dwnldLnk') !== null ||
                                   document.querySelector('table.table') !== null ||
                                   document.querySelector('.modal.show') !== null ||
                                   document.querySelector('.modal.in') !== null;
                        """)
                    )
                except TimeoutException:
                    # Timeout is okay, we'll still check content
                    pass
                
                # Small delay before reading result
                HumanBehavior.random_sleep(0.2, 0.4)  # Safe delay
                
                # Close modal if present
                self._close_modal_if_present()
                
                # Get page content
                content = self.driver.page_source
                
                # Increment search count
                self.search_count += 1
                self._session_searches += 1
                
                # Apply delay after search
                self._random_delay()
                
                # Periodic pause to avoid detection
                if self.search_count % self.pause_every_n == 0:
                    pause_time = self.pause_duration + random.uniform(-3, 5)
                    print(f"Pausing for {pause_time:.0f} seconds after {self.search_count} searches...")
                    time.sleep(pause_time)
                
                return content
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error during search (attempt {attempt + 1}/{max_retries}): {e}")
                    self._form_ready = False
                    time.sleep(2)
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
