"""
Result Validator
Extracts and validates CURP results from the website response.
"""
import re
from typing import Optional, Dict
from datetime import datetime


# CURP format: 18 characters (letters and numbers)
CURP_REGEX = re.compile(r'^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$')


class ResultValidator:
    """Validate and extract CURP information."""
    
    @staticmethod
    def is_valid_curp(curp: str) -> bool:
        """
        Validate CURP format (18 characters, standard pattern).
        
        Args:
            curp: CURP string to validate
            
        Returns:
            True if valid format, False otherwise
        """
        if not curp or not isinstance(curp, str):
            return False
        
        curp_clean = curp.strip().upper()
        
        # Check length
        if len(curp_clean) != 18:
            return False
        
        # Check regex pattern
        return bool(CURP_REGEX.match(curp_clean))
    
    @staticmethod
    def extract_curp_from_text(text: str) -> Optional[str]:
        """
        Extract CURP from text content (web page text).
        
        Args:
            text: Text content from web page
            
        Returns:
            CURP string if found, None otherwise
        """
        if not text:
            return None
        
        # Look for CURP pattern in text
        matches = CURP_REGEX.findall(text.upper())
        
        if matches:
            return matches[0]
        
        return None
    
    @staticmethod
    def extract_date_from_curp(curp: str) -> Optional[str]:
        """
        Extract birth date from CURP (positions 5-10: YYMMDD).
        
        Args:
            curp: Valid CURP string
            
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        if not ResultValidator.is_valid_curp(curp):
            return None
        
        curp_clean = curp.strip().upper()
        
        # Extract date portion (characters 5-10: YYMMDD)
        year_2digit = curp_clean[4:6]
        month = curp_clean[6:8]
        day = curp_clean[8:10]
        
        # Determine full year (assuming 1900-2099 range)
        year = int(year_2digit)
        if year >= 0 and year <= 30:  # 2000-2030
            full_year = 2000 + year
        else:  # 1900-1999
            full_year = 1900 + year
        
        try:
            # Validate date
            date_obj = datetime(int(full_year), int(month), int(day))
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return None
    
    @staticmethod
    def extract_state_code_from_curp(curp: str) -> Optional[str]:
        """
        Extract state code from CURP (characters 12-13).
        
        Args:
            curp: Valid CURP string
            
        Returns:
            State code (2 characters) or None
        """
        if not ResultValidator.is_valid_curp(curp):
            return None
        
        curp_clean = curp.strip().upper()
        
        # State code is at positions 12-13 (0-indexed: 11-12)
        if len(curp_clean) >= 13:
            return curp_clean[11:13]
        
        return None
    
    @staticmethod
    def validate_result(html_content: str, expected_state: str = None) -> Dict:
        """
        Validate search result and extract CURP information.
        
        Args:
            html_content: HTML content from the search result page
            expected_state: Expected state name (for validation)
            
        Returns:
            Dictionary with validation result:
            {
                'valid': bool,
                'curp': str or None,
                'birth_date': str or None,
                'state_code': str or None,
                'found': bool
            }
        """
        result = {
            'valid': False,
            'curp': None,
            'birth_date': None,
            'state_code': None,
            'found': False
        }
        
        if not html_content:
            return result
        
        html_lower = html_content.lower()
        
        # Check for error modal (no match found)
        # The modal contains "Los datos ingresados no son correctos"
        no_match_indicators = [
            'los datos ingresados no son correctos',
            'aviso importante',
            'warningmenssage',
            'estimado/a usuario/a'
        ]
        
        if any(indicator in html_lower for indicator in no_match_indicators):
            result['found'] = False
            return result
        
        # Check for results table (match found)
        # Look for the CURP in the results table structure
        # The CURP appears in a <td> after "CURP:" label
        import re
        
        # Pattern to find CURP in the table: <td>CURP:</td> followed by <td>ACTUAL_CURP</td>
        curp_pattern = r'<td[^>]*>CURP:</td>\s*<td[^>]*style="text-transform:\s*uppercase;">([A-Z0-9]{18})</td>'
        curp_match = re.search(curp_pattern, html_content, re.IGNORECASE)
        
        if curp_match:
            curp = curp_match.group(1).strip().upper()
            if ResultValidator.is_valid_curp(curp):
                result['found'] = True
                result['valid'] = True
                result['curp'] = curp
                result['birth_date'] = ResultValidator.extract_date_from_curp(curp)
                result['state_code'] = ResultValidator.extract_state_code_from_curp(curp)
                
                # Also try to extract birth date from the table if available
                # Pattern: <td>Fecha de nacimiento:</td> followed by <td>DD/MM/YYYY</td>
                date_pattern = r'<td[^>]*>Fecha de nacimiento:[^<]*</td>\s*<td[^>]*style="text-transform:\s*uppercase;">(\d{2}/\d{2}/\d{4})</td>'
                date_match = re.search(date_pattern, html_content, re.IGNORECASE)
                if date_match:
                    date_str = date_match.group(1)
                    # Convert DD/MM/YYYY to YYYY-MM-DD
                    try:
                        from datetime import datetime
                        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                        result['birth_date'] = date_obj.strftime('%Y-%m-%d')
                    except:
                        pass
                
                # Extract state from table if available
                state_pattern = r'<td[^>]*>Entidad de nacimiento:</td>\s*<td[^>]*style="text-transform:\s*uppercase;">([^<]+)</td>'
                state_match = re.search(state_pattern, html_content, re.IGNORECASE)
                if state_match:
                    state_name = state_match.group(1).strip()
                    # Store the state name found in results
                    result['state_name'] = state_name
        
        # Fallback: try to extract CURP from text if not found in table
        if not result['found']:
            curp = ResultValidator.extract_curp_from_text(html_content)
            if curp and ResultValidator.is_valid_curp(curp):
                result['found'] = True
                result['valid'] = True
                result['curp'] = curp
                result['birth_date'] = ResultValidator.extract_date_from_curp(curp)
                result['state_code'] = ResultValidator.extract_state_code_from_curp(curp)
        
        return result

