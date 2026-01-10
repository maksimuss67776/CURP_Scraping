"""
Combination Generator
Generates all combinations of dates, months, states, and years for CURP search.
"""
from typing import Iterator, Tuple, List, Optional
from itertools import product


# Complete list of 33 Mexican states/options
MEXICAN_STATES = [
    "Aguascalientes",
    "Baja California",
    "Baja California Sur",
    "Campeche",
    "Chiapas",
    "Chihuahua",
    "Coahuila",
    "Colima",
    "Durango",
    "Guanajuato",
    "Guerrero",
    "Hidalgo",
    "Jalisco",
    "Michoacán",
    "Morelos",
    "Nayarit",
    "Nuevo León",
    "Oaxaca",
    "Puebla",
    "Querétaro",
    "Quintana Roo",
    "San Luis Potosí",
    "Sinaloa",
    "Sonora",
    "Tabasco",
    "Tamaulipas",
    "Tlaxcala",
    "Veracruz",
    "Yucatán",
    "Zacatecas",
    "Ciudad de México",
    "Nacido en el extranjero"
]


class CombinationGenerator:
    """Generate combinations for CURP search."""
    
    def __init__(self, start_year, end_year):
        """
        Initialize combination generator.
        
        Args:
            start_year: Starting year/year-month (e.g., 1990 or "1990-11")
            end_year: Ending year/year-month (e.g., 2000 or "2000-12") (inclusive)
        """
        # Parse start and end dates
        self.start_year, self.start_month = self._parse_year_month(start_year)
        self.end_year, self.end_month = self._parse_year_month(end_year)
        self.states = MEXICAN_STATES
        
        # Generate year-month pairs
        self.year_month_pairs = self._generate_year_month_pairs()
        
        # Calculate total combinations
        days = 31
        states_count = len(self.states)
        year_month_count = len(self.year_month_pairs)
        
        self.total_combinations = days * states_count * year_month_count
    
    def _parse_year_month(self, value):
        """
        Parse year or year-month string.
        
        Args:
            value: Either int/string year (1990) or year-month string ("1990-11")
            
        Returns:
            Tuple of (year, month). Month is None if not specified.
        """
        value_str = str(value)
        if '-' in value_str:
            parts = value_str.split('-')
            return int(parts[0]), int(parts[1])
        else:
            return int(value_str), None
    
    def _generate_year_month_pairs(self):
        """
        Generate list of (year, month) tuples based on start and end.
        
        Returns:
            List of (year, month) tuples
        """
        pairs = []
        
        # If both have specific months
        if self.start_month is not None and self.end_month is not None:
            # Generate all months from start to end
            for year in range(self.start_year, self.end_year + 1):
                if year == self.start_year:
                    # First year: from start_month to 12
                    months = range(self.start_month, 13)
                elif year == self.end_year:
                    # Last year: from 1 to end_month
                    months = range(1, self.end_month + 1)
                else:
                    # Middle years: all 12 months
                    months = range(1, 13)
                
                for month in months:
                    pairs.append((year, month))
        
        # If only start has month
        elif self.start_month is not None:
            for year in range(self.start_year, self.end_year + 1):
                if year == self.start_year:
                    months = range(self.start_month, 13)
                else:
                    months = range(1, 13)
                for month in months:
                    pairs.append((year, month))
        
        # If only end has month
        elif self.end_month is not None:
            for year in range(self.start_year, self.end_year + 1):
                if year == self.end_year:
                    months = range(1, self.end_month + 1)
                else:
                    months = range(1, 13)
                for month in months:
                    pairs.append((year, month))
        
        # Neither has month - use all months for all years
        else:
            for year in range(self.start_year, self.end_year + 1):
                for month in range(1, 13):
                    pairs.append((year, month))
        
        return pairs
    
    def generate_combinations(self) -> Iterator[Tuple[int, int, str, int]]:
        """
        Generate all combinations of (day, month, state, year).
        
        Yields:
            Tuple of (day, month, state_name, year)
        """
        days = range(1, 32)  # 1-31
        
        # Use itertools.product for efficient combination generation
        for day, state, (year, month) in product(days, self.states, self.year_month_pairs):
            yield (day, month, state, year)
    
    def get_total_count(self) -> int:
        """Get total number of combinations."""
        return self.total_combinations
    
    def get_combination_by_index(self, index: int) -> Optional[Tuple[int, int, str, int]]:
        """
        Get a specific combination by index (for checkpoint resume).
        
        Args:
            index: Zero-based index of the combination
            
        Returns:
            Tuple of (day, month, state_name, year) or None if index out of range
        """
        if index < 0 or index >= self.total_combinations:
            return None
        
        days = list(range(1, 32))
        states_count = len(self.states)
        year_month_count = len(self.year_month_pairs)
        
        # Calculate indices
        day_idx = index // (states_count * year_month_count)
        remaining = index % (states_count * year_month_count)
        
        state_idx = remaining // year_month_count
        year_month_idx = remaining % year_month_count
        
        year, month = self.year_month_pairs[year_month_idx]
        
        return (days[day_idx], month, self.states[state_idx], year)
    
    def get_index_of_combination(self, day: int, month: int, state: str, year: int) -> Optional[int]:
        """
        Get the index of a specific combination.
        
        Args:
            day: Day of month (1-31)
            month: Month (1-12)
            state: State name
            year: Year
            
        Returns:
            Zero-based index or None if combination is invalid
        """
        if day < 1 or day > 31:
            return None
        if month < 1 or month > 12:
            return None
        if state not in self.states:
            return None
        
        # Check if year-month pair exists in our list
        try:
            year_month_idx = self.year_month_pairs.index((year, month))
        except ValueError:
            return None
        
        days = list(range(1, 32))
        states_count = len(self.states)
        year_month_count = len(self.year_month_pairs)
        
        day_idx = days.index(day)
        state_idx = self.states.index(state)
        
        index = (day_idx * states_count * year_month_count +
                state_idx * year_month_count +
                year_month_idx)
        
        return index

