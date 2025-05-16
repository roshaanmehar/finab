"""
data_validation.py - Data validation
----------------------------------
Functions for validating scraped data.
"""
from typing import Dict, Tuple, Any


def validate_data(data: Dict[str, Any]) -> bool:
    """
    Validate scraped data.
    
    Args:
        data: Dictionary of data to validate
        
    Returns:
        True if data is valid, False otherwise
    """
    # This is a placeholder for future implementation
    # Add validation logic as needed
    return True


def derive_sector_subsector(pcd: str) -> Tuple[str, str]:
    """
    Derive sector and subsector from a postcode.
    
    Args:
        pcd: Postcode
        
    Returns:
        Tuple of (sector, subsector)
    """
    if " " not in pcd:
        return pcd, pcd
    outward, inward = pcd.split(" ", 1)
    inward_digit = next((ch for ch in inward if ch.isdigit()), "")
    return outward, f"{outward} {inward_digit}" if inward_digit else outward
