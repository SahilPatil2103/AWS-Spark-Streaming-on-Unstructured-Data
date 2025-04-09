import re
from datetime import datetime
from pyspark.sql.types import ArrayType, StringType

def split_job_postings(file_content):
    """Splits multi-job file into individual postings"""
    try:
        return re.split(r'\n(?=Job Posting: job\d+\.txt)', file_content)[1:]
    except Exception as e:
        raise ValueError(f"Error splitting jobs: {str(e)}")

def extract_file_name(file_content):
    """Gets main category from first line"""
    return file_content.split('\n')[0].strip()

def extract_position(job_text):
    """Extracts all positions from job text"""
    try:
        matches = re.findall(r'Position:\s*(.+?)\n', job_text)
        return ', '.join(matches) if matches else None
    except Exception as e:
        raise ValueError(f"Position error: {str(e)}")

def extract_classcode(job_text):
    """Extracts class codes with flexible formatting"""
    try:
        match = re.search(r'classcode:\s*([^\s]+)', job_text, re.IGNORECASE)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"classcode error: {str(e)}")

def extract_salary_start(job_text):
    """Handles various salary formats"""
    try:
        match = re.search(r'Salary starts at[^\d]*([\d,]+)', job_text)
        return float(match.group(1).replace(',', '')) if match else None
    except Exception as e:
        raise ValueError(f"Salary start error: {str(e)}")

def extract_salary_end(job_text):
    """Handles different end salary formats"""
    try:
        match = re.search(r'ends at[^\d]*([\d,]+)', job_text)
        return float(match.group(1).replace(',', '')) if match else None
    except Exception as e:
        raise ValueError(f"Salary end error: {str(e)}")

def extract_start_date(job_text):
    """Flexible date parsing"""
    try:
        match = re.search(r'from\s*(\d{4}-\d{1,2}-\d{1,2})', job_text)
        return datetime.strptime(match.group(1), '%Y-%m-%d') if match else None
    except Exception as e:
        raise ValueError(f"Start date error: {str(e)}")

def extract_end_date(job_text):
    """End date with validation"""
    try:
        match = re.search(r'until\s*(\d{4}-\d{1,2}-\d{1,2})', job_text)
        return datetime.strptime(match.group(1), '%Y-%m-%d') if match else None
    except Exception as e:
        raise ValueError(f"End date error: {str(e)}")

# Updated regex patterns for other fields
def extract_req(job_text):
    try:
        match = re.search(r'Required skills:\s*(.+?)(\.|\n|$)', job_text, re.DOTALL)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Req error: {str(e)}")

def extract_notes(job_text):
    try:
        match = re.search(r'Additional notes:\s*(.+?)(\.|\n|$)', job_text, re.DOTALL)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Notes error: {str(e)}")

def extract_duties(job_text):
    try:
        match = re.search(r'Primary duties:\s*(.+?)(\.|\n|$)', job_text, re.DOTALL)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Duties error: {str(e)}")

def extract_selection(job_text):
    try:
        match = re.search(r'Selection process:\s*(.+?)(\.|\n|$)', job_text, re.DOTALL)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Selection error: {str(e)}")

def extract_experience_length(job_text):
    try:
        match = re.search(r'Experience required:\s*(\d+)[+]?\s*years', job_text)
        return int(match.group(1)) if match else None
    except Exception as e:
        raise ValueError(f"Exp length error: {str(e)}")

def extract_job_type(job_text):
    try:
        match = re.search(r'Job type:\s*(.+?)(\.|\n|$)', job_text)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Job type error: {str(e)}")

def extract_education_length(job_text):
    try:
        match = re.search(r'Educational requirement:\s*(\d+)\s*years', job_text)
        return int(match.group(1)) if match else None
    except Exception as e:
        raise ValueError(f"Edu length error: {str(e)}")

def extract_school_type(job_text):
    try:
        match = re.search(r'Preferred school type:\s*(.+?)(\.|\n|$)', job_text)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"School type error: {str(e)}")

def extract_application_location(job_text):
    try:
        match = re.search(r'Application location:\s*(.+?)(\.|\n|$)', job_text)
        return match.group(1).strip() if match else None
    except Exception as e:
        raise ValueError(f"Location error: {str(e)}")
