"""
Settings for the IPEDS Data File Downloader
"""
import os
import datetime as dt
import sqlalchemy as sa

SERVER_NAME = 'LOCALHOST'
DATABASE_NAME = 'OSDS_ETL'
TARGET_SCHEMA = 'IPEDS.'
USERNAME = 'sql_alchemy'
PASSWORD = 'sql_alchemy'

# local sql server
ENGINE = sa.create_engine(f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER_NAME}/{DATABASE_NAME}?driver=SQL+Server+Native+Client+11.0')
# local postgresql
# TARGET_SCHEMA = ''
# ENGINE = sa.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{SERVER_NAME}/{DATABASE_NAME}')

# KEEP THIS AS APPEND - data is written to table one year at a time
# replace will drop and recreate an existing table and leave with only the last years data
# append will delete data for each year before writing, but use existing table (updating/appending each year)
APPEND_OR_REPLACE = 'append'

# trying go figure out the best encoding to read/write data for importing to SQL Server
# if there are issues reading a CSV file after it's been downloaded, it could be this;
# also tried #'utf-8' on read; ISO seems to work better
READ_ENCODING = 'ISO-8859-1' 
WRITE_ENCODING = 'latin-1'


IPEDS_BASE_URL = 'http://nces.ed.gov/ipeds/datacenter/data/' #e.g., /HD2015.zip'
# IPEDS_BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/data/' #e.g., HD2015.zip' times out frequently with HTTPS...

MASTER_FILES_DOWNLOADED = []
DOWNLOAD_DATE = dt.datetime.now().strftime('%Y-%m-%d')
# saves downloaded CSV and dictionary files to subfolders by survey within the data directory
DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'Data') #, DOWNLOAD_DATE)

# will download dictionaries and store in a dictionaries folder for each file_code
DOWNLOAD_DICTIONARIES = True

# List of survey groups, and the file_codes that are in each survey
# File codes map to lambdas to formulate file names for download
# comment lines out to exclude survey groups
SURVEY_FILES = {
    'AcademicLibraries' :           {'AL' :     lambda year: 'AL%s.zip' % str(year),},
    'AdmissionsTestScores' :        {'ADM' :    lambda year: 'ADM%s.zip' % str(year),},
    'Completions' :                 {'CA':      lambda year: 'C%s_A.zip' % str(year),   # by race, gender, level, and cip
                                     'CB':      lambda year: 'C%s_B.zip' % str(year),   # by race and gender
                                     'CC':      lambda year: 'C%s_C.zip' % str(year),   # by race gender, level, age category
                                     'CDEP':    lambda year: 'C%sDEP.zip' % str(year),  # distance ed offering by level
                                     },
    'EmployeesByAssignedPosition' : {'EAP':      lambda year: 'EAP%s.zip' % str(year),},
    'FallEnrollment' :              {'EFA':      lambda year: 'EF%sA.zip' % str(year),       # race, gender, level, ft/pt status, ft/ft status
                                     'EFADIST':  lambda year: 'EF%sA_DIST.zip' % str(year),  # DISTANCE EDUCATION STATUS BY LEVEL
                                     'EFB':      lambda year: 'EF%sB.zip' % str(year),       # AGE CATEGORY
                                     'EFC':      lambda year: 'EF%sC.zip' % str(year),       # RESIDENCE AND MIGRATION
                                     'EFCP':     lambda year: 'EF%sCP.zip' % str(year),      # BI-ANNUAL: MAJOR BY CIP FIELDS
                                     'EFD':      lambda year: 'EF%sD.zip' % str(year),       # ENTERING/RETENTION/STUDENT:FACULTY RATIO
                                    },
    'Finance' : {'F1_GASB': lambda year: 'F%s%s_F1A.zip' % (str(year-1)[-2:], str(year)[-2:]),
                 'F2_FASB': lambda year: 'F%s%s_F2.zip' % (str(year-1)[-2:], str(year)[-2:]),
                 'F3_PFP':  lambda year: 'F%s%s_F3.zip' % (str(year-1)[-2:], str(year)[-2:]),},
    'GradRates' : {'GR150': lambda year: 'GR%s.zip' % str(year),
                   'GR200': lambda year: 'GR200_%s.zip' % str(year)[-2:],
                   'GRL2':  lambda year: 'GR%s_L2.zip' % str(year),
                   'GR_PELL_SSL' : lambda year: 'GR%s_PELL_SSL.zip' % str(year),},
    'InstitutionCharacteristics' : {'HD':       lambda year: 'HD%s.zip' % str(year),
                                    'IC':       lambda year: 'IC%s.zip' % str(year),
                                    'IC_AY':    lambda year: 'IC%s_AY.zip' % str(year),
                                    'IC_PY':    lambda year: 'IC%s_PY.zip' % str(year),
                                   },
    'StudentFinAid' : {'SFA': lambda year: 'SFA%s%s.zip' % (str(year-1)[-2:], str(year)[-2:]),},
    'UnduplicatedEnrollment' : {'EFFY': lambda year: 'EFFY%s.zip' % str(year),
                                'EFIA': lambda year: 'EFIA%s.zip' % str(year),},
    'InstructionalStaffSalaries' : {'SAL_IS':  lambda year: 'SAL%s_IS.zip' % str(year),
                                    'SAL_NIS': lambda year: 'SAL%s_NIS.zip' % str(year),},
    'OutcomeMeasures' : {'OM' : lambda year: 'OM%s.zip' % str(year)}
}
# EXAMPLE CALL: SURVEY_FILES['InstitutionCharacteristics']['HD'](2014)

DOWNLOAD_SURVEY_LIST = []



###### start_year = 2003 which is the 2002-2003 Academic Year for these file groups:
# START_YEAR = 2003
# DOWNLOAD_SURVEY_LIST.append('UnduplicatedEnrollment')
# DOWNLOAD_SURVEY_LIST.append('Finance')
# DOWNLOAD_SURVEY_LIST.append('GradRates')
# DOWNLOAD_SURVEY_LIST.append('StudentFinAid')
# DOWNLOAD_SURVEY_LIST.append('Completions')

###### start_year = 2002 which is the 2002-2003 Academic Year for these 2 file groups:
DOWNLOAD_SURVEY_LIST.append('InstitutionCharacteristics')
# DOWNLOAD_SURVEY_LIST.append('FallEnrollment')
# DOWNLOAD_SURVEY_LIST.append('AdmissionsTestScores')
# DOWNLOAD_SURVEY_LIST.append('AcademicLibraries')
# DOWNLOAD_SURVEY_LIST.append('EmployeesByAssignedPosition')
# DOWNLOAD_SURVEY_LIST.append('InstructionalStaffSalaries')

DOWNLOAD_SURVEY_FILE_LIST = {k : SURVEY_FILES[k] for k in DOWNLOAD_SURVEY_LIST}
GET_DICTIONARIES = True

START_YEAR = 2002
END_YEAR = 2018
SURVEY_YEAR_RANGE = range(START_YEAR, END_YEAR + 1)
WRITE_TO_TABLE = True

# print(DOWNLOAD_SURVEY_FILE_LIST)