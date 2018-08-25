"""
Settings for the IPEDS Data File Downloader
"""
import os
import datetime as dt

IPEDS_BASE_URL = 'http://nces.ed.gov/ipeds/datacenter/data/' #e.g., HD2015.zip'
# IPEDS_BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/data/' #e.g., HD2015.zip' TIMEOUT with HTTPS...

MASTER_FILES_DOWNLOADED = []
DOWNLOAD_DATE = dt.datetime.now().strftime('%Y-%m-%d')
DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'Data') #, DOWNLOAD_DATE)

# List of survey groups, and the file_codes that are in each survey
# File codes map to lambdas to formulate file names for download
# comment lines out to exclude survey groups
SURVEY_FILES = {
    'AcademicLibraries' :           {'AL' : lambda year: 'AL%s.zip' % str(year),},
    'AdmissionsTestScores' :        {'ADM' : lambda year: 'ADM%s.zip' % str(year),},
    'Completions' :                 {'CA': lambda year: 'C%s_A.zip' % str(year),       # by race, gender, level, and cip
                                     'CB': lambda year: 'C%s_B.zip' % str(year),      # by race and gender
                                     'CC': lambda year: 'C%s_C.zip' % str(year),        # by race gender, level, age category
                                     'CDEP': lambda year: 'C%sDEP.zip' % str(year),   # distance ed offering by level
                                     },
    'EmployeesByAssignedPosition' : {'EAP': lambda year: 'EAP%s.zip' % str(year),},
    'FallEnrollment' :              {'EFA':      lambda year: 'EF%sA.zip' % str(year),
                                     'EFADIST':  lambda year: 'EF%sA_DIST.zip' % str(year),  #DISTANCE EDUCATION STATUS BY LEVEL
                                     'EFB':      lambda year: 'EF%sB.zip' % str(year),       #AGE CATEGORY
                                     'EFC':      lambda year: 'EF%sC.zip' % str(year),       #RESIDENCE AND MIGRATION
                                     'EFCP':     lambda year: 'EF%sCP.zip' % str(year),      #BI-ANNUAL: MAJOR BY CIP FIELDS
                                     'EFD':      lambda year: 'EF%sD.zip' % str(year),       #ENTERING/RETENTION/STUDENT:FACULTY RATIO
                                    },
    'Finance' : {'F1_GASB': lambda year: 'F%s%s_F1A.zip' % (str(year-1)[-2:], str(year)[-2:]),
                 'F2_FASB': lambda year: 'F%s%s_F2.zip' % (str(year-1)[-2:], str(year)[-2:]),
                 'F3_PFP':  lambda year: 'F%s%s_F3.zip' % (str(year-1)[-2:], str(year)[-2:]),},
    'GradRates' : {'GR150': lambda year: 'GR%s.zip' % str(year),
                   'GR200': lambda year: 'GR200_%s.zip' % str(year)[-2:],
                   'GRL2':  lambda year: 'GR%s_L2.zip' % str(year),},
    'InstitutionCharacteristics' : {#'HD': lambda year: 'HD%s.zip' % str(year),
                                    #'IC': lambda year: 'IC%s.zip' % str(year),
                                    'IC_AY': lambda year: 'IC%s_AY.zip' % str(year),
                                    #'IC_PY': lambda year: 'IC%s_PY.zip' % str(year),
                                   },
    'StudentFinAid' : {'SFA': lambda year: 'SFA%s%s.zip' % (str(year-1)[-2:], str(year)[-2:]),},
    'UnduplicatedEnrollment' : {'EFFY': lambda year: 'EFFY%s.zip' % str(year),
                                'EFIA': lambda year: 'EFIA%s.zip' % str(year),},
    'InstructionalStaffSalaries' : {'SAL_IS': lambda year: 'SAL%s_IS.zip' % str(year),
                                    'SAL_NIS': lambda year: 'SAL%s_NIS.zip' % str(year),}
}
# EXAMPLE CALL: SURVEY_FILES['InstitutionCharacteristics']['HD'](2014)
