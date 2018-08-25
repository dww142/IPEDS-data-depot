import os
import pandas as pd
import settings
import xlrd as xl
import xlsxwriter

# working_surveys = ['FallEnrollment']
# surveys = {k : settings.SURVEY_FILES[k] for k in working_surveys}
surveys = settings.SURVEY_FILES

def read_varlist_descriptions(wb):
    '''
    '''
    # read varlist and descriptions in frames (sheet name case sensitive & inconsistent)
    varlist_sheet = 'varlist' if 'varlist' in wb.sheet_names() else 'Varlist'
    dict_varlist = pd.read_excel(wb, engine='xlrd', sheet_name=varlist_sheet)
    # handle occurances where dictionary does not have a long description worksheet
    if 'Description' in wb.sheet_names() or 'description' in wb.sheet_names():
        dict_description = pd.read_excel(wb, engine='xlrd', sheet_name='Description')
        desc_cols = dict_description.columns.difference(dict_varlist)
        # merge the varlist with the long descriptions
        dict_meta = pd.merge(dict_varlist, dict_description[desc_cols.tolist()], how='left', on=['varnumber','varname'])
    else:
        dict_meta = dict_varlist
    # add a column for the source file (dictionary file name)
    dict_meta.insert(loc=0, column='SurveyDictionaryFile', value=pd.Series(dictionary, dict_meta.index))
    dict_meta.insert(loc=0, column='SurveyFileCode', value=pd.Series(file_code, dict_meta.index))
    dict_meta.insert(loc=0, column='Survey', value=pd.Series(survey, dict_meta.index))

    return dict_meta
    
def read_lookup_df(wb):
    '''
    input: wb = xlrd workbook of the single year dictionary file
    read the lookup variables from the IPEDS dictionary file and 
    return a pandas dataframe
    '''
    # read variable lookups
    # if revised lookups availabe - use; else use Frequencies sheet (sometimes name 'Statistics' instead of frequences)
    if 'Frequencies' in wb.sheet_names():
        lookup_sheet = ('FrequenciesRV' if 'FrequenciesRV' in wb.sheet_names() else 'Frequencies')
    else:
        lookup_sheet = None
    # elif 'Statistics' in wb.sheet_names():
    #     lookup_sheet = ('StatisticsRV' if 'StatisticsRV' in wb.sheet_names() else 'Statistics')
    if lookup_sheet:
        dict_lookups = pd.read_excel(wb, engine='xlrd', sheet_name=lookup_sheet)
        dict_lookups.insert(loc=0, column='SOURCE', value=pd.Series('IPEDS_'+file_code, dict_lookups.index))
    else:
        dict_lookups = pd.DataFrame()
    return dict_lookups

file_code_dictionaries = []
file_code_lookupvalues = []
for survey in surveys:
    for file_code in settings.SURVEY_FILES[survey]:
        dict_directory = os.path.join(settings.DATA_DIRECTORY, survey, file_code, 'Dictionaries')
        for dictionary in os.listdir(dict_directory):
            if dictionary.endswith('.xls') or dictionary.endswith('.xlsx'):
                print(survey, file_code, dictionary)
                wb = xl.open_workbook(os.path.join(dict_directory, dictionary))
                # read dictionary lookups from frequencies or statistics tab:
                dict_lookups = read_lookup_df(wb)
                file_code_lookupvalues.append(dict_lookups)
                
                #read varlist and append to the list of meta dictionaries (one for each survey/file/year; concatenate later)
                dict_meta = read_varlist_descriptions(wb)
                file_code_dictionaries.append(dict_meta)
        

file_code_master_dictionary = pd.concat(file_code_dictionaries)
#re-order columns
print(file_code_master_dictionary.columns)
# file_code_master_dictionary.columns = ['Survey', 'SurveyDictionaryFile', 'SurveyFileCode' 'varnumber', 'varname', 'varTitle', 'DataType', 'Fieldwidth', 'format', 'imputationvar' ,'longDescription', 'index']
file_code_consolidated_lookups = pd.concat(file_code_lookupvalues)
file_code_consolidated_lookups.drop_duplicates(subset=['SOURCE', 'varnumber', 'varname', 'codevalue', 'valuelabel'], keep='last', inplace=True)
# set output dictionary file path
xl_writer = pd.ExcelWriter(os.path.join(settings.DATA_DIRECTORY, '..\\_Metadata', 'IPEDS_ConsolidatedDictionary.xls'))
file_code_master_dictionary.to_excel(xl_writer, sheet_name='VarDescriptions', index=False)
file_code_consolidated_lookups.to_excel(xl_writer, sheet_name='VarLookups', index=False)
xl_writer.save()
