import os
import datetime as dtype
import json
import pandas as pd
import xlrd as xl 
import settings




def read_varlist_descriptions(ef, survey, file_code, file):
    '''
    Read variable list and long descriptions from dictionary worksheets
    and combine into a single frame. (Field names and descriptions NOT lookup codes-descs)
    '''
    # read varlist (lookup codes) and descriptions in frames (sheet name case sensitive & inconsistent)
    varlist_sheet = 'varlist' if 'varlist' in ef.sheet_names else 'Varlist'
    dict_varlist = ef.parse(varlist_sheet, engine='xlrd')
    # handle occurances where dictionary does not have a long description worksheet
    if 'Description' in ef.sheet_names or 'description' in ef.sheet_names:
        dict_description = ef.parse('Description', engine='xlrd')
        desc_cols = dict_description.columns.difference(dict_varlist)
        # merge the varlist with the long descriptions
        dict_meta = pd.merge(dict_varlist, dict_description[desc_cols.tolist()], how='left', on=['varnumber','varname'])
    else:
        dict_meta = dict_varlist
    # add a column for the source file (dictionary file name)
    dict_meta.insert(loc=0, column='SurveyDictionaryFile', value=pd.Series(file, dict_meta.index))
    dict_meta.insert(loc=0, column='SurveyFileCode', value=pd.Series(file_code, dict_meta.index))
    dict_meta.insert(loc=0, column='Survey', value=pd.Series(survey, dict_meta.index))

    return dict_meta
    
def read_lookups(ef, survey, file_code, file):
    '''
    Read the lookup variables from the frequencies worksheet in the dictionary file and 
    return a pandas dataframe. (reads revised frequencies if available)
    '''
    # read variable lookups
    # if revised lookups availabe - use; else use Frequencies sheet (sometimes name 'Statistics' instead of frequences)
    if 'Frequencies' in ef.sheet_names:
        lookup_sheet = ('FrequenciesRV' if 'FrequenciesRV' in ef.sheet_names else 'Frequencies')
    else:
        lookup_sheet = None
    # elif 'Statistics' in wb.sheet_names():
    #     lookup_sheet = ('StatisticsRV' if 'StatisticsRV' in wb.sheet_names() else 'Statistics')
    if lookup_sheet:
        dict_lookups = ef.parse(lookup_sheet, engine='xlrd')
        dict_lookups.insert(loc=0, column='SOURCE', value=pd.Series('IPEDS_'+file_code, dict_lookups.index))
        dict_lookups['SourceFile'] = pd.Series(file, dict_lookups.index)
    else:
        dict_lookups = pd.DataFrame()
    return dict_lookups




def __main__():
    '''
    Set LOG DATE before run; 
    looks for logs for each file in list for that date; 
    loads to DB
    '''
    surveys = settings.DOWNLOAD_SURVEY_FILE_LIST
    # surveys = settings.SURVEY_FILES
    log_date = '2019-03-05'
    desc_frames = []
    lookup_frames = []
    for survey in surveys:
        print(survey, end='...')
        for file_code in surveys[survey]:
            dict_path = os.path.join(settings.DATA_DIRECTORY, survey, file_code, log_date, 'Dictionaries')
            if os.path.exists(dict_path):
                print(file_code, end='...')
                for file in os.listdir(dict_path):
                    if file.endswith('.xlsx') or file.endswith('.xls'):
                        print(file, end='...')
                        ef = pd.ExcelFile(os.path.join(dict_path, file))
                        lookups = read_lookups(ef, survey, file_code, file)
                        lookup_frames.append(lookups)
                        descriptions = read_varlist_descriptions(ef, survey, file_code, file)
                        desc_frames.append(descriptions)
            else:
                print(f'No dictioanries for {survey} {file_code} for {log_date}')
        print('done')
    consolidated_lookups = pd.concat(lookup_frames, sort=True)
    consolidated_descriptions = pd.concat(desc_frames, sort=True)
    xl_writer = pd.ExcelWriter(os.path.join(settings.DATA_DIRECTORY, '_Metadata', f'IPEDS_ConsolidatedDictionary_{log_date}.xlsx'))
    consolidated_lookups.to_excel(xl_writer, sheet_name='VarDescriptions', index=False)
    consolidated_descriptions.to_excel(xl_writer, sheet_name='VarLookups', index=False)
    xl_writer.save()


__main__()