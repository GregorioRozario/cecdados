import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)
import configparser # for reading settings file
import pandas as pd # For dealing with dataframes
from datetime import date
import datetime
from dateutil.relativedelta import relativedelta
import math

# Reads rozlib library path from config.ini file and import libraries packages
rozlib_path = ''
def add_rozlib_library_path():
    global rozlib_path
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini', encoding='utf-8')
    rozlib_path = config['GENERAL']['rozlibFolderPath']
    sys.path.insert(1, rozlib_path)
    
    fiislib_path = config['GENERAL']['fiislibFolderPath']
    sys.path.insert(1, fiislib_path)
add_rozlib_library_path()

from rozlib.util import Utilities
from rozlib.ai.ai import AI
from rozlib.ai.supervised_ml import SupervisedML
import fiislib

# Constants
verbosity = 1
end_date_for_trainning = '2021-03-31'
start_date_for_trainning = datetime.datetime.strptime(end_date_for_trainning, '%Y-%m-%d') - relativedelta(months=+12)
end_date_for_validation = date.today()
start_date_for_validation = end_date_for_validation - relativedelta(months=+13)

daily_quotation_column_names=['date',
                          'last',
                          'open',
                          'maximum',
                          'minimum',
                          'volume',
                          'variation']
columns_to_exclude_from_data_vector = ['date',
                                       #'last',
                                       #'open',
                                       #'maximum',
                                       #'minimum',
                                       'volume',
                                       'variation'
                                       ]

beta_classes = {
    'high':     {'label': 1, 'upper_range': math.inf, 'lower_range': 0.2},
    'average':  {'label': 2, 'upper_range': 0.2, 'lower_range': 0.1},
    'low':      {'label': 3, 'upper_range': 0.1, 'lower_range': 0.0},
}

dy_classes = {
    'high':     {'label': 1, 'upper_range': math.inf, 'lower_range': 1.0},
    'average':  {'label': 2, 'upper_range': 1.0, 'lower_range': 0.6},
    'low':      {'label': 3, 'upper_range': 0.6, 'lower_range': 0.0},
}
# end of constants

# Globals
#fiis_to_evaluate = ['MXRF11', 'KISU11', 'VILG11', 'JSRE11', 'ABCP11']
# Utilities object
utilities = Utilities(verbosity)
# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()
# Results from ML algorithms (used widely throughout the code)
beta_supervised_ml_results = None
dy_supervised_ml_results = None
# end of globals

def categorize_by_range(value, categories_dict):
    """Picks from a categories dictionary that category that fits the value within its boundaries (upper and lower range)

    Args:
        value (numeric): a number to be fitted within a category from the categories dictionary
        categories_dict (dict): a dictionary with categories to fit 'value'

    Returns:
        int: a number indicating the category that fits 'value' within its range
    """
    
    for current_key in categories_dict:
        if categories_dict[current_key]['lower_range'] <= value < categories_dict[current_key]['upper_range']:
            return categories_dict[current_key]['label']

def init():
    Utilities.clearConsole()
    # Retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

def get_fiis_quotations(valid_fiis_list=None, start_date=None, end_date=None):
    """Picks daily quotation for FIIs, given a start date, end date and a list of FIIs (all optional)

    Args:
        valid_fiis_list (list, optional): list of registered FIIs, to which daily quotations should be retrieved from database. Defaults to None.
        start_date (str or date, optional): Date when first daily quotation should be retrieved. If None is supplied, picks 01/01/1990 as start date. Defaults to None.
        end_date (str or date, optional): Date when last daily quotation should be retrieved. If None is supplied, picks current date as end date. Defaults to None.

    Returns:
        dict: a dictionary with "ticker":"daily quotations" entries
    """
    
    # Validating list of FIIs
    if valid_fiis_list == None:
        valid_fiis_list = fiis_definition.get_registered_fiis_list()
    
    # Dates verification
        if start_date == None:
            start_date = '1990-01-01'
        if end_date == None:
            end_date = datetime.today().strftime('%Y-%m-%d')
    
    fiis_quotations_dict = {}
    for current_ticker in [valid_fii[1] for valid_fii in valid_fiis_list]:
        current_df = AI.get_dataframe_from_list(fiis_definition.get_fii_daily_quotation_for_date_interval(current_ticker, start_date, end_date),
                                            daily_quotation_column_names)
        current_df.insert(1, "year_reference", current_df["date"].apply(lambda x: Utilities.get_timestamp_refenced_to_year(x)), True)
        fiis_quotations_dict[current_ticker] = {}
        fiis_quotations_dict[current_ticker]['quotation_data'] = current_df.loc[:, ~current_df.columns.isin(columns_to_exclude_from_data_vector)].to_numpy().flatten().tolist()
    
    return fiis_quotations_dict

def data_preparation(valid_fiis_list=None, start_date=None, end_date=None):
    """_summary_

    Args:
        valid_fiis_list (_type_, optional): _description_. Defaults to None.
        start_date (_type_, optional): _description_. Defaults to None.
        end_date (_type_, optional): _description_. Defaults to None.

    Returns:
        tuple: features vector (pandas dataframe), beta classes series (pandas series), DY classes series (pandas series)
    """
    
    # Picks quotation information for given FIIs
    fiis_quotations_dict = utilities.call_function_with_elapsed_time(get_fiis_quotations, valid_fiis_list, start_date, end_date)
    
    # Calculates beta and dividend yield to join these information to the FIIs quotations dictionary    
    beta_indices = fiis_definition.calculate_beta_for_time_interval(start_date, end_date, is_simetric_beta=True)
    dy_dict = fiis_definition.get_dy_for_interval(start_date=start_date, end_date=end_date)
    for current_ticker in fiis_quotations_dict:
        try:
            fiis_quotations_dict[current_ticker]['beta']=beta_indices[current_ticker]
            fiis_quotations_dict[current_ticker]['DY']=dy_dict[current_ticker]
        except KeyError:
            # beta filtering tickers (those tickers not in beta dictionary will not join machine learning evaluation)
            pass
    
    # Makes a dataframe from the FIIs quotations dictionary
    quotation_dataframe = pd.DataFrame.from_dict(fiis_quotations_dict).transpose()
    quotation_dataframe = quotation_dataframe.dropna()  # really necessary?
    
    # Categorizes beta and DY indices, according to predefined parameters (ranges)
    # This step is necessary to ease ML algorithms: they work properly with classes, not with floating point
    # TODO: permitir análises com dois tipos de categorias: por intervalos estipulados por mim e por percentis
    #quotation_dataframe['beta'] = quotation_dataframe['beta'].apply(lambda x: categorize_by_range(abs(x), beta_classes))
    #quotation_dataframe['DY'] = quotation_dataframe['DY'].apply(lambda x: categorize_by_range(abs(x), dy_classes))
    
    quotation_dataframe["beta"] = pd.qcut(quotation_dataframe["beta"], q=[0, .33, .67, 1], labels=[3, 2, 1])
    quotation_dataframe["DY"] = pd.qcut(quotation_dataframe["DY"], q=[0, .33, .67, 1], labels=[3, 2, 1])
        
    # Prepares X axis dataframe (features vector splitted into many columns)
    X = pd.DataFrame(quotation_dataframe['quotation_data'].to_list(), index=quotation_dataframe.index)
    
    # Beta Y axis series
    Y_beta=quotation_dataframe.iloc[:, (quotation_dataframe.columns == "beta")]
    
    # Dividend Yield Y axis series
    Y_dy=quotation_dataframe.iloc[:, (quotation_dataframe.columns == "DY")]
    
    return X, Y_beta, Y_dy

def train_algorithms(valid_fiis_list=None):
    """Train supervised machine learning algorithms

    Args:
        valid_fiis_list (list, optional): List of FIIs, to be used for trainning ML algorithms. Defaults to None.

    Returns:
        pandas.DataFrame: features vector used for trainning algorithms
    """
    
    X, Y_beta, Y_dy = data_preparation(valid_fiis_list=valid_fiis_list, start_date=start_date_for_trainning, end_date=end_date_for_trainning)
    
    # Trainning the algorithms
    # Beta analysis
    beta_supervised_ml = SupervisedML(X=X, Y=Y_beta)
    global beta_supervised_ml_results
    beta_supervised_ml_results = utilities.call_function_with_elapsed_time(beta_supervised_ml.evaluate_best_supervised_method)
    utilities.print_verbose('beta results', verbosity_level=1)
    utilities.print_verbose(Utilities.get_tabular_data(
            ['Approach', 'Accuracy'],
            ([key, f'{100 * beta_supervised_ml_results[key]["accuracy"]:.3f}%'] for key in beta_supervised_ml_results)),
            verbosity_level=1)
    
    # Dividend Yield analysis
    dy_supervised_ml = SupervisedML(X=X, Y=Y_dy)
    global dy_supervised_ml_results
    dy_supervised_ml_results = utilities.call_function_with_elapsed_time(dy_supervised_ml.evaluate_best_supervised_method)
    utilities.print_verbose('DY results', verbosity_level=1)
    utilities.print_verbose(Utilities.get_tabular_data(
            ['Approach', 'Accuracy'],
            ([key, f'{100 * dy_supervised_ml_results[key]["accuracy"]:.3f}%'] for key in dy_supervised_ml_results)),
            verbosity_level=1)
    
    return X

def evaluates_single_classification(X_for_validation, Y_for_validation, supervised_ml_results, indicator_label, is_evaluate_only_best_approach=False):
    """Evaluates a single classification model set (either for beta or for DY)

    Args:
        X_for_validation (pandas.DataFrame): _description_
        Y_for_validation (pandas.Series): _description_
        supervised_ml_results (dict): dictionary with trained supervised ML models
        indicator_label (str): label to be used for printing information on stdout
        is_evaluate_only_best_approach (bool, optional): If True, evaluates only the best approach, ignoring others. Defaults to False
    """
    
    true_counter = 0
    false_counter = 0
    for index, row in X_for_validation.iterrows():
            table_header = [index, f'{indicator_label.title()} Class', 'Predicted Value', 'Real Value', 'Matches prediction?']
            table_content = []
            for current_method in supervised_ml_results:
                    predicted_value = supervised_ml_results[current_method]["model"].predict(pd.DataFrame(row).transpose())[0]
                    table_content.append([current_method, predicted_value, predicted_value, Y_for_validation[indicator_label].loc[index], Y_for_validation[indicator_label].loc[index] == predicted_value])
                    
                    # Counting results
                    if Y_for_validation[indicator_label].loc[index] == predicted_value:
                            true_counter = true_counter + 1
                    else:
                            false_counter = false_counter + 1
                            
                    # if evaluate only the best approach flag is set
                    if is_evaluate_only_best_approach:
                        break
            utilities.print_verbose(
                Utilities.get_tabular_data(
                    table_header,
                    table_content), verbosity_level=2)

    utilities.print_verbose(f'Trues: {true_counter}', verbosity_level=1)
    utilities.print_verbose(f'Falses: {false_counter}', verbosity_level=1)
    utilities.print_verbose(f'Performance: {100 * true_counter / (true_counter + false_counter):.2f}%', verbosity_level=1)

def validate_algorithms(X_trainning, valid_fiis_list=None):
    X_for_validation, Y_validation_beta, Y_validaton_dy = data_preparation(valid_fiis_list=valid_fiis_list, start_date=start_date_for_validation, end_date=end_date_for_validation)
    if len(X_for_validation.columns) < len(X_trainning.columns):
        utilities.eprint(0, 'Validation set must be at least the same size as the trainning set. Rearrange start and end states for either trainning or validation sets!')
        exit(1)
    X_for_validation = X_for_validation.iloc[:,len(X_for_validation.columns) - len(X_trainning.columns):]   # Necessary to keep both modeling and testing datasets with same size (amount of columns)
    X_for_validation = X_for_validation.dropna()
    
    # Models evaluation
    # Beta models evaluation
    utilities.print_verbose("Beta evaluation (all approaches)", verbosity_level=1)
    evaluates_single_classification(X_for_validation, Y_validation_beta, beta_supervised_ml_results, 'beta')
    utilities.print_verbose("Beta evaluation (best approach)", verbosity_level=1)
    evaluates_single_classification(X_for_validation, Y_validation_beta, beta_supervised_ml_results, 'beta', is_evaluate_only_best_approach=True)

    utilities.print_verbose('', verbosity_level=1)
    
    # DY models evaluation
    utilities.print_verbose("DY evaluation (all approaches)", verbosity_level=1)
    evaluates_single_classification(X_for_validation, Y_validaton_dy, dy_supervised_ml_results, 'DY')
    utilities.print_verbose("DY evaluation (best approach)", verbosity_level=1)
    evaluates_single_classification(X_for_validation, Y_validaton_dy, dy_supervised_ml_results, 'DY', is_evaluate_only_best_approach=True)

def main():
    init()
    
    # Picks only valid FIIs for evaluation
    valid_fiis_list = fiis_definition.get_registered_fiis_list(only_valid_fiis=True)
     
    # Trainning algorithms
    utilities.print_verbose('Daily quotation retrieval (Trainning)', verbosity_level=1)
    X_trainning = train_algorithms(valid_fiis_list=valid_fiis_list)
    
    # Validating algorithms
    utilities.print_verbose('Daily quotation retrieval (Validation)', verbosity_level=1)
    validate_algorithms(X_trainning, valid_fiis_list)
    

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
