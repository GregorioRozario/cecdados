from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

import pandas as pd # for dealing with dataframes
import seaborn as sns   # for heatmap plotting
import matplotlib.pyplot as plt # for plotting graphs
import numpy as np  # for number manipulation and reshaping data

class AI:
    """Artificial intelligence facility class

    Author: Rozario Engenharia

    First release: December 8th, 2021
    
    Attributes
    ----------
    _test_set_size : float
        Test set size in absolute counts
    _seed : int
        Seed to be used in pseudo random value generation
    _X : pandas.DataFrame or pandas.Series
        Dataframe/Series values for machine learning predictions. Should not contain classification values (for supervised learning).
        This dataframe/series should be used when test/trainning sets are splitted by method split_test_train_sets
    _Y : pandas.DataFrame or pandas.Series
        Dataframe/Series classification values for machine learning predictions.
        This dataframe/series should be used when test/trainning sets are splitted by method split_test_train_sets
    _X_train : pandas.DataFrame or pandas.Series
        Dataframe/Series values for machine learning trainning purposes.
        This dataframe/series should be used when test/trainning sets are splitted outside this class
    _Y_train : pandas.DataFrame or pandas.Series
        Dataframe/Series classification values for machine learning trainning purposes.
        This dataframe/series should be used when test/trainning sets are splitted outside this class
    _X_test : pandas.DataFrame or pandas.Series
        Dataframe/Series values for machine learning testing purposes.
        This dataframe/series should be used when test/trainning sets are splitted outside this class
    _Y_test : pandas.DataFrame or pandas.Series
        Dataframe/Series classification values for machine learning testing purposes.
        This dataframe/series should be used when test/trainning sets are splitted outside this class
    _epochs : int
        Epochs for neural networks or autoencoders usage
    
    Methods
    -------
    
    """
    
    _test_set_size = 0.3
    _seed = 10
    _X = None
    _Y = None
    _X_train = None
    _Y_train = None
    _X_test = None
    _Y_test = None
    _epochs = 10
    
    def __init__(self, X=None, Y=None, X_train=None, Y_train=None, X_test=None, Y_test=None, epochs=30, seed=10, test_set_size=0.3):
        """
        Parameters
        ----------
        X_train : pandas.DataFrame or pandas.Series
           Dataframe/Series values for machine learning trainning purposes.
           This dataframe/series should be used when test/trainning sets are splitted outside this class
        Y_train : pandas.DataFrame or pandas.Series
           Dataframe/Series classification values for machine learning trainning purposes.
           This dataframe/series should be used when test/trainning sets are splitted outside this class
        X_test : pandas.DataFrame or pandas.Series
           Dataframe/Series values for machine learning testing purposes.
           This dataframe/series should be used when test/trainning sets are splitted outside this class
        Y_test : pandas.DataFrame or pandas.Series
            Dataframe/Series classification values for machine learning testing purposes.
            This dataframe/series should be used when test/trainning sets are splitted outside this class
        epochs : int, optional
            Epochs for neural networks or autoencoders usage (default is 30)
        seed : int, optional
            Seed to be used in pseudo random value generation (default is 10)
        test_set_size : float, optional
            Test set size in absolute counts (default is 0.3)
        """
        
        if X is None:
            self._X_train = X_train
            self._Y_train = Y_train
            self._X_test = X_test
            self._Y_test = Y_test
            self._X = np.concatenate((X_train, X_test), axis=0)
            try:
                self._Y = np.concatenate((Y_train, Y_test), axis=0)
            except ValueError:
                # Unssupervised learning has no Y (classes) array
                pass
        else:
            self._X = X
            self._Y = Y
            try:
                (self._X_train, self._X_test, self._Y_train, self._Y_test) = self.split_test_train_sets()
            except ValueError as e:
                print(e)
                # Clustering approaches (C-means or K-means) have no need to split trainning and testing sets
                pass
        
        self._seed = seed
        self._test_set_size = test_set_size
        self._epochs = epochs
        np.random.seed(self._seed)
    
    def split_test_train_sets(self):
        """Splits the test and trainning sets according to class attribute _test_set_size

        Returns
        -------
        tuple
            A tuple with pattern (X train, Y train, X test, Y test) for supervised machine learning or
            (X train, X test) for unsupervised machine learning
        """
        
        if self._Y is not None:
            test = train_test_split(self._X, self._Y, test_size=self._test_set_size, stratify=self._Y, random_state=self._seed)
            return train_test_split(self._X, self._Y, test_size=self._test_set_size, stratify=self._Y, random_state=self._seed)
        else:
            return train_test_split(self._X, test_size=self._test_set_size, stratify=self._Y, random_state=self._seed)
    
    def plot_correlation_heat_map(dataset, fig_length=15, fig_width=15, path_to_file=None, show_graph=True):
        """Plots a 2D correlation heat map, showing how variables in a dataset are correlated with each other

        Parameters
        ----------
        dataset : pandas.DataFrame
            A dataframe with all variables data
        fig_length : int, optional
            Chart's length (default is 15)
        fig_width : int, optional
            Chart's width (default is 15)
        path_to_file : str, optional
            Relative or absolute path for saving the graph (to a file) (default is None)
        show_graph : bool, optional
            Flag indicating whether the graph should be displayed (default is True)
        """
        
        plt.figure(figsize=(fig_length,fig_width))
        sns.heatmap(dataset.corr(), annot=True, cmap='OrRd')
        if show_graph:
            plt.show(block=True)
        
        if path_to_file != None:
            plt.savefig(path_to_file)
    
    def get_top_correlation_variables_from_dataset(dataset_df, correlation_limit=1.0, top_n=5, is_abs=True):
        """Retrieves top correlation variables (mutual correlation) comparing pairs of variables in a pandas dataframe
        
        Parameters
        ----------
        dataset_df : pandas.DataFrame
            The dataframe to be evaluated
        correlation_limit : float
            Maximum (absolute) value of correlation for listing variables correlation. Any correlation above this threshold is ignored (default is 1.0)
        top_n : int, optional
            Number of correlations to be listed by this method (default is 5)
        is_abs : bool, optional
            Flag indicating if positive and negative correlations should be treated in absolute (no signals) (default is True)

        Returns:
        ----------
            dataframe
            top_n strongest correlations of pairs of variables
        """
        
        if is_abs:
            corr = dataset_df.corr().abs()
        else:
            corr = dataset_df.corr()
        upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(np.bool))
        to_drop = [column for column in upper.columns if any(upper[column] >= correlation_limit)]
        upper.drop(to_drop, axis=1, inplace=True)

        if top_n > len(dataset_df.columns):
            top_n = len(dataset_df.columns)
        return upper.unstack().sort_values(kind="quicksort", ascending=False).head(top_n)
    
    def get_least_correlated_variables_from_dataset(dataset_df, correlation_limit=1.0, top_n=5, is_abs=True):
        """Retrieves reverse top correlation variables (mutual correlation) comparing pairs of variables in a pandas dataframe
        
        Parameters
        ----------
        dataset_df : pandas.DataFrame
            The dataframe to be evaluated
        correlation_limit : float
            Maximum (absolute) value of correlation for listing variables correlation. Any correlation above this threshold is ignored (default is 1.0)
        top_n : int, optional
            Number of correlations to be listed by this method (default is 5)
        is_abs : bool, optional
            Flag indicating if positive and negative correlations should be treated in absolute (no signals) (default is True)

        Returns:
        ----------
            dataframe
            top_n weakest correlations of pairs of variables
        """
        
        top_correlaction = AI.get_top_correlation_variables_from_dataset(dataset_df,
                                                                         correlation_limit=correlation_limit,
                                                                         top_n=len(dataset_df.columns),
                                                                         is_abs=is_abs)
        return top_correlaction.reindex(index=top_correlaction.index[::-1]).head(top_n)
    
    def evaluate_method_accuracy(self, model):
        """Evaluates an Artificial Inteligence prediction model

        Parameters
        ----------
        model : scikitlearn.ClassifierMixin

        Returns:
            [type]: [description]
        """
        
        return accuracy_score(self._Y_test.values, model.predict(self._X_test))
    
    # Function for replacing textual column values by its equivalent numeric values (to ease machine learning approaches)
    # @param dataframe the dataframe where the replacements should take place
    # @param data_frame_column_name the column name where the replacements should take place
    # @param categories a list whith all possible categories for the given column, to make it possible replacing a text by a number
    def replace_textual_column_by_numeric(dataframe, data_frame_column_name, categories):
        mapping = {}
        for i in range(0, len(categories)):
            mapping[categories[i]] = i
        dataframe.replace({data_frame_column_name: mapping}, inplace=True)

    # Function to replace given textual column values by its equivalent numveric values, for the entire column
    # @param dataset_df the dataframe containing the dataset
    # @param categories_columns text columns to be turned into numeric values
    # @return a dictionary with categories dictionaries, for further usage
    def replace_textual_labels_by_numeric(dataset_df, categories_columns):
        # The categories dictionary to be returned
        categories_dict = {}
        
        for current_category_name in categories_columns:
            current_category = dataset_df[current_category_name].unique()
            categories_dict[current_category_name] = current_category
            AI.replace_textual_column_by_numeric(dataset_df, current_category_name, current_category)

        dataset_df = dataset_df.astype(int)
        return categories_dict

    # Function to replace all textual column values by its equivalent numveric values, in the entire dataset
    # @param dataset_df the dataframe containing the dataset
    # @return a dictionary with categories dictionaries, for further usage
    def replace_all_textual_labels_by_numeric(dataset_df):
        return AI.replace_textual_labels_by_numeric(dataset_df, categories_columns = dataset_df.select_dtypes(include='object').columns)

    def get_dataframe_from_list(data_list, columns_list):
        """Builds a dataframe from a list of lists and a list of columns (to act as header)

        Args:
            data_list (list): list of lists of data to populate dataframe
            columns_list (list): list of columns, to act as dataframe's header

        Returns:
            pandas.Dataframe: a dataframe with data_list contents and columns_list header
        """
        
        return pd.DataFrame(data_list, columns=columns_list)