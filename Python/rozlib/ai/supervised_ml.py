"""
Class with supervised Machine Learning (ML) facilities

Covered methods:
KNN
Decision Tree (DT)
Random Forest (RF)
MLP
Bagging
Boosting
Voting

Author: Rozario Engenharia

First release: December 8th, 2021
"""

from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import VotingClassifier, BaggingClassifier, AdaBoostClassifier, RandomForestClassifier
#import pandas as pd # for dealing with dataframes

try:
    from .ai import AI
except ImportError:
    from ai import AI

class SupervisedML(AI):
    
    # If a model is evaluated once, keeps track of it to avoid evaluating same model more than once
    __decision_tree = None
    __mlp = None
    __random_forest = None
    __voting = None
    __bagging = None
    __knn = None
    __boosting = None
    
    def __init__(self, X=None, X_train=None, X_test=None, Y=None, Y_test=None, Y_train=None, seed=10, test_set_size=0.3, epochs=10):
        super().__init__(X=X, X_train=X_train, X_test=X_test, Y=Y, Y_test=Y_test, Y_train=Y_train, epochs=epochs, seed=seed, test_set_size=test_set_size)

    def __evaluate_generic_supervised_method(self, method_name, X_train, y_train, re_evaluate=False):
        # Generic model
        generic_model = None

        if method_name.lower() == 'tree':
            generic_model = DecisionTreeClassifier(min_samples_split=5, min_samples_leaf=3, random_state=self._seed)
        elif method_name.lower() == 'mlp':
            generic_model = MLPClassifier(hidden_layer_sizes=(10,), max_iter=10000, random_state=self._seed)
        elif method_name.lower() == 'knn':
            generic_model = KNeighborsClassifier(n_neighbors=3)
        elif method_name.lower() == 'bagging':
            if (self.__decision_tree == None) or (re_evaluate == True):
                self.__decision_tree = self.__evaluate_generic_supervised_method('tree')
            generic_model = BaggingClassifier(base_estimator=self.__decision_tree, n_estimators=50, random_state=self._seed)
        elif method_name.lower() == 'boosting':
            generic_model = AdaBoostClassifier(n_estimators=50,random_state=self._seed)
        elif method_name.lower() == 'random_forest':
            generic_model = RandomForestClassifier(n_estimators=50, random_state=self._seed)
        elif method_name.lower() == 'voting':
            if (self.__decision_tree == None) or (re_evaluate == True):
                self.__decision_tree = self.__evaluate_generic_supervised_method('tree')
            if (self.__mlp == None) or (re_evaluate == True):
                self.__mlp = self.__evaluate_generic_supervised_method('mlp')
            if (self.__knn == None) or (re_evaluate == True):
                self.__knn = self.__evaluate_generic_supervised_method('knn')
            generic_model = VotingClassifier(estimators=[('Tree', self.__decision_tree), ('MLP', self.__mlp), ('kNN', self.__knn)], voting='hard') 

        generic_model.fit(X_train, y_train.values.ravel().astype(int))
        return generic_model
    
    def evaluate_decision_tree(self, X_train, y_train):
        self.__decision_tree = self.__evaluate_generic_supervised_method('tree', X_train, y_train)
        return self.__decision_tree

    def evaluate_mlp(self, X_train, y_train):
        self.__mlp = self.__evaluate_generic_supervised_method('mlp', X_train, y_train)
        return self.__mlp

    def evaluate_knn(self, X_train, y_train):
        self.__knn = self.__evaluate_generic_supervised_method('knn', X_train, y_train)
        return self.__knn

    def evaluate_voting(self, X_train, y_train):
        self.__voting = self.__evaluate_generic_supervised_method('voting', X_train, y_train)
        return self.__voting
        """
        VotingClassifier(estimators=[('Tree',
                                    DecisionTreeClassifier(ccp_alpha=0.0,
                                                            class_weight=None,
                                                            criterion='gini',
                                                            max_depth=None,
                                                            max_features=None,
                                                            max_leaf_nodes=None,
                                                            min_impurity_decrease=0.0,
                                                            min_impurity_split=None,
                                                            min_samples_leaf=3,
                                                            min_samples_split=5,
                                                            min_weight_fraction_leaf=0.0,
                                                            presort='deprecated',
                                                            random_state=10,
                                                            splitter='best')),
                                    ('MLP',
                                    MLPClassifier(
                                                    nesterovs_momentum=True,
                                                    power_t=0.5, random_state=10,
                                                    shuffle=True, solver='adam',
                                                    tol=0.0001, validation_fraction=0.1,
                                                    verbose=False, warm_start=False)),
                                    ('kNN',
                                    KNeighborsClassifier(algorithm='auto',
                                                        leaf_size=30,
                                                        metric='minkowski',
                                                        metric_params=None,
                                                        n_jobs=None, n_neighbors=3,
                                                        p=2, weights='uniform'))],
                        flatten_transform=True, n_jobs=None, voting='hard',
                        weights=None)
        """

    def evaluate_bagging(self, X_train, y_train):
        self.__bagging = self.__evaluate_generic_supervised_method('bagging', X_train, y_train)
        return self.__bagging
        """
        BaggingClassifier(base_estimator=DecisionTreeClassifier(ccp_alpha=0.0,
                                                                class_weight=None,
                                                                criterion='gini',
                                                                max_depth=None,
                                                                max_features=None,
                                                                max_leaf_nodes=None,
                                                                min_impurity_decrease=0.0,
                                                                min_impurity_split=None,
                                                                min_samples_leaf=3,
                                                                min_samples_split=5,
                                                                min_weight_fraction_leaf=0.0,
                                                                presort='deprecated',
                                                                random_state=10,
                                                                splitter='best'),
                        bootstrap=True, bootstrap_features=False, max_features=1.0,
                        max_samples=1.0, n_estimators=50, n_jobs=None,
                        oob_score=False, random_state=10, verbose=0,
                        warm_start=False)
        """
        return bagging_clf

    def evaluate_boosting(self, X_train, y_train):
        self.__boosting = self.__evaluate_generic_supervised_method('boosting', X_train, y_train)
        return self.__boosting
        """
        AdaBoostClassifier(algorithm='SAMME.R', base_estimator=None, learning_rate=1.0,
                        n_estimators=50, random_state=10)
        return boosting_clf
        """

    def evaluate_random_forest(self, X_train, y_train):
        self.__random_forest = self.__evaluate_generic_supervised_method('random_forest', X_train, y_train)
        return self.__random_forest
        """
        RandomForestClassifier(bootstrap=True, ccp_alpha=0.0, class_weight=None,
                            criterion='gini', max_depth=None, max_features='auto',
                            max_leaf_nodes=None, max_samples=None,
                            min_impurity_decrease=0.0, min_impurity_split=None,
                            min_samples_leaf=1, min_samples_split=2,
                            min_weight_fraction_leaf=0.0, n_estimators=50,
                            n_jobs=None, oob_score=False, random_state=10, verbose=0,
                            warm_start=False)
        """
    
    # Tests each method, storing its accuracy and returning the best model
    def evaluate_best_supervised_method(self):
        # splitting testing and trainning sets
        #[X_train, X_test, y_train, y_test] = self.split_test_train_sets()

        # Results dict
        results = {}

        # Decision tree:
        dt = self.evaluate_decision_tree(self._X_train, self._Y_train)
        results['decision_tree'] = {}
        results['decision_tree']['accuracy'] = self.evaluate_method_accuracy(dt)
        results['decision_tree']['model'] = dt

        # MLP:
        mlp = self.evaluate_mlp(self._X_train, self._Y_train)
        results['mlp'] = {}
        results['mlp']['accuracy'] = self.evaluate_method_accuracy(mlp)
        results['mlp']['model'] = mlp

        # KNN:
        knn = self.evaluate_knn(self._X_train, self._Y_train)
        results['knn'] = {}
        results['knn']['accuracy'] = self.evaluate_method_accuracy(knn)
        results['knn']['model'] = knn
        
        # Voting:
        voting = self.evaluate_voting(self._X_train, self._Y_train)
        results['voting'] = {}
        results['voting']['accuracy'] = self.evaluate_method_accuracy(voting)
        results['voting']['model'] = voting

        # Bagging:
        bagging = self.evaluate_bagging(self._X_train, self._Y_train)
        results['bagging'] = {}
        results['bagging']['accuracy'] = self.evaluate_method_accuracy(bagging)
        results['bagging']['model'] = bagging

        # Boosting:
        boosting = self.evaluate_boosting(self._X_train, self._Y_train)
        results['boosting'] = {}
        results['boosting']['accuracy'] = self.evaluate_method_accuracy(boosting)
        results['boosting']['model'] = boosting

        # Random forest:
        rf = self.evaluate_random_forest(self._X_train, self._Y_train)
        results['random_forest'] = {}
        results['random_forest']['accuracy'] = self.evaluate_method_accuracy(rf)
        results['random_forest']['model'] = rf

        sorted_dict = {}
        for key in sorted(results, key=lambda item: results[item]['accuracy'], reverse=True): 
            sorted_dict[key] = results[key]
        return sorted_dict
