"""
Class with unsupervised Machine Learning (ML) facilities

Covered methods:
K-means
C-mean (fuzzy)

Author: Rozario Engenharia

First release: December 16th, 2021
"""

from sklearn.cluster import KMeans  # for the k-means clustering
import skfuzzy as fuzz  # Fuzzy clustering
import numpy as np  # for number manipulation and reshaping data
import configparser # for reading settings file
import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)

# for autoenconders
import keras
from tensorflow.keras import layers, losses, models

# Reads rozlib library path from config.ini file and import libraries packages
rozlib_path = ''
def add_rozlib_library_path():
    global rozlib_path
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini', encoding='utf-8')
    rozlib_path = config['GENERAL']['rozlibFolderPath']
    sys.path.insert(1, rozlib_path)
add_rozlib_library_path()

from rozlib.util import Utilities

try:
    from .ai import AI
except ImportError:
    from ai import AI

verbosity = 1
# Utilities object
utilities = Utilities(verbosity)

class UnsupervisedML(AI):
    __n_clusters = 10
    
    # Unsupervised clustering methods covered by this library
    __unsupervised_clustering_methods = ['kmeans', 'cmeans']
    
    #def __init__(self, X, epochs=30, seed=10, test_set_size=0.3, n_clusters=10):
    #    super().__init__(X=X, Y=None, epochs=epochs, seed=seed, test_set_size=test_set_size)
    #    self.__n_clusters = n_clusters
    
    def __init__(self, X=None, X_train=None, X_test=None, epochs=30, seed=10, test_set_size=0.3, n_clusters=10):
        super().__init__(X=X, X_train=X_train, Y_train=None, X_test=X_test, Y_test=None, epochs=epochs, seed=seed, test_set_size=test_set_size)
        self.__n_clusters = n_clusters
    
    # Evaluates a generic non supervised approach, according to input
    # @param approach the name of non supervised approach
    # @return a dictionary with number of clusters as keys and values:
    # k-means/c-means object with the given approach data (key: centers)
    # clusters predicted by the model (key: model)
    # cluster membership (for grouping data according to clusters, key: clusters)
    def __evaluate_clustering_approach(self, approach):
        # Checks if given approach is covered by this library
        if approach.lower() not in self.__unsupervised_clustering_methods:
            raise ValueError(f'{approach} is not covered by this library. Approaches covered: {self.__unsupervised_clustering_methods}')
        
        clusters_dict = {}
        for current_cluster_amount in range(2, self.__n_clusters + 1):
            clusters_dict[current_cluster_amount] = {}
            model = None
            if approach.lower() == 'kmeans':
                model = KMeans(n_clusters=current_cluster_amount, random_state=self._seed)
                #print(model)
                cluster_membership = model.fit_predict(self._X.to_numpy())
                clusters_dict[current_cluster_amount]['centers'] = model.cluster_centers_
            else:
                model = fuzz.cluster.cmeans(
                    self._X.to_numpy().transpose(), current_cluster_amount, 2, error=0.005, maxiter=1000, init=None)
                cluster_membership = np.argmax(model[1], axis=0)
                clusters_dict[current_cluster_amount]['centers'] = model[0]
            clusters_dict[current_cluster_amount]['model'] = model
            clusters_dict[current_cluster_amount]['clusters'] = cluster_membership
        return clusters_dict
    
    # Evaluates the non supervised approach K-Means for clustering data from a dataset. Varies the amount of clusters to find a best cluster arrangement
    # for the problem
    # @return a dictionary with number of clusters as keys and values:
    # k-means object with the given approach data (key: centers)
    # clusters predicted by the model (key: model)
    # cluster membership (for grouping data according to clusters, key: clusters)
    def evaluate_k_means_approach(self):
        return self.__evaluate_clustering_approach('kmeans')
    
    # Evaluates the non supervised approach C-Means for clustering data from a dataset. Varies the amount of clusters to find a best cluster arrangement
    # for the problem
    # @return a dictionary with number of clusters as keys and values:
    # c-means object with the given approach data (key: centers)
    # clusters predicted by the model (key: model)
    # cluster membership (for grouping data according to clusters, key: clusters)
    def evaluate_c_means_approach(self):
        return self.__evaluate_clustering_approach('cmeans')
    
    def __evaluate_autoencoder(self,
                               n_features,
                               encoder_layers_list,
                               decoder_layers_list,
                               optimizer='adam',
                               loss=losses.BinaryCrossentropy(),
                               autoencoder_prefix='',
                               normalization_value=0,
                               denoising_ae_noise_rate=0):
        # Verifica se há necessidade de normalizar os valores
        if normalization_value != 0:
            # Vamos normalizar todos os valores (pixels) entre 0 e 1
            x_train = self._X_train.astype('float32') / float(normalization_value)
            x_test  = self._X_test.astype('float32') / float(normalization_value)

        # aqui achatamos o sinal, cada imagem 28 x 28 vira um vetor de 784 valores
        x_train = x_train.reshape((len(x_train), np.prod(x_train.shape[1:])))
        x_test  = x_test.reshape((len(x_test), np.prod(x_test.shape[1:])))

        # checando a dimensao dos dados
        utilities.print_verbose(f'* Dimensões do conj treinamento: {x_train.shape}', verbosity_level=1)
        utilities.print_verbose(f'* Dimensões do conj teste:       {x_test.shape}', verbosity_level=1)

        n_neurons = x_train.shape[1]
        
        # Prepara os layers do encoder e do decoder
        encoder_layers = []
        
        # Caso o autoencoder seja do tipo denoising (remoção de ruídos), insere uma primeira camada de Dropout:
        if denoising_ae_noise_rate != 0:
            encoder_layers.append(layers.Dropout(rate = denoising_ae_noise_rate, input_shape = [n_neurons]))
        for current_layer, i in zip(encoder_layers_list, range(0, len(encoder_layers_list))):
            if i == 0:
                encoder_layers.append(layers.Dense(current_layer[0],
                                                   current_layer[1],
                                                   input_shape = [n_neurons]))
            else:
                encoder_layers.append(layers.Dense(current_layer[0],
                                                   current_layer[1]))
        
        decoder_layers = []
        for current_layer, i in zip(decoder_layers_list, range(0, len(decoder_layers_list))):
            if i == 0:
                # Autoencoders com uma camada só tem um comportamente levemente diferente: o valor vindo
                # da tupla do decoder é desconsiderado e a quantidade de neurônios é utilizada em seu lugar
                decoder_layers.append(layers.Dense(current_layer[0] if len(decoder_layers_list) != 1 else n_neurons,
                                                   current_layer[1],
                                                   input_shape = [n_features]))
            else:
                decoder_layers.append(layers.Dense(current_layer[0],
                                                   current_layer[1]))

        # definir um encoder, que recebe como entrada o sinal já "achatado", 
        # e possui apenas uma camada densa com 32 neurônios (codings),
        # A função de ativação de cada neurônio é do tipo ReLU
        encoder = models.Sequential(name = autoencoder_prefix + "_ae_encoder",
            layers = encoder_layers
            )

        # imprimindo o modelo criado
        utilities.print_verbose(encoder.summary(), verbosity_level=1)

        # definir um decoder, que recebe como entrada um vetor com 32 valores 
        # numéricos (codings), e recria as inputs (saida = 784 valores)
        # a função de ativação dos neurônios é sigmoidal
        decoder = models.Sequential(name = autoencoder_prefix + "_ae_decoder", 
            layers = decoder_layers
            )

        # imprimindo o modelo criado
        utilities.print_verbose(decoder.summary(), verbosity_level=1)

        # Agora podemos criar o autoencoder combinando enconder + decoder
        autoencoder = models.Sequential(
            name = autoencoder_prefix + "_ae", 
            layers = [encoder, decoder]
        )

        # imprimindo o modelo
        utilities.print_verbose(autoencoder.summary(), verbosity_level=1)

        # Vamos configurar as opções de treinamento do nosso modelo (autoencoder)
        # usaremos o algoritmo Adam, e como função de custo (loss function)
        # a entropia binária - medida recomendada para classificação binária
        # (é a mesma que temos descrita nos slides de aula)
        autoencoder.compile(optimizer=optimizer, loss=loss)

        # Configurado o aprendizado, precisamos treinar o modelo
        # vamos armazenar todas as informações de treinamento na variável 
        # 'autoencoder_history'
        autoencoder_history = autoencoder.fit(
            # x = dados de treinamento (input data)
            x = x_train,
            # y = target / labels, aqui será x_train também, pois queremos reconstruir a entrada
            y = x_train,
            # épocas de treinamento
            epochs = self._epochs,
            # número de amostras computadas para ter uma atualização do gradiente
            batch_size = 256,
            # embaralhar os exemplos de treinamento antes de cada época
            shuffle = True,
            # usamos o conjunto de teste como validação interna do treinamento
            validation_data =(x_test, x_test)
        )

        # realizar a codificação do conjunto de teste
        encoded_data = encoder.predict(x_test)

        # reconstruir as entradas por meio dos codings
        decoded_data = decoder.predict(encoded_data)

        return decoded_data, autoencoder_history
    
    # Evaluates a single layer autoencoder
    # @param n_features amount of features the autoencoder should evaluate
    # @param encoder_activation activation algorithm used by the encoder. Default: relu
    # @param decoder_activation activation algorithm used by the decoder. Default: sigmoid
    # @param optimizer optimizer used by the autoencoder. Default: adam
    # @param loss loss approach used by the autoencoder. Default: losses.BinaryCrossentropy()
    # @param normalizartion_value value used to normalize the dataset. Usually this value should put all dataset between 0 and 1. Default: 0
    # @return tuple with:
    # 0. decoded data, with features mapped by the autoencoder
    # 1. autoencoder history, with data returned by the "model fit" method
    def evaluate_simple_autoencoder(self,
                                    n_features,
                                    encoder_activation='relu',
                                    decoder_activation='sigmoid',
                                    optimizer='adam',
                                    loss=losses.BinaryCrossentropy(),
                                    normalization_value=0):
        return self.__evaluate_autoencoder(n_features,
                                           [(n_features, encoder_activation)],
                                           [(0, decoder_activation)],
                                           optimizer=optimizer,
                                           loss=loss,
                                           autoencoder_prefix='simple',
                                           normalization_value=normalization_value)
    
    # Evaluates a stacked (multiple layer) autoencoder
    # @param n_features amount of features the autoencoder should evaluate
    # @param encoder_layers_list a list of tuples with pattern ("number of neurons for layer", "activation algorithm")
    # @param decoder_layers_list a list of tuples with pattern ("number of neurons for layer", "activation algorithm")
    # @param optimizer optimizer used by the autoencoder. Default: adam
    # @param loss loss approach used by the autoencoder. Default: losses.BinaryCrossentropy()
    # @param normalizartion_value value used to normalize the dataset. Usually this value should put all dataset between 0 and 1. Default: 0
    # @return tuple with:
    # 0. decoded data, with features mapped by the autoencoder
    # 1. autoencoder history, with data returned by the "model fit" method
    def evaluate_stacked_autoencoder(self,
                                     n_features,
                                     encoder_layers_list,
                                     decoder_layers_list,
                                     optimizer='adam',
                                     loss=losses.BinaryCrossentropy(),
                                     normalization_value=0):
        return self.__evaluate_autoencoder(
                                   n_features,
                                   encoder_layers_list,
                                   decoder_layers_list,
                                   optimizer=optimizer,
                                   loss=loss,
                                   autoencoder_prefix='stacked',
                                   normalization_value=normalization_value)
    
    # Evaluates a denoising autoencoder
    # @param n_features amount of features the autoencoder should evaluate
    # @param encoder_layers_list a list of tuples with pattern ("number of neurons for layer", "activation algorithm")
    # @param decoder_layers_list a list of tuples with pattern ("number of neurons for layer", "activation algorithm")
    # @param optimizer optimizer used by the autoencoder. Default: adam
    # @param loss loss approach used by the autoencoder. Default: losses.BinaryCrossentropy()
    # @param normalizartion_value value used to normalize the dataset. Usually this value should put all dataset between 0 and 1. Default: 0
    # @param denoising_ae_noise_rate denoising rate used by the Dropout layer. Default: 0.5
    # @return tuple with:
    # 0. decoded data, with features mapped by the autoencoder
    # 1. autoencoder history, with data returned by the "model fit" method
    def evaluate_denoising_autoencoder(self,
                                     n_features,
                                     encoder_layers_list,
                                     decoder_layers_list,
                                     optimizer='adam',
                                     loss=losses.BinaryCrossentropy(),
                                     normalization_value=0,
                                     denoising_ae_noise_rate=0.5):
        return self.__evaluate_autoencoder(
                                   n_features,
                                   encoder_layers_list,
                                   decoder_layers_list,
                                   optimizer=optimizer,
                                   loss=loss,
                                   autoencoder_prefix='denoising',
                                   normalization_value=normalization_value,
                                   denoising_ae_noise_rate=denoising_ae_noise_rate)
