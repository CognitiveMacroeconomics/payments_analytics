from keras.models import Model
from keras.layers import Input, GRU, Dense, Dropout, RepeatVector, SimpleRNN

#input_shape (window_size, feature_size)
def make_autoencoder(input_shape, activation = "relu", hidden_layer_size = 50):
    """
    make the autoencoder model with a specific input_shape.

    Parameters:
    input_shape (int,int): (1,number of features)
    activation (str): activation function. default is relu
    hidden_layer_size (int): size of hidden layer 
        
    Model specifics:
    """
    
    inp_layer = Input(shape=input_shape)
    hidden_layer = Dense(hidden_layer_size, activation = activation)(inp_layer)
    output_layer = Dense(7, activation = activation)(hidden_layer_size)
    model = Model(inputs=inp_layer, outputs=output_layer, name="simple_autoencoder")
    return model





if __name__=="__main__":
    model = make_rnn((1,7))
    print(model.summary())