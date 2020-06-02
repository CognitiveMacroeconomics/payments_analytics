from keras.models import Model
from keras.layers import Input, GRU, Dense, Dropout, RepeatVector, SimpleRNN

#input_shape (window_size, feature_size)
def make_rnn(input_shape):
    """
    make the RNN model with a specific input_shape.
        
    Model specifics:
    """
    
    inp_layer = Input(shape=input_shape)
    #gru_layer = GRU(32)(inp_layer)
    #repeat = RepeatVector(input_shape[0])(gru_layer)
    #dummy model this but do we return sequence?
    rnn_layer = SimpleRNN(400, return_sequences=True)(inp_layer)
    output_layer = Dense(7, activation = "relu")(rnn_layer)
    model = Model(inputs=inp_layer, outputs=output_layer, name="FerengiGRU")
    return model





if __name__=="__main__":
    model = make_rnn((1,7))
    print(model.summary())


    


