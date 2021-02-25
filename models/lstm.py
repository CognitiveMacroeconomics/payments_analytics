from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, Dense, RepeatVector, TimeDistributed

#input_shape (window_size, feature_size)
def make_lstm(input_shape, hidden_layer_size = 16):
    """
    make the lstm model with specific input_shape.
    
    Parameters:
    input_shape (tuple(int, int)): (window_size, n_features)
    hidden_layer_size (int): size of het latent space. This should be the same for
    all models.
    
    Model specifics:
    
    Model: "FerengiLSTM"
    _________________________________________________________________
    Layer (type)                 Output Shape                
    =================================================================
    input_2 (InputLayer)         [(None, window, n_features)]               
    _________________________________________________________________
    lstm (LSTM)                  (None, hidden_layer_size)            
    _________________________________________________________________
    repeat_vector_1 (RepeatVecto (None, window, hidden_layer_size)       
    _________________________________________________________________
    lstm_1 (LSTM)                (None, window, hidden_layer_size)    
    _________________________________________________________________
    time_distributed_1 (TimeDist (None, window, n_features)            
    =================================================================
    
    
    """
    inp_layer = Input(shape=input_shape)
    input_lstm = LSTM(hidden_layer_size, return_sequences=False)(inp_layer)
    repeat = RepeatVector(input_shape[0])(input_lstm)
    output_lstm = LSTM(hidden_layer_size*2, return_sequences=True)(repeat)
    outp_layer = TimeDistributed(Dense(input_shape[1]))(output_lstm)
    model = Model(inputs=inp_layer, outputs=outp_layer, name="FerengiLSTM")
    return model


## EXAMPLE MODEL FROM https://towardsdatascience.com/step-by-step-understanding-lstm-autoencoder-layers-ffab055b6352
