from keras.models import Model
from keras.layers import Input, GRU, Dense, Dropout, RepeatVector

#input_shape (window_size, feature_size)
def make_lstm(input_shape):

    inp_layer = Input(shape=input_shape)
    gru_layer = LSTM(32)(inp_layer)
    repeat = RepeatVector(input_shape[0])(gru_layer)
    #dummy model this but do we return sequence?
    output_gru = LSTM(400, return_sequences=True)(repeat)
    model = Model(inputs=inp_layer, outputs=output_gru, name="FerengiLSTM")
    return model




if __name__=="__main__":
    model = make_gru((20, 400))
    print(model.summary())