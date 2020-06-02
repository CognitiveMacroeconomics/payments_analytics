from models import make_gru, make_rnn, make_lstm
from data import ScenarioGenerator, TargetHandler, RTGSSequence

#load local config to hide database values
try:
    import local_config
except:
    import config
    
def run_rnn():
    """
    Run a RNN model
    
    input:
        model
        db_handler
        generator
        model & training parameters
    """
    model = make_rnn((1,7))
    model.compile(loss="mse", optimizer="adam", metrics=["accuracy"])
    db_handler = TargetHandler("localhost","tempdb","sa","123456QWERD!")
    generator = RTGSSequence(db_handler, 16, 1)

    model.fit_generator(generator, epochs=10)

    return None

def exp2():
    """
    Run experiment 2
    """
    return None


#def run_all():
    """
    run all experiments
    """
   # res1 = exp1()
   # res2 = exp2()
   # return {"exp1":res1, "exp2":res2}



if __name__=="__main__":
    run_rnn()