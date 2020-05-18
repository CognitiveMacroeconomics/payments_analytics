from models import make_gru
from data import ScenarioGenerator, RTGSSequence

#load local config to hide database values
try:
    import local_config
except:
    import config
    
def exp1():
    return None

def exp2():
    return None


def run_all():
    res1 = exp1()
    res2 = exp2()
    return {"exp1":res1, "exp2":res2}



if __name__=="__main__":
    model = make_gru((20,400))
    print(model.summary())