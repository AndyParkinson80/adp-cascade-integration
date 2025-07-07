from datetime import datetime, time

def find_run_type():
    current_time = datetime.now().time()

    if time(0,0) <= current_time < time(0,10):
        run_type = 1
    elif time(0,30) <= current_time < time(0,40):
        run_type = 2
    elif time(1,0) <= current_time < time(1,10):
        run_type = 3
    elif time(3,0) <= current_time < time(3,10):
        run_type = 1
    elif time(3,30) <= current_time < time(3,40):
        run_type = 4    
    elif time(4,0) <= current_time < time(4,10):
        run_type = 5
    else:
        run_type = 1

    return run_type

if __name__ == "__main__":
    find_run_type()   
