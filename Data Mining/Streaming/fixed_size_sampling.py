from blackbox import BlackBox
import random
import math
from statistics import mean, median



if __name__ == '__main__':

    filename='hw5/users.txt'
    stream_size=100
    num_of_asks=30
    random.seed(553)

    bx=BlackBox()

    outfile='out.csv'

    textfile=open(outfile,'w')

    textfile.write('segnum,0_id,20_id,40_id,60_id,80_id')

    res=[]
    n_users=0

    for i in range(0, num_of_asks):
        stream=bx.ask(filename,stream_size)
        if i == 0:
            for user in stream:
                res.append(user)
                n_users+=1
        else:
            for user in stream:
                n_users+=1
                if random.random()<100/n_users:
                    res[random.randint(0,99)]=user
            
        textfile.write('\n{},{},{},{},{},{}'.format((i+1)*100,res[0],res[20],res[40],res[60],res[80]))
        