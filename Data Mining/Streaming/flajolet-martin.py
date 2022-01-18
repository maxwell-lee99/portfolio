from blackbox import BlackBox
import random
import math
from statistics import mean, median

def myhashes(s):
    result=[]
    for i in range(len(ax)):
        for x in range(len(ax[i])):
            result.append((hash(s)*ax[i][x]+b[i][x])%512)
    
    return result

if __name__ == '__main__':

    filename='hw5/users.txt'
    stream_size=300
    num_of_asks=30

    bx=BlackBox()

    ax=[[1,3,5],[7,11,13],[17,19,23]]
    b=[[0 for x in range(3)] for i in range(3)]

    for i in range(3):
        for x in range(3):
            b[i][x]=random.randint(1,100)

    

    stream_pred=[]
    stream_true=[]

    for it in range(0, num_of_asks):
        true_set=set()
        stream=bx.ask(filename,stream_size)
        for user in stream:
            true_set.add(user)
        global_results=[]
        for group in range(3):
            group_results=[]
            for f in range(3):
                bit_map=[0 for x in range(9)]
                for user in stream:
                    hashed=(hash(user)*ax[group][f]+b[group][f])%512
                    bined=format(hashed,'b')
                    
                    one=False
                    count=len(bined)-2
                    
                    while one==False:
                        if bined[count]==1:
                            one=True
                        try:
                            bined[count-1]
                            count-=1
                            
                        except:
                            one=True
                    try:
                        bit_map[count]=1
                    except:
                        print(count)
                lowest=9
                for x in range(len(bit_map)):
                    
                    if bit_map[x]==0 and x<lowest:
                        lowest=x
                
                group_results.append(2**lowest/.77351)
            
            global_results.append(median(group_results))
        stream_true.append(len(true_set))
        stream_pred.append(round(mean(global_results),0))

    outfile='out.csv'

    textfile=open(outfile,'w')

    textfile.write('Time,Ground Truth,Estimation')

    for i in range(len(stream_true)):
        textfile.write('\n{},{},{}'.format(i,stream_true[i],stream_pred[i]))

    

