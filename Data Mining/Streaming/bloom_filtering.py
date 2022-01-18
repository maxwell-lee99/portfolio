from blackbox import BlackBox
import random
import math
from statistics import mean

def myhashes(s):
    result=[]
    for i in range(len(ax)):
        result.append(hash(s)*ax[i]+b[i]%69997)
    
    return result


if __name__ == '__main__':

    filename='hw5/users.txt'
    stream_size=100
    num_of_asks=30

    bx=BlackBox()

    

    bit_array=[0 for i in range(69997)]

    global ax
    global b

    m=0

    with open(filename,'r') as file:
        for line in file:
            m+=1

    optimal_k=round(m/69997*math.log(2))

    

    ax=[0 for i in range(optimal_k)]
    b=[0 for i in range(optimal_k)]

    

    for i in range(optimal_k):
        ax[i]=random.randint(1,69997)*2-1
        b[i]=random.randint(1,69997)
    print(optimal_k)
    true_set=set()

    fprs=[0 for i in range(num_of_asks)]
        
    for i in range(num_of_asks):
        stream=bx.ask(filename,stream_size)
        pred_seens=0
        true_seens=0
        true_negs=0
        for user in stream:
            results=[]
            for x in range(optimal_k):
                results.append((hash(user)*ax[x]+b[x])%69997)
                
            seen=True

            for y in results:
                if bit_array[y]==0:
                    seen=False
                bit_array[y]=1
            
            if seen==True:
                pred_seens+=1
            
            if user in true_set:
                true_seens+=1
            else:
                true_negs+=1

            true_set.add(user)

        fprs[i]=(pred_seens-true_seens)/((pred_seens-true_seens)+true_negs)

    outfile='out.csv'

    textfile=open(outfile,'w')

    textfile.write('Time,FPR')

    for i in range(len(fprs)):
        textfile.write('\n{},{}'.format(i,fprs[i]))

    

    
