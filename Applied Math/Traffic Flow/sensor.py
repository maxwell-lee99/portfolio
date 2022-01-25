import numpy as np
import matplotlib.pyplot as plt

T=2000      #total time steps
rushhour=400
rushhour2=T-rushhour
K=10        #chunks for each road
I=16        #number of roads
cycle=20    #length of a green light cycle
p=np.zeros((K,T,I))     #this 3d array will hold the density of the ith road at the kth position at time t. dumb that i didn't write them in that order
x=np.ones(K)*.1      #this will be the initial densities on the road, assigned later

deltat=.1
deltax=.2       #deltat and deltax chosen to get smoothness, not important bc not fitting to data
a=np.zeros((I,I,T+1))     #this 3d array will represent the traffic light of the ith road moving to the jth road at time t, 1 for green 0 for red
pmax=1
popt=.5
velocity=.5      #maximum and optimal densities along with the constant velocity

decidetoturnleft=.2         #these represent the proportion of density that decides to make the decision at junction
straightorright=1-decidetoturnleft
left=1
right=.3
straight=1-right

d=np.zeros((I,I))       #this will appropriately assign the previous decision proportions for the ith road moving to the jth road. if that decision is not possible, this stays 0
d[12,0]=decidetoturnleft
d[12,1]=straightorright
d[0,11]=left
d[1,9]=right
d[1,10]=straight
d[13,2]=decidetoturnleft
d[13,3]=straightorright
d[2,8]=left
d[3,10]=right
d[3,11]=straight
d[14,4]=decidetoturnleft
d[14,5]=straightorright
d[4,9]=left
d[5,11]=right
d[5,8]=straight
d[15,6]=decidetoturnleft
d[15,7]=straightorright
d[6,10]=left
d[7,8]=right
d[7,9]=straight


#the following for loop assigns traffic light values according to the cycles
for t in range(0,T):
    #the following "traffic lights" are always turned on bc they are people choosing to move into the left turn lane or the straight/right lane
    a[12,0,t]=1
    a[12,1,t]=1
    a[13,2,t]=1
    a[13,3,t]=1
    a[14,4,t]=1
    a[14,5,t]=1
    a[15,6,t]=1
    a[15,7,t]=1
    # cyclenum=(t//cycle)%4
    # if cyclenum == 0:
    #     #a cycle of two opposing left turn lights
    #     a[0,11,t]=1
    #     a[4,9,t]=1
    #
    # elif cyclenum == 1:
    #     #a cycle of two opposing straight/right lights
    #     a[1,9,t]=1
    #     a[1,10,t]=1
    #     a[5,8,t]=1
    #     a[5,11,t]=1
    # elif cyclenum == 2:
    #     #a cycle of two opposing left turn lights
    #     a[6,10,t]=1
    #     a[2,8,t]=1
    # else:
    #     #a cycle of two opposing straight/right lights
    #     a[7,9,t]=1
    #     a[7,8,t]=1
    #     a[3,10,t]=1
    #     a[3,11,t]=1

#the following lists hold the roads that represent a road containing an endpoint or an entry point on the boundary of the map
#inward boundaries used for density entering the system, outward boundaries for density leaving the system
inwardboundaries=[12,13,14,15]
outwardboundaries=[8,9,10,11]

#assigning constant initial density along the inward boundaries, a completely arbitrary choice
for i in range(I):
    p[:,0,i]=x

#basic triangular flow function
def piece(p):
    if p <= popt:
        return velocity*p
    else:
        return velocity*(2*popt-p)
'''
the following is the most complicated function. gammaout represents the flow at the end (k=K-1) of a road i at time t, 
if the road i is an outward boundary road, we set the flow to be what it would normally be with the piece function
if the road i is not an outward boundary road, we use the following logic:
we first find the maximum possible flow, call this flow in, that could leave road i at the end point
if the density is less than the optimal density, then only the current flow could leave
if the density is greater than the optimal density, the optimal flow could leave
finally, if the traffic light is red then obviously no density can leave
we then look at the flow, call this flowin, that can be accepted at each of the points i is trying to go to, call that road j
if the density at the beginning point k=0 of road j is less than the optimal density, then it can accept the optimal flow
if the density is greater than the optimal density then it can accept its current flow
we then multiply the flowin number by 1/dij, the proportion of road i moving to road j to get our final flow in
gammaout is the minimum of the flowins and the flowout number
'''
def gammaout(p,t,i):
    if i not in outwardboundaries:
        values=[]
        js = [j for j in range(0, I) if d[i, j] > 0]
        if p[-1,t-1,i]<= popt:
            fout=piece(p[-1,t-1,i])
        else:
            fout=velocity*popt
        values.append(a[i,js[0],t-1]*fout)
        for j in js:
            if p[0, t - 1, j] <= popt:
                fin = (1/d[i,j])*velocity * popt
            else:
                fin = (1/d[i,j])*piece(p[0, t - 1, j])
            values.append(fin)
        return min(values)
    if i in outwardboundaries:
        return piece(p[-1,t-1,i])
'''
gammain is a similar function to gammaout, representing the flow in to road i
if road i is an inward boundary, we can arbitrarily assign the flow of traffic
if it is not, it is the sum of the gammaouts into the road i
we use the list js here to represent each road j that flows into road i
we then call gammaout for the function to see what the flow out of road i is
we then multiply that by dji representing the proportion of road j that enters road i
we do this for each road j coming into road i and sum the result
this guarantees conservation of flow
'''

def gammain(p,t,i):
    if i in inwardboundaries:
        if i == 12:
            if t < rushhour:
                return np.random.rand()/2.5
            else:
                return np.random.rand()/10
        elif i == 14:
            if t < rushhour2:
                return np.random.rand()/10
            else:
                return np.random.rand() / 2.5
        else:
            return np.random.rand() / 10
    else:
        sum=0
        js=[j for j in range(0, I) if d[j,i] > 0]
        for j in js:
            sum=sum+d[j,i]*gammaout(p,t,j)

        return sum

'''
this for loop is the main function
this finds the density of the ith road at the kth point at time t
it looks at the surrounding densities and flows at the time t-1
'''

def assignlight(i,t):
    if i == 0 or i == 4:
        a[0,11,t+1:t+cycle+1]=1
        a[4, 9, t + 1:t + cycle + 1] = 1
    elif i == 1 or i == 5:
        a[1, 10, t + 1:t + cycle + 1] = 1
        a[1, 9, t + 1:t + cycle + 1] = 1
        a[5, 8, t + 1:t + cycle + 1] = 1
        a[5, 11, t + 1:t + cycle + 1] = 1
    elif i == 6 or i == 2:
        a[6, 10, t + 1:t + cycle + 1] = 1
        a[2, 8, t + 1:t + cycle + 1] = 1
    else:
        a[7, 9, t + 1:t + cycle + 1] = 1
        a[7, 8, t + 1:t + cycle + 1] = 1
        a[3, 11, t + 1:t + cycle + 1] = 1
        a[3, 10, t + 1:t + cycle + 1] = 1

for t in range(1,T):
    for k in range(0,K):
        for i in range(0,I):
            if k == 0:
                p[k,t,i]=max([min([1/4*(3*p[k,t-1,i]+p[k+1,t-1,i])-.5*deltat/deltax*(piece(p[k+1,t-1,i])+piece(p[k,t-1,i])-2*gammain(p,t,i)),pmax]),0])
            elif k==1:
                p[k, t, i] = max([min([1/4*(p[k-1,t-1,i]+2*p[k,t-1,i]+p[k+1,t-1,i])-.5*deltat/deltax*(piece(p[k+1,t-1,i])-gammain(p,t,i)),pmax]),0])
            elif k==K-1:
                p[k, t, i] = max([min([1/4*(p[k-1,t-1,i]+3*p[k,t-1,i])-.5*deltat/deltax*(2*gammaout(p,t,i)-piece(p[k,t-1,i])-piece(p[k-1,t-1,i])),pmax]),0])
            elif k == K-2:
                p[k, t, i] = max([min([1/4*(p[k-1,t-1,i]+2*p[k,t-1,i]+p[k+1,t-1,i])-.5*deltat/deltax*(gammaout(p,t,i)-piece(p[k-1,t-1,i])),pmax]),0])
            else:
                p[k,t,i]=max([min([1/4*(p[k-1,t-1,i]+2*p[k,t-1,i]+p[k+1,t-1,i])-.5*deltat/deltax*(piece(p[k+1,t-1,i])-piece(p[k-1,t-1,i])),pmax]),0])


    maximum = -1
    maxlist = []
    checkpoint=-3
    if t % cycle == 0:

        for i in [0,1,2,3,4,5,6,7]:

            if p[checkpoint,t,i] > maximum:
                maximum=p[checkpoint,t,i]
                maxlist=[i]
            elif p[checkpoint,t,i] ==maximum:
                maxlist.append(i)
        rng=np.random.randint(len(maxlist))
        maxpoint=maxlist[rng]
        assignlight(maxpoint,t)



tscale=[t for t in range(0,T)]


#using a for loop to plot graphs that i am interested in checking, currently plotting the 6th road at its start and end points with
#its traffic light graph also displayed
count=0

for i in [1,5,3,7]:
    for k in [-1]:
        count=count+1
        plt.figure(count)
        plt.plot(tscale,p[k,:,i])
        js = [j for j in range(0, I) if d[i, j] > 0]
        for j in js:
            plt.plot(tscale,a[i,j,:-1])
        string='Density of the road {} at the point {}'.format(i,k)
        plt.title(string)
        plt.xlabel('Time')
        plt.ylabel('Density')


plt.show()

# plt.figure(1)
# plt.plot(tscale,p[0,:,7])
#
# plt.figure(2)
# plt.plot(tscale,p[-1,:,15])
#
# plt.figure(3)
# plt.plot(tscale,p[0,:,6])
