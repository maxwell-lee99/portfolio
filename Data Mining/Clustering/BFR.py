from pyspark import SparkContext
import itertools
from collections import defaultdict
import math
from datetime import datetime
import numpy as np
from sklearn.cluster import KMeans

def init_kmeans(initial):

    X=[]
    for row in initial:
        X.append(row[1][0][2:])
    
    

    kmean=KMeans(n_clusters=n_clusters*5).fit(X)
    labels=kmean.labels_
    

    result=[]
    for i in range(len(initial)):
        result.append((labels[i],[initial[i][1]]))
    return result

def small_kmeans(multi,n_clusters):
    X=[]
    for row in multi:
        for item in row[1][0][1]:
            
            X.append(item[0][2:])
        
    n_clusters=min(n_clusters,len(X))

    result=[]

    if len(X)>0:

        if len(X)==1:
            X=np.array(X).reshape(1,-1)

        kmean=KMeans(n_clusters=n_clusters,n_init=100).fit(X)
        labels=kmean.labels_
        
        
        i=0
        
        for row in multi:
            for item in row[1][0][1]:
                result.append((labels[i],(item[0],)))
                i+=1
    
    return result

def generate_statistics(group,x,DS):
    result=defaultdict(list)
    if DS:
    
        for cluster in group:
            stats=[]
            
            stats.append(len(cluster[1]))
            sums=[]
            sumsqs=[]
            for i in range(2,len(cluster[1][0])):
                sum0=0
                sumsq=0
                for point in cluster[1]:
                    sum0+=point[i]
                    sumsq+=point[i]**2
                sums.append(sum0)
                sumsqs.append(sumsq)
            stats.append(sums)
            stats.append(sumsqs)
            result[cluster[0]]=stats

    else:
        
        for cluster in group:
            
            stats=[]
            
            stats.append(len(cluster[1][1]))
            sums=[]
            sumsqs=[]
            for i in range(2,len(cluster[1][1][0])):
                sum0=0
                sumsq=0
                for point in cluster[1][1]:
                    sum0+=point[i]
                    sumsq+=point[i]**2
                sums.append(sum0)
                sumsqs.append(sumsq)
            stats.append(sums)
            stats.append(sumsqs)
            result[cluster[1][0]+x*10000]=stats
        

    return result

def clean_DS(DS):
    clusters=defaultdict(list)
    for row in DS:
        
        for point in row[1]:
            
            clusters[row[0]].append((point[0],point[1]))

    

    return clusters

def final_kmean(points,n_clusters):

    
    X=[]
    for point in points:
        
        X.append(point[2:])

    n_clusters=min(n_clusters,len(X))

    result=[]

    if len(X)>0:

        if len(X)==1:
            X=np.array(X).reshape(1,-1)

        kmean=KMeans(n_clusters=n_clusters,n_init=100).fit(X)
        labels=kmean.labels_
        
        
        i=0

        for point in points:
            result.append((labels[i],(point,)))
            i+=1

    return result
        
if __name__ == '__main__':
    
    init_time=datetime.now()

    outfile='out.txt'

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")

    small=False

    n_clusters=10

    if small:
        train='hw6/small_data.txt'
    else:
        train='hw6/data.txt'
    
    rdd=sc.textFile(train)

    rdd=rdd.map(lambda row: ([float(item) for item in row.split(',')]))

    hashed=rdd.map(lambda row: (int(row[0])%5,(row,)))
    
    initial=hashed.filter(lambda row: row[0]==0)

    in_kmeans=sc.parallelize(init_kmeans(initial.collect()))


    lens=in_kmeans.reduceByKey(lambda a,b: a + b).map(lambda row: (len(row[1]),(row,)))

    RS=lens.filter(lambda row: row[0]==1)

    

    multi=lens.filter(lambda row: row[0]>1)

    smaller_kmeans=small_kmeans(multi.collect(),n_clusters)

    

    kmean_result=sc.parallelize(smaller_kmeans)

    DS=kmean_result.reduceByKey(lambda a,b: a+b)

    

    DS_clean=clean_DS(DS.collect())

    
    

    
    statistics=generate_statistics(DS.collect(),10,True)

    
    
    DS_points=0
    for key, value in DS_clean.items():
        DS_points+=len(value)
    
    
    smaller_kmeans_RS=small_kmeans(RS.collect(),n_clusters*5)

    smaller_kmeans_RS=sc.parallelize(smaller_kmeans_RS)

    lens_RS=smaller_kmeans_RS.reduceByKey(lambda a,b: a + b).map(lambda row: (len(row[1]),row))

    still_RS=lens_RS.filter(lambda row: row[0]==1).map(lambda row: row[1][1][0]).collect()

    CS=lens_RS.filter(lambda row: row[0]>1).collect()

    

    CS_clusters=len(CS)

    CS_points=0

    if CS_clusters>0:
        
        for cluster in CS:
            for point in cluster[1][1]:
                CS_points+=0
    
    RS_points=len(still_RS)

    
    

    

    
    
    CS_statistics=generate_statistics(CS,0,False)

    CS_dict=defaultdict(list)

    for cluster in CS:
        
        for point in cluster[1][1]:
            CS_dict[cluster[1][0]].append((point[0], point[1]))
    
    

    #END OF INITIALIZATION

    textFile=open(outfile,'w')
    textFile.write('The intermediate results:\nRound 1: {},{},{},{}'.format(DS_points,CS_clusters,CS_points,RS_points))


    
    for x in range(1,5):
    
        rdd=hashed.filter(lambda row: row[0]==x).map(lambda row: row[1][0])
        
        for row in rdd.collect():
            
            #TRY TO PUT IN DS CLUSTER
            
            min_d=math.inf
            
            for key,stat in statistics.items():
                
                d=0
                for i in range(len(stat[1])):
                    num=row[i+2]-(stat[1][i]/stat[0])
                    denom=math.sqrt((stat[2][i]/stat[0]-((stat[1][i]/stat[0])**2)))
                    
    
                    d+=((num/denom)**2)
                d=math.sqrt(d)
                
                if d<min_d:
                    min_d=d
                    min_d_cluster=key
                
            if min_d < .001*(len(stat[1])**(1/2)):
                
                DS_clean[min_d_cluster].append((row[0],row[1]))
                statistics[min_d_cluster][0]+=1
                for i in range(len(stat[1])):
                    
                    statistics[min_d_cluster][1][i]+=row[i+2]
                    statistics[min_d_cluster][2][i]+=(row[i+2]**2)
            
            else:
                #TRY TO PUT IN CS CLUSTER
                min_d=math.inf
                for key,stat in CS_statistics.items():
                    
                    d=0
                    
                    for i in range(len(stat[1])):
                        num=row[i+2]-(stat[1][i]/stat[0])
                        
                        denom=math.sqrt((stat[2][i]/stat[0]-((stat[1][i]/stat[0])**2)))
                        
        
                        d+=(num/denom)**2
                    d=math.sqrt(d)
                    if d<min_d:
                        min_d=d
                        min_d_cluster=key
                    
                if min_d < 2*(len(stat[1])**(1/2)):
                    CS_dict[min_d_cluster].append((row[0],row[1]))
                    
                    
                    CS_statistics[min_d_cluster][0]+=1
                    for i in range(len(stat[1])):
                        
                        CS_statistics[min_d_cluster][1][i]+=row[i+2]
                        CS_statistics[min_d_cluster][2][i]+=(row[i+2]**2)

                else:
                    #PUT IN RS GROUP
                    still_RS.append(row)
                
        #CLUSTER RS GROUP
        kmean_result=final_kmean(still_RS,n_clusters=n_clusters*5)

        kmean_result=sc.parallelize(kmean_result)

        lens_RS=kmean_result.reduceByKey(lambda a,b: a + b).map(lambda row: (len(row[1]),row))

        still_RS=lens_RS.filter(lambda row: row[0]==1).map(lambda row: row[1][1][0]).collect()

        new_CS=lens_RS.filter(lambda row: row[0]>1).collect()

        

        new_CS_statistics=generate_statistics(new_CS,x,False)

        #SET UP NEW CS CLUSTERS

        for key,value in new_CS_statistics.items():
            CS_statistics[key]=value
        
       

        for cluster in new_CS:
        
            for point in cluster[1][1]:
                CS_dict[cluster[1][0]+x*10000].append((point[0], point[1]))

        #MERGE CS CLUSTERS

        keys=[key for key in CS_statistics.keys()]

        key_combos=[pair for pair in itertools.combinations(keys,2)]
        
        for combo in key_combos:
            if combo[0] in CS_statistics.keys() and combo[1] in CS_statistics.keys():
                stat1=CS_statistics[combo[0]]
                stat2=CS_statistics[combo[1]]
                
                d=0
                
                means=[]
                for i in range(len(stat1[1])):
                    var1=stat1[2][i]/stat1[0]-((stat1[1][i]/stat1[0])**2)
                    var2=stat2[2][i]/stat2[0]-((stat2[1][i]/stat2[0])**2)
                    mean1=stat1[1][i]/stat1[0]
                    mean2=stat2[1][i]/stat2[0]
                    
                    combined_mean=(stat1[1][i]+stat2[1][i])/(stat1[0]+stat2[0])
                    
                    d+=((mean1-mean2)/(math.sqrt(var1)))**2
                    
                d=math.sqrt(d)
                
                
                if d<2*math.sqrt(len(stat1[1])):
                    
                    new_stat=[]
                    new_stat.append(stat1[0]+stat2[0])
                    new_sums=[]
                    new_sumsq=[]
                    for j in range(len(stat1[1])):
                        new_sums.append(stat1[1][j]+stat2[1][j])
                        new_sumsq.append(stat1[2][j]+stat2[2][j])
                    new_stat.append(new_sums)
                    new_stat.append(new_sumsq)
                    
                    for point in CS_dict[combo[1]]:
                        CS_dict[combo[0]].append(point)
                    del CS_dict[combo[1]]
                    
                    CS_statistics[combo[0]]=new_stat
                    del CS_statistics[combo[1]]

                
        #OUTPUT RESULTS
        DS_points=0
        for value in DS_clean.values():
            for point in value:
                DS_points+=1
        CS_clusters=len(CS_dict)
        CS_points=0
        for value in CS_dict.values():
            for point in value:
                CS_points+=1
        RS_points=len(still_RS)
        if x<4:
            textFile.write('\nRound {}: {},{},{},{}'.format(x+1,DS_points,CS_clusters,CS_points,RS_points))

            
    for key1,stat1 in CS_statistics.items():
        min_d=math.inf
        min_d_cluster=math.inf
        for key2,stat2 in statistics.items():
            d=0
            for i in range(len(stat1[1])):
                var1=stat1[2][i]/stat1[0]-((stat1[1][i]/stat1[0])**2)
                var2=stat2[2][i]/stat2[0]-((stat2[1][i]/stat2[0])**2)
                mean1=stat1[1][i]/stat1[0]
                mean2=stat2[1][i]/stat2[0]
                
                d+=((mean1-mean2)/math.sqrt(var2))**2
            d=math.sqrt(d)
            
            if d<min_d:
                min_d=d
                min_d_cluster=key2
        if min_d<2*math.sqrt(len(stat1[1])):
            for point in CS_dict[key1]:
                DS_clean[min_d_cluster].append(point)
            del CS_dict[key1]

    DS_points=0
    for value in DS_clean.values():
        for point in value:
            DS_points+=1
    CS_clusters=len(CS_dict)
    CS_points=0
    for value in CS_dict.values():
        for point in value:
            CS_points+=1
    RS_points=len(still_RS)

    textFile.write('\nRound {}: {},{},{},{}'.format(x+1,DS_points,CS_clusters,CS_points,RS_points))

    #ASSIGNING CLUSTERS
    cluster_assignments=[]
    for value in DS_clean.values():
        cluster_groups=defaultdict(int)
        for point in value:
            
            cluster_groups[point[1]]+=1
        key=max(cluster_groups,key=cluster_groups.get)
        for point in value:
            cluster_assignments.append((int(point[0]),int(key)))
    for value in CS_dict.values():
        cluster_groups=defaultdict(int)
        for point in value:
            cluster_groups[point[1]]+=1
        key=max(cluster_groups,key=cluster_groups.get)
        for point in value:
            cluster_assignments.append((int(point[0]),int(key)))
    for point in still_RS:
        cluster_assignments.append((int(point[0]),-1))

    sorted_cluster=sorted(cluster_assignments,key= lambda row: row[0])
    
    
    textFile.write('\n\nThe clustering results:')
    for point in sorted_cluster:
        textFile.write('\n{},{}'.format(point[0],point[1]))

    






    

    
