from pyspark import SparkContext
import sys
import itertools
from collections import defaultdict, Counter
import math
from datetime import datetime
from statistics import mean
from graphframes import *
from pyspark.sql.functions import col, lit, when
from pyspark.sql import SparkSession

def bfs(node,graph):

    visited=defaultdict(int)
    shortest_paths=defaultdict(lambda:1)
    for key in graph.keys():
        shortest_paths[key]

    queue=[node]
    visited[node]
    
    while queue:
        current=queue.pop(0)
        for neighbor in graph[current]:
            if neighbor not in visited:
                visited[neighbor] = visited[current]+1
                queue.append(neighbor)
            else:
                if visited[neighbor]==visited[current]+1:
                    shortest_paths[neighbor]+=1
    
    weights=defaultdict(lambda:1)
    edge_weights=defaultdict(int)

    level=max(visited.values())
    bottom_leaves=[k for k,v in visited.items() if v == level]
    for leaf in bottom_leaves:
        weights[leaf]
    while level>0:
        upper_leaves=[k for k,v in visited.items() if v == level-1]
        for bottom_leaf in bottom_leaves:
            count=0
            matches=[]
            for upper_leaf in upper_leaves:
                if upper_leaf in graph[bottom_leaf]:
                    count+=1
                    matches.append(upper_leaf)
            total_shortest_paths=sum([shortest_paths[match] for match in matches])
            for upper_leaf in matches:
                
                edge_weights[tuple(sorted((bottom_leaf,upper_leaf)))]+=weights[bottom_leaf]*shortest_paths[upper_leaf]/total_shortest_paths
                weights[upper_leaf]+=weights[bottom_leaf]*shortest_paths[upper_leaf]/total_shortest_paths
        
        level+=-1
        bottom_leaves=upper_leaves
        
    return edge_weights

def assign_communities(graph,users,user_dict):
    
    degrees=[]
    for user in users:
        degrees.append((user,len(graph[user])))

    degrees=sorted(degrees,key=lambda row: row[1])
    order=[row[0] for row in degrees]

    communities={}
    new_community=0 
    queue=[]

    user=order.pop(0)
    more_community=True

    while more_community:
        
        community_count=defaultdict(int)

        for pair in graph[user]:
            try:
                comm=communities[pair]
                community_count[comm]+=1
            except:
                pass
            if pair in order and pair not in queue:
                queue.append(pair)
        
        if len(community_count)>0:
            communities[user]=max(community_count, key=community_count.get)
        else:
            communities[user]=new_community
            new_community+=1
        
        if len(order)>0:
            if len(queue)>0:
                user=queue.pop(0)
                order.remove(user)
                
            else:
                user=order.pop(0)
                
        else:
            more_community=False
    res = defaultdict(list)
    for key, val in sorted(communities.items()):
        res[val].append(user_dict[key])
    communities=[]
    for item in res.values():
        communities.append(item)
    return communities

def get_modularity(communities,A,k,m):
    modularity=0
    for community in communities:
        edge_combos=[combo for combo in itertools.combinations(community,2)]
        for combo in edge_combos:
            modularity+=A[combo[0]][combo[1]]-k[combo[0]][1]*k[combo[1]][1]/(2*m)
    modularity=modularity/(2*m)
    return modularity

def remove_edge(graph, betweenness):
    remove_edges=[k for k,v in betweenness.items() if v == max(betweenness.values())]
    
    for edge in remove_edges:
        graph[edge[0]].remove(edge[1])
        graph[edge[1]].remove(edge[0])
    
    return graph


def get_betweenness(graph,users):

    betweenness=defaultdict(float)
        
    for user in users:
        result=bfs(user,graph)
        
        for key,value in result.items():
            betweenness[key]+=value/2
    return betweenness


if __name__ == '__main__':
    
    init_time=datetime.now()

    

    sc=SparkContext('local[*]','testReview')
    spark=SparkSession(sc)
    sc.setLogLevel("WARN")

    filter=int(sys.argv[1])
    train=sys.argv[2]
    outfile1=sys.argv[3]
    outfile2=sys.argv[4]

    rdd = sc.textFile(train)
    
    header=rdd.first()
    no_header=rdd.filter(lambda row: row!=header)

    users=no_header.map(lambda row: row.split(',')[0]).distinct().collect()

    graph=defaultdict(list)
    for user in users:
        graph[user]=[]
    

    user_sets=no_header.map(lambda row: (row.split(',')[0],frozenset([row.split(',')[1]])))\
        .reduceByKey(lambda a,b: a.union(b)).collect()

    count=0
    edges=[]

    for user in user_sets:
        count+=1
        if count<=len(user_sets):
            for second_user in user_sets[count:]:
                if len(user[1].intersection(second_user[1]))>=filter:
                    edges.append(sorted((user[0],second_user[0])))
    

    for edge in edges:
        graph[edge[0]].append(edge[1])
        graph[edge[1]].append(edge[0])

    

    betweenness=defaultdict(float)
        
    for user in users:
        result=bfs(user,graph)
        
        for key,value in result.items():
            betweenness[key]+=value/2

    all_edges=[(key,value) for key,value in betweenness.items()]
    
    
    output_edges=sorted(all_edges,key=lambda row: (-row[1],row[0][0],row[0][1]))

    outfile='out.txt'

    textFile=open(outfile1,'w')

    first=True
    for row in output_edges:
        if first:
            textFile.write("('{}','{}'),{}".format(row[0][0],row[0][1],round(row[1],5)))
            first=False
        else:
            textFile.write("\n('{}','{}'),{}".format(row[0][0],row[0][1],round(row[1],5)))
    

    
    
    user_dict={}
    count=0
    for user in users:
        user_dict[user]=count
        count+=1
    

    A=[]
    k=[]
    for user in users:
        A.append([0 for user in users])
        k.append((user,len(graph[user])))
        for pair in graph[user]:
            A[user_dict[user]][user_dict[pair]]=1
    m=len(all_edges)

    best_modularity=-math.inf

    runtime=0

    while runtime<370:

    
        communities=assign_communities(graph,users,user_dict)

        new_modularity=get_modularity(communities,A,k,m)

        graph=remove_edge(graph,betweenness)

        betweenness=get_betweenness(graph,users)

        if new_modularity>best_modularity:

            best_communities=communities
            best_modularity=new_modularity

        runtime=(datetime.now()-init_time).total_seconds()

        


    final_communities=[]
        
    for community in best_communities:
        new_community=[]
        for item in community:
            new_community.append(users[item])

        final_communities.append((tuple(sorted(new_community)),len(new_community)))

    final_communities=sorted(final_communities, key=lambda x: (x[1],x[0][0]))

    textfile=open(outfile2,'w')
    first=True
    for row in final_communities:
        if first:
            first_id=True
            for user_id in row[0]:
                if first_id:
                    textfile.write("'{}'".format(user_id))
                    first_id=False
                else:
                    textfile.write(",'{}'".format(user_id))
            first=False
        else:
            first_id=True
            for user_id in row[0]:
                if first_id:
                        textfile.write("\n'{}'".format(user_id))
                        first_id=False
                else:
                    textfile.write(",'{}'".format(user_id)) 


    

    
    






















    print('Runtime',(datetime.now()-init_time).total_seconds())
