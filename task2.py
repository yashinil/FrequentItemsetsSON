#command to run the code
#python second2.py 20 50 ta-feng-grocery-dataset\ta_feng_all_months_merged.csv second.txt
from pyspark import SparkContext, SparkConf
import pyspark
import sys
import time
import math
import itertools
import csv

start_time=time.time()

#creating a spark context
sc = SparkContext('local[*]','second')
sc.setLogLevel('ERROR')

#take command line inputs
filter_threshold=int(sys.argv[1])
threshold=int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]

#reading the dataset into an RDD
largeRDD=sc.textFile(input_path).map(lambda line: line.split(',')).filter(lambda line: True if(line[0]!='"TRANSACTION_DT"') else False).map(lambda line: (line[0][1:-1]+"-"+line[1][1:-1],[line[5][1:-1].lstrip("0")])).persist(pyspark.StorageLevel.DISK_ONLY)
intermediateRDD=largeRDD.collect()

with open('intermediate.csv', 'w') as f:
    writer = csv.writer(f , lineterminator='\n')
    for tup in intermediateRDD:
        writer.writerow(tup)

#making user/business baskets
basketsRDD=largeRDD.reduceByKey(lambda a,b: a+b).filter(lambda line: True if len(line[1])>filter_threshold else False).persist(pyspark.StorageLevel.DISK_ONLY)

#toatl number of baskets
total_baskets=basketsRDD.count()

def apriori(iterator):
    baskets=[]
    baskets_in_partition=0
    itemset_size=1
    candidate=set()
    frequent_itemset=set()
    
    #get all the baskets
    for x in iterator:
        baskets.append(x[1])
    #get candidate frequent items of size one
    for basket in baskets:
        for item in basket:
            frequent_itemset.add((item,))
    
    frequent_itemset=list(frequent_itemset)
    #print(frequent_itemset)
    baskets_in_partition=len(baskets)
    reduced_threshold=threshold*(baskets_in_partition/total_baskets)
    reduced_threshold=math.floor(reduced_threshold)
    while len(frequent_itemset)!=0:  
        #print(frequent_itemset) 
        frequency=dict() 
        for basket in baskets: 
            basket=set(basket)
            for tpl in frequent_itemset:
                flag=1
                for item in tpl:
                    if item not in basket:
                        flag=0
                if flag==1:
                    if tpl in frequency:
                        frequency[tpl]+=1
                    else:
                        frequency[tpl]=1
        frequent_itemset.clear()
        for i in frequency:
            if frequency[i]>=reduced_threshold:
                candidate.add((i,1))
                frequent_itemset.append(i)
        
        frequent_itemset=set([item for t in frequent_itemset for item in t])
        itemset_size+=1
        frequent_itemset=list(itertools.combinations(frequent_itemset,itemset_size))
    
        temp=[]
        for tpl in frequent_itemset:
            temp1=list(tpl)
            temp1.sort()
            temp1=tuple(temp1)
            temp.append(temp1)
        frequent_itemset=temp
        #print(frequent_itemsets)
        #print(candidate)
        #print("************************************")
    return candidate

candidates=basketsRDD.mapPartitions(apriori).reduceByKey(lambda a,b: 1).map(lambda line: line[0]).collect()
candidates.sort()

can_output=dict()
for tpl in candidates:
    if len(tpl) in can_output:
        can_output[len(tpl)]=can_output[len(tpl)]+[tpl]
    else:
        can_output[len(tpl)]=[tpl]

print("candidates are:")
print(can_output)
print("__________________________________________________")

#definining function for findng the actual frequent pairs
def apriori_1(iterator):
    frequency=dict()
    frequent_itemset=set()
    for x in iterator:
        basket=set(x[1])
        for tpl in candidates:
            flag=1
            for item in tpl:
                if item not in basket:
                    flag=0
            if flag==1:
                if tpl in frequency:
                    frequency[tpl]+=1
                else:
                    frequency[tpl]=1
    for i in frequency:
        frequent_itemset.add((i,frequency[i]))
    #print(frequent_itemset)
    #print("************************************")
    return frequent_itemset

frequent_itemsets=basketsRDD.mapPartitions(apriori_1).reduceByKey(lambda a,b:a+b).filter(lambda line: True if line[1]>=threshold else False).map(lambda line: line[0]).collect()
frequent_itemsets.sort()
final_output=dict()
for tpl in frequent_itemsets:
    if len(tpl) in final_output:
        final_output[len(tpl)]=final_output[len(tpl)]+[tpl]
    else:
        final_output[len(tpl)]=[tpl]
print("frequent itemsets are:")
print(final_output)
print("__________________________________________________")

out_file = open(output_path,"w") 
out_file.write("Candidates:\n")
for size in can_output:
    if size==1:
        for item in can_output[size][:-1]:
            temp=str(item).replace(",","")
            out_file.write(temp+",")
        temp=str(can_output[size][-1]).replace(",","")
        out_file.write(temp)
    else:
        for item in can_output[size][:-1]:
            out_file.write(str(item)+",")
        out_file.write(str(can_output[size][-1]))
    out_file.write("\n\n")
out_file.write("Frequent Itemsets:\n")
for size in final_output:
    if size==1:
        for item in final_output[size][:-1]:
            temp=str(item).replace(",","")
            out_file.write(temp+",")
        temp=str(final_output[size][-1]).replace(",","")
        out_file.write(temp)
    else:
        for item in final_output[size][:-1]:
            out_file.write(str(item)+",")
        out_file.write(str(final_output[size][-1]))
    if(size!=len(final_output)):
        out_file.write("\n\n")

out_file.close() 
print("Duration: %s" % (time.time() - start_time))
