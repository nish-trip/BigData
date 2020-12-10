import statistics
import sys
import matplotlib.pyplot as plt
# file name from command line
fileName=sys.argv[1]
#lists for storing tasks and completion times respectively
time_list=list()
task_list=list()
dict_analysis={}

# opening file as f
f = open(fileName,"r")
for line in f:
    line=line.strip("\n")     
    line = line.split(",")   
    task = line[0].split(':')[1]     
    time = float(line[1])            
    if(task not in dict_analysis.keys()):  
        dict_analysis[task]=list()
        dict_analysis[task].append(time)
    else:
        try:
            if dict_analysis[task][1]<time : 
                dict_analysis[task][1]=time
        except Exception as e:
            dict_analysis[task].append(time)       
# looping through key-value pairs of analysis dictionary
for key ,value in dict_analysis.items():    
    print(key,value) # print the pair
sum =0 # initialising the sum to 0
for t in time_list: # running a loop to increment the sum
    sum = sum + t

#calulate Mean 
time_mean = sum/len(time_list)
print(time_mean)
# calculate median
time_median = statistics.median(time_list)

print(time_median)
#PLotting the graph 
plt.plot(task_list,time_list) 
plt.xlabel('Tasks')
plt.ylabel('Completion Time')
plt.title('Task Completion TIme')
#Displaying the graph
plt.show()  
