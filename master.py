#!/usr/bin/python
import sys
import json
import socket
import random
import time
import threading
import datetime 

#Setting up TCP connection
class Tcp:                        # initialising the TCP socket
    def __init__(self,port):
        self.port = port                         #port number from the passed argument
        self.ip = "localhost"               # localhost for the IP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                    # creating a TCP socket using socket.SOCK_stream
        self.sock.bind((self.ip, self.port))                                    #binding the socket to the IP addr and the port                   
        self.sock.listen(1)                          #listen to the port for incoming requests
        self.jobQueue=list()                    # list for recieved job


    #definition to start the server
    def server_start(self):
        
        #keep the server running
        while True:
                print(f'waiting for connection at port number :{self.port}')
                #accept requests
                connection, clientAddress = self.sock.accept()
                #receiving the job from the connection made
                job=connection.recv(1024)
                self.jobQueue.append(job)
                print(f'job {job} receives from {clientAddress} through port {self.port} ')
                #closing connection
                connection.close() 
                #decoding message     
                decoded_str=job.decode('utf-8')
                #convert decoded message to python object(dict) using json.loads
                analyticMessage=json.loads(decoded_str)
                try:
                    fwrite="received:"+str(analyticMessage['job_id'])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                    f.write(fwrite)
                    
                except Exception as e:
                    print('')

                popped_job=self.jobQueue.pop(0)     
                if(self.port==5000):  
                    
                    message=popped_job.decode('utf-8') 
                    message=json.loads(message)
                    job_contents[message['job_id']]=list()
                    task_list=list()
                    for i in message['map_tasks']:
                        task_list.append([i['task_id'],i['duration'],False])
                    job_contents[message['job_id']].append(task_list)                                    #append the map tasks to a list
                    task_list=list()
                    for i in message['reduce_tasks']:
                        task_list.append([i['task_id'],i['duration'],False,False])
                    job_contents[message['job_id']].append(task_list)                                    #append the reducer tasks
                    
                    job_contents[message['job_id']].append(False)                               #boolean value indicates if we should start reducer task or it is completed
                    job_contents[message['job_id']].append(False)
                    i=0
                    while(i<len(job_contents[message['job_id']][0])):                   
                        temp_worker=None                     # checking for the algorithm used
                        if(schedule_type=='RANDOM'):
                            temp_worker=schedulingRandom()
                        elif(schedule_type=='RR'):
                            temp_worker=schedulingRound()
                        elif(schedule_type=='LL'):
                            temp_worker=schedulingLeast()

                        if(temp_worker!=None):
                            
                            if(temp_worker.avaSlots>0):              #check for available slots
                                send_request(job_contents[message['job_id']][0][i],temp_worker)
                                i+=1
     
                if(self.port==5001):                         #decodeing the received message
                    message=popped_job.decode('utf-8') 
                    message=json.loads(message)             #converting message to a dictionary 
                    lock_2.acquire()                        #acquring locks to change available slots and slot jobs
                    globalWorkers[message['worker_id']].avaSlots=message['avaSlots']
                    globalWorkers[message['worker_id']].slotJobs[message['slot_id']]=message['slotJobs']
                                                            #releasing the locks of jobs
                    lock_2.release()
                    
                    
                    a=message['jobCompleted']               
                    #checking for mapper task
                    if(a.split('_')[1][0]=='M'):                     #updating global job list  
                        for i in job_contents[a.split('_')[0]][0]:
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                           
                        reducer_start=False                          #checking if reducer can start 
                        for i in job_contents[a.split('_')[0]][0]:
                            if(i[2]==False):
                                reducer_start=False
                                break
                            else:
                                reducer_start=True
                                
                        if(reducer_start):
                            lock_4.acquire()
                            job_contents[a.split('_')[0]][2]=True            #showing reducer tasks have been started in global job list
                            lock_4.release()

                    
                    elif(a.split('_')[1][0]=='R'):
                        for i in job_contents[a.split('_')[0]][1]:              # updating global job list if task is a reducer
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                        completeReducer=False
                        for i in job_contents[a.split('_')[0]][1]:
                            if(i[2]==False):
                                completeReducer=False
                                break
                            else:
                                completeReducer=True

                        if(completeReducer):
                            lock_3.acquire()                        # acquiring lock
                            job_contents[a.split('_')[0]][3]=True               #updating global list that reducer tasks have been completed 
                            lock_3.release()                            #releasing lock
                            fwrite="completed:"+str(a.split('_')[0])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                            f.write(fwrite)
                    else:
                        print('Invalid task received')
                
#definition of worker class
class Worker:
    def __init__(self,portNo,worker_id,slots):
        #port no
        self.portNo=portNo 
        #worker_id
        self.worker_id=worker_id
        #total number of slots
        self.slots=slots
        #total number of available slots
        self.avaSlots=slots
        #jobs running in each slot
        self.slotJobs=dict()
        #initialising slot jobs
        for i in range(1,self.slots+1):
            self.slotJobs[i]=[True,0,'']   #[slotAvailable,duration value,running task]


if __name__ == '__main__':
    def analysis_2():
        timeX=0
        while(True):
            for key, value in globalWorkers.items():
                writeContent="time:"+ str(timeX)+";"+ "worker_id:"+str(value.worker_id)+";"+"jobs_running:"+str(value.slots-value.avaSlots)+'\n'
                f1.write(writeContent)
            timeX+=1
            time.sleep(1)
 
    def schedulingRandom():                                            #function implementing random scheduling
        solWorker=random.choice(list(globalWorkers.values()))
        while True:
            lock_5.acquire()
            if(solWorker.avaSlots>0):
                
                solWorker.avaSlots-=1
                lock_5.release()
                return solWorker
            else:
                lock_5.release()
                time.sleep(1)
                solWorker=random.choice(list(globalWorkers.values())) 
                                                                   # function implementing round robin scheduling 
    def schedulingRound():
        while True:
            temp=sorted (globalWorkers.keys())
            for key in temp:
                
                print("Available Slots= ",globalWorkers[key].avaSlots)
                lock_5.acquire()
                if(globalWorkers[key].avaSlots>0):
                    
                    globalWorkers[key].avaSlots-=1
                    lock_5.release()
                    
                    return globalWorkers[key]
                else:
                    lock_5.release()
                    time.sleep(1)

    def schedulingLeast():                                          #function implementing least loaded scheduling
        temp=list(globalWorkers.values())
        while True:

            maxAva=temp[0].avaSlots
            solWorker=temp[0]
            for i in temp[1:]:
                if(i.avaSlots>maxAva):
                    maxAva=i.avaSlots
                    solWorker=i
            if(maxAva==0):
                time.sleep(1)
            else:
                lock_5.acquire()
                solWorker.avaSlots-=1
                lock_5.release()
                return solWorker   

    def reducer_start():
        while True:
            try:
                for key,value in job_contents.items():
                            if(not value[3]):   #check if all reducer tasks are completed
                                if(value[2]):
                                    count=0 # initialising count to 0
                                    while(count<len(value[1])):
                                        if(not value[1][count][3]):                                        
                                            reducerJob=value[1][count]
                                            temp_worker=None # initialising the worker to None
                                            # setting the temp worker to use the algorithm passed in command line
                                            if(schedule_type=='RANDOM'):
                                                temp_worker=schedulingRandom()
                                            elif(schedule_type=='RR'):
                                                temp_worker=schedulingRound()
                                            elif(schedule_type=='LL'):
                                                temp_worker=schedulingLeast()
                                            
                                            if(temp_worker!=None): # if valid command line argument is passed 
                                                if(temp_worker.avaSlots>0):
                                                    #changing value to true since task was sent
                                                    value[1][count][3]=True                                                  
                                                    send_request(reducerJob,temp_worker)
                                            break
                                            
                                        count+=1
            
            except Exception as e: # on exception print nothing
                print('')                         

    #helper function for sending reques
    def send_request(job,worker):
        lock_1.acquire() #acqure locks
        x=job # initise x to the job passed as argument
        job={"task_id":x[0],"duration":x[1]}
        for key,value in worker.slotJobs.items(): #for each worker we update the slots
            if(value[0]):
                value[0]=False
                value[1]=job["duration"]
                value[2]=job["task_id"]
                break
        lock_1.release() # release the lock
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #sending message
            s.connect(("localhost", worker.portNo)) # connecting 
            message=json.dumps(job)
            print(f'sending {message} to {worker.portNo}')   
            s.send(message.encode())

    #initialising locks
    lock_1=threading.Lock()
    lock_2=threading.Lock()
    lock_3=threading.Lock()
    lock_4=threading.Lock()
    lock_5=threading.Lock()
    
    #setting path and schedule type
    config_path= sys.argv[1]
    schedule_type=sys.argv[2]

    #opening config file
    with open(config_path) as f:
            data = json.load(f)
    
    workers=data['workers'] # list of workers from data
    globalWorkers=dict()


    for i in workers: #dictionary for portNo,worker_id,Slots
        globalWorkers[i['worker_id']]=Worker(i['port'],i['worker_id'],i['slots'])

    #opening log files to record job initial request time and job completion time        
    f=open("mlog.txt","a+",buffering=1)
    #log file tasks for each scheduling algorithm
    fileOneName=str(schedule_type)+"Logs.txt"
    f1=open(fileOneName,"a+",buffering=1)
        
    job_contents={}            # we initialse job contents to an empty dictionary               
    server_5000=Tcp(5000) #TCP server is initialised at port 5000 
    server_5001=Tcp(5001) #TCP server is initialised at port 5001
    threads = [threading.Thread(target=server_5000.server_start), threading.Thread(target=server_5001.server_start),threading.Thread(target=analysis_2),threading.Thread(target=reducer_start)]

    for thread in threads:
        thread.start()
        print(f'started thread {thread}')
        thread.join(0.1)