#!/usr/bin/python
import sys
import json
import socket
import threading
import time
import datetime 

#Setting up TCP connection
class TCPServer:
# init for TCP socket
    def __init__(self,port):
        # list for recieved jobs
        self.jobQueue= list()    
        # localhost for the IP
        self.ip = "localhost"
        # creating a TCP socket using socket.SOCK_stream
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port = port

#Starting the TCP server
    def startserver(self):
        # binding the IP addr and the port to the socket
        self.sock.bind((self.ip, self.port))
        # listening for requests
        self.sock.listen(1)
        while True:   
                print(f'waiting for connection at {self.port}')
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)     
                #receiving and displaying the job the connection we accepted
                print(f'port number {self.port} receives -> {job}')      
                #Accessing job parameters and loading into json
                temp_str=job.decode('utf-8')
                message=json.loads(temp_str)
                #acquiring a lock
                l_1.acquire()      
                # reducing the number of available slots
                workerClass.avaSolts-=1  
                while True:
                    count=0
                    for key,value in workerClass.slotJobs.items():      
                        if value[0]:
                            value[0]=False     
                            value[1]= int(message["duration"]) 
                            value[2]=message["task_id"] 
                            break 
                        else:
                            count+=1

                    if(count!=workerClass.noSlots):
                        break
                # releasing the lock
                l_1.release()    
                # closing the existing connection 
                connection.close()
                
                #sending info for logs analysis
                fileWrite="received:"+str(message['task_id'])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                print(fileWrite)
                f.write(fileWrite)  

#Creating a class for Worker
class Worker:
    # init function for worker class
    def __init__(self,portNo,workerId,noSlots):     
        self.portNo=portNo
        self.avaSolts=noSlots
        # creating an empty dictionary for the jobs
        self.slotJobs={}
        self.noSlots=noSlots
        self.workerId=workerId
        for i in range(1,self.noSlots+1):     
            self.slotJobs[i]=[True,0,'']

if __name__ == '__main__':

    # Replying with an acknowledgement to Master using 5001
    def send_request():
        while(True):
            #modifying slot variabels after completing tasks
            for key,value in workerClass.slotJobs.items():
                #checks if duration is 0 which means task is completed       
                if(value[1]==0 and value[2]!=''):
                    jobCompleted=value[2]   
                    #Append task to completed task list
                    l_2.acquire()     
                     #Consume another lock
                    workerClass.avaSolts+=1      
                    #Change the slot variable values for the worker
                    value[2]=''
                    value[0]=True           
                    l_2.release()     #Release the lock
                    #Add the time since epoch when task is over into a file 
                    fileWrite="completed:"+str(jobCompleted)+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                    f.write(fileWrite)                   
                    #Sending the job completion information to Master through socket port 5001
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect(("localhost", 5001))
                        message={"worker_id":workerClass.workerId,"avaSlots":workerClass.avaSolts,"slotJobs":value,"slot_id":key,"jobCompleted":jobCompleted}
                        print(f'sending {message} to {5001}') 
                        message=json.dumps(message)
                        s.send(message.encode())
                #Sleep for 1 second and decrease the duration  of the task if task is not completed
                elif(value[1]>0):      
                    print(value)
                    l_3.acquire()
                    value[1]-=1
                    l_3.release()            
            time.sleep(1)

    #Loading data from the config file
    with open('config.json') as f:
        data = json.load(f)

    #Getting port number and worker IDfrom command line
    portNo=int(sys.argv[1])
    workerId=int(sys.argv[2])
    #the locks are being initialsed
    l_1=threading.Lock()
    l_2=threading.Lock()
    l_3=threading.Lock()          
    # the config json file is loaded into data and 
    workers=data['workers']
    workerClass=None
    for i in range(len(workers)):
        if workers[i]['worker_id'] == workerId and workers[i]['port'] == portNo :
            workerClass = Worker(portNo,workerId,workers[i]['slots'])
            break
            # we break out of the for loop once the condition in the if statement is matched

    #If the parameters above never match then :
    if workerClass==None :
        print('ERROR! worker ID doesnt match the port number')
        exit
    else:    
        #Closing the file which was creating for logs analysis
        f=open('wlogs'+str(workerClass.portNo)+'.txt',"a+",buffering=1)
        serverWorker=TCPServer(workerClass.portNo)
        # list of threads
        threads = [threading.Thread(target=serverWorker.startserver), threading.Thread(target=send_request)]
        # looping through the list of threads
        for thread in threads:
            thread.start()
            print(f'started thread {thread}')
            thread.join(0.1)