import threading
import socket
import sys
import time

logs=open("logs.txt",'a')
execution_pool=dict()
s_time=dict()
e_time=dict()
ip="127.0.0.1"
master_ip="127.0.0.1"
port = int(sys.argv[2])


class Rcv_task(threading.Thread):
    def run(self):
        s=socket.socket()
        s.bind((ip,port))
        s.listen(5)
        while True:
            c,addr=s.accept()
            request=str(c.recv(1024),'utf-8')
            print("task received : ",request)
            task_id,duration = request.split()
            lock.acquire()
            s_time[task_id]=time.time()
            execution_pool[task_id]=duration
            lock.release()
        c.close()


class Exc_task(threading.Thread):
    def run(self):

        print("executing tasks ")
        while True:
            if not len(execution_pool) :
                continue

            print("executing pool right now :")
            print(execution_pool)

            del_tasks = []
            lock.acquire()
            for i in execution_pool.keys():
                execution_pool[i] -= 1
                time.sleep(1)
                if not execution_pool[i]:
                    e_time[i] = time.time()

                    output_string = i + ' ' + str(s_time[i]) + ' ' + str(e_time[i])
                    logs.write(output_string)
                    logs.flush()

                    with socket(AF_INET,SOCK_STREAM) as s:
                        s.connect((master_ip, 5001))
                        message = sys.argv[1] + ' ' + i
                        print("done with : ",i)
                        s.send(message.encode())
                        print(f'sent :  {message} ')
                    
                    del_tasks.append(i)

            for i in del_tasks:
                del execution_pool[i]

            lock.release()

lock = threading.Lock()
t1 = Rcv_task()
t2 = Exc_task()

t1.start()
t2.start()
t1.join()
t2.join()
logs.close()