#!/usr/bin/python3
import socket
import threading
import signal
import os
import json
import base64
import sys
import time
import datetime
import errno
import configparser
import traceback
from utility import bcolors

BUFFERSIZE=65535

recording=[]
currDir=os.path.dirname(os.path.abspath(__file__))
masterDir=os.path.join(currDir,"master")
class EdgeStorage(object):
    lock = threading.Lock()

    def __init__(self, host, port,eaAddress,eaPort):
        signal.signal(signal.SIGINT, self.stopEdgeStorage)
        signal.signal(signal.SIGCHLD, self.addLink)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.eaAddress=eaAddress
        self.eaPort=eaPort
        if not os.path.exists(masterDir):
            os.makedirs(masterDir)
    
    #TODO CREATE FOLDER
    def createFolder(self,client,name):
        newDir=os.path.join(currDir,name)
        if not os.path.exists(newDir):
            os.makedirs(newDir)
        response = {}
        response['status'] = 'success'
        response = json.dumps(response)
        client.send(response.encode())

    def startRecording(self,nameContent,nameProvider,timer):
        pid = os.fork()
        if pid < 0:
            print("cannot fork process")
            exit(-1)
        elif pid == 0:
            rectimer=str(int(timer))
            destination=os.path.join(masterDir,"{}.mp4".format(nameContent))
            print (destination)
            os.execvp("ffmpeg", ["ffmpeg", "-strict", "-2","-loglevel","quiet",
                                   "-t", rectimer , "-i", "http://{}:{}/{}.mp4".format(self.eaAddress,self.eaPort,nameProvider),
                                   "-strict","-2","-preset","ultrafast",destination])
        else:
            print("Recording of {} started with pid {}".format(nameContent,pid))
            self.addPidOnStruct(nameContent,pid)

    def addLink(self, signum,stack):
        global recording
        pid, sts = os.waitpid(-1, os.WNOHANG)
        if (sts == 0):
            for record in recording:
                if record['pid'] == pid:
                    EdgeStorage.lock.acquire()
                    for user in record['list']:
                        fileToLink=os.path.join(masterDir,"{}.mp4".format(record['nameContent']))
                        newFile=os.path.join(currDir,user)
                        newFile=os.path.join(newFile,"{}.mp4".format(record['nameContent']))
                        os.link(fileToLink,newFile)
                    recording.remove(record)
                    EdgeStorage.lock.release()
        print(json.dumps(recording,default=str))
                
    
    def stopEdgeStorage(self, signum,stack):
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        for record in recording:
            if(record['pid'] is not None):
                os.kill(record['pid'], signal.SIGINT)
        print("Edge Storage stopped")
        sys.exit(0)

    def listen(self):
        self.sock.listen(5)
        print("EdgeStorage ready to accept")
        while True:
            client, address = self.sock.accept()
            print("New connection from {}".format(address))
            client.settimeout(60)
            threading.Thread(target = self.listenToClient, args = (client, address)).start()

    def addPidOnStruct(self,nameContent,pid):
        global recording
        EdgeStorage.lock.acquire()
        for record in recording:
            if record['nameContent'] == nameContent:       
                record['pid'] = pid 
        print("PID ADDED:"+ json.dumps(recording,default=str))
        EdgeStorage.lock.release()


    def alredyRecording (self,nameContent):
        found=False
        for record in recording:
            if record['nameContent'] == nameContent:
                found=True
        return found

    def addedUser(self,name,list):
        toAdd = True
        for user in list:
            if user == name:
                toAdd= False
                break
        return toAdd

    def addOnStruct(self,nameContent,name,t=None,pid=None):
        global recording
        EdgeStorage.lock.acquire()
        found=False
        for record in recording:
            if record['nameContent'] == nameContent:
                found=True
                toAdd=self.addedUser(name,record['list'])
                if (toAdd is True) :     
                    record['list'].append(name)
                break
        record={}
        if not found:
            record['nameContent'] = nameContent
            record['t']=t
            record['pid']=pid
            record['list']=[name]
            recording.append(record)
        EdgeStorage.lock.release()

    def recContent(self,client,nameContent,startingTime,length,name,nameProvider):
        now = datetime.datetime.now()
        print(startingTime)
        print(now)
        timer = startingTime - now
        if(startingTime+length < now):
            print("Programma finito non è possibile registrarlo")
        else:
            if not self.alredyRecording(nameContent):       
                if int(timer.total_seconds()) >=0 :
                    print ("Registration inizia fra {} e dura {}".format(timer, length))
                    t=threading.Timer(int(timer.total_seconds()),self.startRecording,args=(nameContent,nameProvider,length.total_seconds()))
                    self.addOnStruct(nameContent,name,t)
                    t.start()
                else:
                    timer = now - startingTime
                    #print (timer)
                    length=length-timer
                    print("Programma già iniziato registrazione dura {}".format(length))
                    t=threading.Thread(target=self.startRecording,args=(nameContent,nameProvider,length.total_seconds()))
                    self.addOnStruct(nameContent,name,t)
                    t.start()
            else:
                self.addOnStruct(nameContent,name) 
        print("AFTER REQUEST HANDLING"+json.dumps(recording,default=str))
        response = {}
        response['status'] = 'Recording'
        response = json.dumps(response)
        print(bcolors.OKGREEN+"{}".format(response)+bcolors.ENDC)
        client.send(response.encode())

    def convertParameter(self,response):
        nameContent = response.get('nameContent',None)
        startingTime = response.get('startingTime',None)
        length = response.get('length',None)
        nameProvider=response.get('nameProvider',None)
        startingTime = datetime.datetime.strptime(startingTime, '%Y-%m-%d %H:%M:%S')
        length =  datetime.datetime.strptime(length, '%H:%M:%S').time()
        length=datetime.timedelta(hours=length.hour,minutes=length.minute,seconds=length.second)
        name = response.get('name',"export") #NOME UTENTE DA DEFINIRE DOPO
        #name = "export"
        return (nameContent,startingTime,length,name,nameProvider)

    def listenToClient(self, client, address):
        
        while True:
            try:
                data = client.recv(BUFFERSIZE).decode().strip()
                print (bcolors.HEADER+ "received {}, type: {} ".format(data,type(data))+bcolors.ENDC)
                if data:
                    response = json.loads(data)
                    print (bcolors.HEADER+ "decoded {}, type: {} ".format(response,type(response))+bcolors.ENDC)
                    operation = response.get('operation', None)             
                    print("operation: " + operation)
                    if operation in 'rec content':
                        nameContent,startingTime,length,name,nameProvider= self.convertParameter(response)
                        self.recContent(client,nameContent,startingTime,length,name,nameProvider)
                    elif operation in 'create folder':
                        name=response.get('name',None)
                        self.createFolder(client,name)
                    else:
                        print("operation not allowed")
                        response=json.dumps({"status":"operation not recognized"})
                        client.send(response.encode())
                else:
                    raise Exception('Client disconnected')
            except Exception as ex:
                print(ex)
                client.close()
                return False

if __name__ == "__main__":
    if len(sys.argv) == 2:
        config=configparser.ConfigParser()
        config.read(sys.argv[1])
        if ("EdgeStorage") in config.sections():
            print(bcolors.OKGREEN+"there is a Edge Storage Section"+bcolors.ENDC)
            addr=config['EdgeStorage']['ip']
            port=int(config['EdgeStorage']['port'])
        else: 
            print (bcolors.FAIL+"There is no EdgeStorage section"+ bcolors.ENDC)
            sys.exit(-1)
        if ("EdgeAcquirer") in config.sections():
            eaAddr=config['EdgeAcquirer']['ip']
            eaPort=int(config['EdgeAcquirer']['port'])
            print(bcolors.OKGREEN+"There is a Edge acquirer section"+bcolors.ENDC)
        else:
            print(bcolors.FAIL+" ERROR There is no edge acquirer section"+bcolors.ENDC)
            sys.exit(-1)
        print("Edge Storage starts on port {} with addr {}".format(port,addr))
        print("Edge Acquirer Address {} port {}".format(eaAddr,eaPort))
        EdgeStorage(addr,port,eaAddr,eaPort).listen()
    else:
        print(bcolors.FAIL +"Usage {} configfile".format(sys.argv[0])+ bcolors.ENDC)
