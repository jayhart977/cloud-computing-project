## This is an naive version of memcache and PARSEC scheduler
## Please first run img_Pull.py or uncomment line 32 before you run this file
## docker -cpuset -cpus

## Please run the following command first, before running the scheduler
## sudo usermode -a -G docker <your-username>

## Different images for PARSEC jobs
## splash2x.fft anakli/parsec:splash2x-fft-native-reduced
## freqmine anakli/parsec:freqmine-native-reduced
## ferret anakli/parsec:ferret-native-reduced
## canneal anakli/parsec:canneal-native-reduced
## dedup anakli/parsec:dedup-native-reduced
## blackscholes anakli/parsec:blackscholes-native-reduced
## For example (in shell)
## docker run --cpuset-cpus="0" -d --rm --name parsec anakli/parsec:blackscholes-native-reduced ./bin/parsecmgmt -a run -p blackscholes -i native -n 2

## Memcached will run on core 0-1, the rest is for PARSEC

########################################################################################
###                              How this scheduler works                            ###
###   In this workload, memcached is coscheduled with PARSEC jobs, memcache resource ###
### is constrainted by taskset command using memcached_resource_set function, please ###
### change this function if you want to change constraint strategy of memcache. PARS ###
### EC containers are constrained by docker sdk command.                             ###
###                                                                                  ### 
###   The scheduling policy major is based on counters. For memcached, we define 3   ###
### thresholds for it, one upper bound, one lower bound and one change bound. And we ###
### also have two counters for it. One used to count the trend of ascending the other###
### is used for downsize. I. if cpu_util of memcache is larger than the upper bound  ###
### and we can use one more cpu then allocate one more cpu for memcache. The same    ###
### stands for those two counters, code can be found inside resource_set function    ###
###                                                                                  ###
###                                                                                  ###
########################################################################################

import os
import numpy as np
from time import sleep
import docker
import psutil
import time
import sys
import subprocess

# Define global variables
scheduler_interval = 1000
last_idle = 0
last_total = 0
global_counter = 0 # Used for scheduler interval
parsec_available_cpu = 1 # Number of cpus that can be used by parsec jobs

## DICT follows the following convetion: ('image_name', 'container_name', 'command')
PARSEC_DICT = {
    'dedup' : ("anakli/parsec:dedup-native-reduced", "parsec_dedup", "./bin/parsecmgmt -a run -p dedup -i native -n 2"),
    'ferret' : ("anakli/parsec:ferret-native-reduced", "parsec_ferret", "./bin/parsecmgmt -a run -p ferret -i native -n 4"),
    'canneal' : ("anakli/parsec:canneal-native-reduced", "parsec_canneal", "./bin/parsecmgmt -a run -p canneal -i native -n 2"),
    'freqmine' : ("anakli/parsec:freqmine-native-reduced", "parsec_freqmine", "./bin/parsecmgmt -a run -p freqmine -i native -n 4"),
    'fft' : ("anakli/parsec:splash2x-fft-native-reduced", "parsec_fft", "./bin/parsecmgmt -a run -p splash2x.fft -i native -n 4"),
    'blackscholes' : ("anakli/parsec:blackscholes-native-reduced", "parsec_blackscholes", "./bin/parsecmgmt -a run -p blackscholes -i native -n 2")
}

client = docker.from_env()

## Global Control parameters
### Memcache
class memcached(object):
    def __init__(self):
        # This part defines scheduling strategies for memcached, please test and change accordingly
        self.memca_c1_bound = 80
        self.memca_change_bound = 50
        self.memca_c2_bound = 25
        self.memca_cpu_utilization_last = 0
        self.memca_cpu_utilization_new = 0
        self.memca_up_counter = 0
        self.memca_down_counter = 0
        self.memca_down_add_thr = 5
        self.memca_up_add_thr = 5
        self.memca_counter_thrd = 3
        self.memca_used_cpu = 1
        self.pid = os.popen("pidof memcached").read()
    
    def mem_cpu_allocation_logger(self,cpu_file):
        cpu_file.write("%d cpu allocated to memcached at %d"%(self.memca_used_cpu,int(round(time.time() * 1000))))

    def cpu_util(self, interval=None):
        cpu_util_list = psutil.cpu_percent(interval=interval, percpu=True)
        temp = sum(cpu_util_list[:self.memca_used_cpu])
        # Write to cpu_util.log
        #cpu_log_file.write(time.asctime(time.localtime()))
        cpu_log_file.write(str(int(round(time.time() * 1000))))
        cpu_log_file.write(str(cpu_util_list))
        self.memca_cpu_utilization_new = temp / self.memca_used_cpu
        diff = self.memca_cpu_utilization_new - self.memca_cpu_utilization_last
        # Counter value change
        if (diff > 0):
            if (diff > self.memca_up_add_thr):
                self.memca_up_counter += 1
                if (self.memca_down_counter >= 1):
                    self.memca_down_counter -= 1
        elif (diff < 0):
            if (abs(diff) > self.memca_down_add_thr):
                self.memca_down_counter += 1
                if (self.memca_up_counter >= 1):
                    self.memca_up_counter -= 1
    
    def resource_set(self,parsec_stat):
        'This function is used to schedule memcache workload'
        if (self.memca_cpu_utilization_new >= self.memca_c1_bound):
            # If cpu_util is larger than upper bound
            global parsec_available_cpu
            if (self.memca_used_cpu == 1):
                if (parsec_stat.C1_container):
                    parsec_stat.C1_container.reload()
                    if (parsec_stat.C1_container.status == 'running'):
                        parsec_stat.C1_container.pause()
                        print("pause container %s at %d"%(parsec_stat.C1_running_app,int(round(time.time() * 1000))))
                memcached_resource_set("0,1", self.pid)
                parsec_available_cpu = 0
                self.memca_used_cpu += 1
                self.refresh()

        elif (self.memca_cpu_utilization_new <= self.memca_c2_bound):
            # If cpu_util is lower than lower bound
            if (self.memca_used_cpu == 2):
                memcached_resource_set("0", self.pid)
                parsec_available_cpu = 1
                self.memca_used_cpu -= 1
                self.refresh()

        '''elif ((self.memca_up_counter > self.memca_counter_thrd) and (self.memca_cpu_utilization_new >= self.memca_change_bound)):
            # Up Counter reaches thre
            if (self.memca_used_cpu == 1):
                parsec_stat.C1_container.reload()
                if (parsec_stat.C1_container.status == 'running'):
                    parsec_stat.C1_container.pause()
                    print("pause container %s at %d"%(parsec_stat.C1_running_app,int(round(time.time() * 1000))))
                memcached_resource_set("0,1", self.pid)
                parsec_available_cpu = 0
                self.memca_used_cpu += 1
                self.refresh()

        elif ((self.memca_down_counter > self.memca_counter_thrd) and (self.memca_cpu_utilization_new < self.memca_change_bound)):
            # Down counter reaches thre
            if (self.memca_used_cpu == 2):
                memcached_resource_set("0", self.pid)
                parsec_available_cpu = 1
                self.memca_used_cpu -= 1
                self.refresh() '''
            
    def refresh(self):
        self.memca_up_counter = 0
        self.memca_down_counter = 0

### PARSEC
class parsec(object):
    def __init__(self):
        self.PARSEC_JOB_C1 = ["dedup", "fft", "canneal"]
        self.PARSEC_JOB_C2 = ["blackscholes", "freqmine","ferret"]
        self.C2_running_app = " "
        self.C1_running_app = " "
        self.C1_container = 0
        self.C2_container = 0
    
    def schedule_update(self):
        global parsec_available_cpu
        #print(self.PARSEC_JOB_C1)
        #print(self.PARSEC_JOB_C2)

        if(self.C1_container):
            self.C1_container.reload()
            #print(self.C1_container.status)
        if(self.C2_container):
            self.C2_container.reload()
            #print(self.C2_container.status)

        if (len(self.PARSEC_JOB_C2)):
            # If C1 and C2 list are not empty first check C2
            if (self.C2_running_app == " "):
                self.C2_running_app = self.PARSEC_JOB_C2[0]
                self.C2_container = spin_up_container(PARSEC_DICT[self.C2_running_app][0], "2-3", PARSEC_DICT[self.C2_running_app][1], PARSEC_DICT[self.C2_running_app][2])

            elif (self.C2_container.status == 'exited'):
                # C2 container has finished
                # First remove corresponding app in C2_list
                print("container %s exited at %d"%(self.C2_running_app,int(round(time.time() * 1000))))
                self.PARSEC_JOB_C2.remove(self.C2_running_app)
                
                if (len(self.PARSEC_JOB_C2)):
                    # Still some app remained in C2_list, spawn one of them
                    self.C2_running_app = self.PARSEC_JOB_C2[0]
                    self.C2_container = spin_up_container(PARSEC_DICT[self.C2_running_app][0], "2-3", PARSEC_DICT[self.C2_running_app][1], PARSEC_DICT[self.C2_running_app][2])


            # Now check the status of containers inside C1_list
        if len(self.PARSEC_JOB_C1):
            if (self.C1_running_app == " "):
                # Check whether we can spawn a new C1 app or not
                if (parsec_available_cpu):
                    self.C1_running_app = self.PARSEC_JOB_C1[0]
                    self.C1_container = spin_up_container(PARSEC_DICT[self.C1_running_app][0], "1", PARSEC_DICT[self.C1_running_app][1], PARSEC_DICT[self.C1_running_app][2])
                    #print(" Status of C1 container : %s"%(self.C1_container.status))

            elif (self.C1_container.status == 'exited'):
                # Remove corresponding app from C1_list
                print("container %s exited at %d"%(self.C1_running_app,int(round(time.time() * 1000))))
                self.PARSEC_JOB_C1.remove(self.C1_running_app)
                if(~parsec_available_cpu):
                    # If no spare cpu
                    self.C1_running_app = " "
                else:
                    # Spawn a new app on cpu1 if possible
                    self.C1_running_app = self.PARSEC_JOB_C1[0]
                    self.C1_container = spin_up_container(PARSEC_DICT[self.C1_running_app][0], "1", PARSEC_DICT[self.C1_running_app][1], PARSEC_DICT[self.C1_running_app][2])

            elif (self.C1_container.status == 'paused'):
                # Unpause corresponding app from C1_list
                if(parsec_available_cpu):
                    # If there is a spare cpu
                    self.C1_container.unpause()
                    print("unpause container %s at %d"%(self.C1_running_app,int(round(time.time() * 1000))))



# Define Logger class to obtain log file for this run
cpu_util_log = "CPU_UTIL" + ".log"
mem_cpu_log = "MEM_CPU" + ".log"
log_file = "logger.log"

class Logger(object):
    def __init__(self, filename='default.log'):
        self.terminal = sys.stdout
        self.log = open(filename, 'w')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass

class STAT(object):
    def __init__(self, filename='default_cpu_util.log'):
        self.cpu_log = open(filename, 'w')
    
    def write(self, message):
        self.cpu_log.write(message)
        self.cpu_log.write("\n")
    
    def close(self):
        self.cpu_log.close()

# Create class instances
sys.stdout = Logger(log_file)
cpu_log_file = STAT(cpu_util_log)
mem_cpu_log_file = STAT(mem_cpu_log)
memca_stat = memcached()
parsec_stat = parsec()
# Set memcache PID  
#memca_stat.pid = os.system("pidof memcached")

#sys.stderr = Logger('error_file', sys.stderr)

#################################################################
##                     Function Definition                     ##
#################################################################

def cpu_util():
    """ This function will return cpu utilization """
    global last_idle,last_total
    with open('/proc/stat') as f:
        fields = [float(column) for column in f.readline().strip().split()[1:]]
    idle, total = fields[3], sum(fields)
    idle_delta, total_delta = idle - last_idle, total - last_total
    last_idle, last_total = idle, total
    utilization = 100.0 * (1.0 - idle_delta / total_delta)
    print("%5.1f%%\n" % utilization)
    
    return utilization

def cpu_util_pc(interval=None):
    'This function will return per core cpu utilization'
    return psutil.cpu_percent(interval=interval, percpu=True)

def init():
    """
        This function is used to initialize the whole env, always run it first =)
    """
    # Pull all images if needed，please comment the following commands if you are running this file more than onece
    print("######### Start pulling all needed images #########")
    print(" Start Time:", time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))
    start_time = time.time()
    os.system("sh image_pull.sh")
    print(" \nAll images pulled, time used : %f s\n"%(start_time - time.time()))

    # Initiate psutil cpu stats and write it to log file
    psutil.cpu_percent(interval=None, percpu=True)
    cpu_log_file.write("############## This file contains the trace of cpu utils ##############\n")
    cpu_log_file.write(time.asctime(time.localtime()))
    cpu_log_file.write(str(psutil.cpu_percent(interval=None, percpu=True)))

def list_containers():
    for container in client.containers.list():
        print(container.id)

def list_images():
    for image in client.images.list():
        print(image.id)

def memcached_resource_set(core_id, pid):
    """ 
        Used to set available resources for memcached application
        Core_id and pid must be strings
        For example:
        coreid = "0-2", pid = "128"
    """

    command = "sudo taskset -a -cp " + core_id  + " " + pid
    subprocess.run(command,shell=True)

def check_container_log(container_id):
    container = client.container.get(container_id)
    print(container.logs())

def spin_up_container(img_name, core_id, container_name, command):
    'This function is used to spin up containers'
    print("start container %s at %d"%(container_name,int(round(time.time() * 1000))))
    container = client.containers.run(img_name, command,cpuset_cpus=core_id, name=container_name, auto_remove=False, detach=True)
    return container


#################################################################
##                          Scheduler                          ##
#################################################################

print("######### Scheduler Start Executing #########")
init()

while True:
    if (global_counter % scheduler_interval == 0):
        global_counter = 0
        # Update Memcache resource constraint
        memca_stat.cpu_util(interval=0.5)
        memca_stat.mem_cpu_allocation_logger(mem_cpu_log_file)
        memca_stat.resource_set(parsec_stat)
        # Update PARSEC Containers accordingly
        parsec_stat.schedule_update()
    

    global_counter += 1





