from __future__ import print_function
from time import sleep,time
import socket 

def monitor():
    last_idle = last_total = 0
    while True:
        with open('/proc/stat') as f:
            fields = [float(column) for column in f.readline().strip().split()[1:]]
        idle, total = fields[3], sum(fields)
        idle_delta, total_delta = idle - last_idle, total - last_total
        last_idle, last_total = idle, total
        utilisation = (1.0 - idle_delta / total_delta)
        print('%5.1f%%,%d' % (100.0*utilisation,int(round(time() * 1000))) )
        sleep(5)

def main():    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('10.156.0.7', 11211))
    s.listen(5)
    print('Waiting for connection...')
    sock, addr = s.accept()
    sock.close()
    monitor()

if __name__ == "__main__":
    main()
