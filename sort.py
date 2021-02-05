import sys
import math
import heapq
import _thread
from threading import Thread
import os
import time
from threading import *

t_count=0
thread_over=0
key_len=0

def create_temp_files(ip_fname, no_of_tuple,index_dict,column_list, metadata):
    global key_len
    i=0
    k=1
    fd=open(ip_fname,"r")
    for line in fd:
        #print(line[:-1])
        if i >= no_of_tuple:
            k+=1
            i=0
        s="./temp"+str(k)+".txt"
        t1=open(s,"a")
        line=line[:-1]
        lst=line.split("  ")
        if len(lst) != len(metadata):
            n=len(lst)
            g=0
            temp_str=""
            temp_lst=[]
            while g < n-2:
                temp_str+=lst[g]
                temp_str+="  "
                g+=1
            temp_str=temp_str[:-2]
            temp_lst.append(temp_str)
            temp_lst.append(lst[n-2])
            temp_lst.append(lst[n-1])
            lst=temp_lst
        s=""
        for cl in column_list:
            indx=index_dict[cl]
            s+=lst[indx]
        key_len=len(s)
        s+="  "
        s+=line
        s+="\n"
        t1.write(s)
        i+=1
        t1.close()
    fd.close()


class sort_each_chunk_thread(Thread):
    def __init__(self,j,order,metadata):
        super(sort_each_chunk_thread,self).__init__()
        self.j=j
        self.order=order
        self.metadata=metadata
        self.start()
    
    def run(self):
        mutex.acquire()
        try:
            s="./temp"+str(self.j)+".txt"
            f=open(s,"r")
            d=f.read()
            f.close()
            d_lst=d.split("\n")
            sx=len(d_lst)
            i=0
            data=[]
            while i < sx:
                st=d_lst[i]
                if(st==""):
                    i+=1
                    continue
                temp=[]
                temp=st.split("  ")
                if len(temp) != len(metadata):
                    n=len(temp)
                    g=0
                    temp_str=""
                    temp_lst=[]
                    while g < n-2:
                        temp_str+=temp[g]
                        temp_str+="  "
                        g+=1
                    temp_str=temp_str[:-2]
                    temp_lst.append(temp_str)
                    temp_lst.append(temp[n-2])
                    temp_lst.append(temp[n-1])
                    temp=temp_lst

                data.append(temp)
                i+=1
            '''print(data)
            print()
            print(len(data))
            print("********************************")'''
            if self.order.upper() == "ASC":
                data.sort(key=lambda x:x[0])
            else:
                data.sort(key=lambda x:x[0], reverse=True)
            
            dz=len(data)
            i=0
            f=open(s,"w")
            f.write("")
            f.close()
            print("Writing to disk #",self.j)
            while i<dz:
                lst=data[i]
                st=""
                l=len(lst)
                k=0
                while k<l:
                    st+=lst[k]
                    if(k==l-1):
                        st+="\n"
                    else:
                        st+="  "
                    k+=1
                i+=1
                f=open(s,"a")
                f.write(st)
                f.close()
        finally:
            mutex.release()
        


def sort_each_chunk(j,order, metadata):
    s="./temp"+str(j)+".txt"
    f=open(s,"r")
    d=f.read()
    f.close()
    d_lst=d.split("\n")
    sx=len(d_lst)
    i=0
    data=[]
    while i < sx:
        st=d_lst[i]
        if(st==""):
            i+=1
            continue
        temp=[]
        temp=st.split("  ")
        if len(temp) != len(metadata):
            n=len(temp)
            g=0
            temp_str=""
            temp_lst=[]
            while g < n-2:
                temp_str+=temp[g]
                temp_str+="  "
                g+=1
            temp_str=temp_str[:-2]
            temp_lst.append(temp_str)
            temp_lst.append(temp[n-2])
            temp_lst.append(temp[n-1])
            temp=temp_lst
        data.append(temp)
        i+=1
    '''print(data)
    print()
    print(len(data))
    print("********************************")'''
    if order.upper() == "ASC":
        data.sort(key=lambda x:x[0])
    else:
        data.sort(key=lambda x:x[0], reverse=True)
    
    dz=len(data)
    i=0
    f=open(s,"w")
    f.write("")
    f.close()
    print("Writing to disk #",j)
    while i<dz:
        lst=data[i]
        st=""
        l=len(lst)
        k=0
        while k<l:
            st+=lst[k]
            if(k==l-1):
                st+="\n"
            else:
                st+="  "
            k+=1
        i+=1
        f=open(s,"a")
        f.write(st)
        f.close()
    


def merge_files(total_chunks,order, total_tuples, op_fname):
    op=open(op_fname,"w")
    #print(123)
    #if order.upper()=="ASC":
    heap=[]
    heapq.heapify(heap)
    i=1
    fd_list=[]
    fname_list=[]
    print("Sorting...")
    while i<=total_chunks:
        s="./temp"+str(i)+".txt"
        fi=open(s,"r")
        fname_list.append(s)
        fd_list.append(fi)
        i+=1
    
    i=0
    while i < total_chunks:
        d=fd_list[i].readline()
        if(d==""):
            break
        d=d[:-1]
        heapq.heappush(heap,[d,i])
        i+=1
    r=i
    count=0
    wc=0
    if(order.upper()=="DESC"):
        heapq._heapify_max(heap)
    '''for i in heap:
        print(i)'''
    #print("count=",r)
    #print("tuple=",total_tuples)
    print("Writing to disk")
    while count<r and wc<total_tuples-1:
        wc+=1
        lt=heap[0]
        dt=lt[0]
        #dt=dt[:-1]
        indx=lt[1]
        #yg=dt.find("  ")
        dt=dt[key_len+2:]
        op.write(dt)
        op.write("\n")
        dt=fd_list[indx].readline()
        if dt=="":
            count+=1
            #continue
        dt=dt[:-1]
        #print(indx,dt)
        #print(count)
        #print("ghj")
        #print(dt)
        lt[0]=dt
        if order.upper() == "DESC":
            heapq._heappop_max(heap)
        else:
            heapq.heappop(heap)
        if dt!="":
            heapq.heappush(heap,[dt,indx])
            if(order.upper()=="DESC"):
                heapq._heapify_max(heap)
    
    '''for i in heap:
        print(i)'''
    lt=heap[0]
    dt=lt[0]
    indx=lt[1]
    #yg=dt.find("  ")
    dt=dt[key_len+2:]
    #print(dt)
    op.write(dt)
    op.write("\n")
    try:
        dt=fd_list[indx].readline()
    except:
        pass
    lt[0]=dt
    if order.upper() == "DESC":
        heapq._heappop_max(heap)
    else:
        heapq.heappop(heap)
    heapq.heappush(heap,[dt,indx])
    if(order.upper()=="DESC"):
        heapq._heapify_max(heap)
    op.close()
    i=0
    while i<total_chunks:
        fd_list[i].close()
        os.remove(fname_list[i])
        i+=1



start_time = time.time()
#..........................handling run time arguments....................
flag=0
if len(sys.argv) < 6:
    print("Invalid No of Arguments")
    exit()
op_fname=sys.argv[2]
memory_size=sys.argv[3]
try:
    memory_size=int(memory_size)*1000000       # memory size in bytes 
except:
    print("Invalid Memory Size")
    exit()
thread_count=0
i=0
if(sys.argv[4].upper()=="ASC" or sys.argv[4].upper()=="DESC"):
    order=sys.argv[4]
    i=5
else:
    try:
        thread_count=int(sys.argv[4])
    except:
        print("Invalid thread count")
        exit()
    order=sys.argv[5]
    flag=1
    i=6

#print(len(sys.argv))
al=len(sys.argv)
column_list=[]
while i<al:
    column_list.append(sys.argv[i])
    i+=1
#print(op_fname,memory_size,thread_count,order)
#print(column_list)



#....................reading the metadata file....................
f2=open("metadata.txt","r")
d=f2.read()
c_lst=d.split("\n")
sz2=len(c_lst)
i=0
metadata={}
index_dict={}
temp=[]
while i<sz2:
    st=c_lst[i]
    if(st==""):
        i+=1
        continue
    temp=[]
    temp=st.split(",")
    index_dict[temp[0]]=i
    metadata[temp[0]]=temp[1]
    i+=1
#print(metadata)
#print(index_dict)

#........................tuple and file size calculations............
tuple_size=0
file_size=0
for key in metadata:
    try:
        tuple_size+=int(metadata[key])
    except:
        print("Invalid Column size in metadata.txt file")
        exit()
#print("Tuple size=",tuple_size)


# ..............................validation of the column list...........................
for cl in column_list:
    if cl not in metadata.keys():
        print("The Column",cl,"does not exist")
        exit()

print("###start execution")
print("##running Phase-1")

'''no_of_tuple=math.floor(memory_size/tuple_size)
if(no_of_tuple==0):
    print("A tuple size is greater than the memory size, therefore cannot sort")
    exit()'''
#print("No of tuple in each chunk= ",no_of_tuple)

ip_fname=sys.argv[1]
fd=open(ip_fname,"r")
total_tuples=0
for line in fd:
    total_tuples+=1
fd.close()
#print(total_tuples)



#normal sorting
if flag==0:
    no_of_tuple=math.floor(memory_size/tuple_size)
    #print(no_of_tuple,total_tuples)
    if(no_of_tuple==0):
        print("A tuple size is greater than the memory size, therefore cannot sort")
        exit()
    total_chunks=math.ceil(total_tuples/no_of_tuple)
    print("Number of sub-files (splits): ",total_chunks)
    print("****************************************************")
    create_temp_files(ip_fname,no_of_tuple,index_dict,column_list, metadata)
    j=1
    while j <= total_chunks:
        print("sorting #",j,"sublist")
        sort_each_chunk(j,order,metadata)
        j+=1
    print("****************************************************")
    print("##running phase-2")

    merge_files(total_chunks,order,total_tuples, op_fname)
    print("###completed execution")

    
#parallel sorting 
else:
    #print(2)
    global mutex
    mutex=BoundedSemaphore(thread_count)
    effective_memory=memory_size/thread_count
    no_of_tuple=math.floor(effective_memory/tuple_size)
    if(no_of_tuple==0):
        print("A tuple size is greater than the memory size, therefore cannot sort")
        exit()
    total_chunks=math.ceil(total_tuples/no_of_tuple)
    print("Number of sub-files (splits): ",total_chunks)
    print("****************************************************")
    create_temp_files(ip_fname,no_of_tuple,index_dict,column_list, metadata)


    t_count=0
    thread_list=[]
    i=1
    while i <= total_chunks:
        '''if t_count >= thread_count:
            while t_count >= thread_count:
                pass'''
        #print(i)
        while t_count==thread_count:
                pass
        #elif t_count < thread_count:
        t_count+=1
        p=sort_each_chunk_thread(i, order, metadata)
        thread_list.append(p)
        #p.join()
        t_count-=1
        i+=1
    
    k=len(thread_list)
    i=0
    while i < k:
        thread_list[i].join()
        i+=1
    
    merge_files(total_chunks,order,total_tuples, op_fname)

print("***************************************************************")
print("###completed execution")
print()
print()
print("Execution Time :- %s seconds" % (time.time() - start_time))


    
    
