import wmi
import psutil
import nvidia_smi
import psycopg2
import calendar
import time
from datetime import datetime
from pythonping import ping
#building the app like its a centralized poller rather than a disttibuted self pollerpoller
#should probably adjust it so it can accomodate either.
device_id = 1 #Jakes, 2 for zach
disks_obj = {}

def get_metric_types(cursor):
    by_id = {}
    by_name = {}

    cursor.execute('SELECT * \
                    FROM metric_types \
                    WHERE type_name != \'disk utilization\'')

    for i in cursor.fetchall():
        by_id[i[0]] = i[1]
        by_name[i[1]] = i[0]

    return by_id, by_name

def get_disk_metric(cursor):
    disk_metric_id = None
    cursor.execute('SELECT id \
                    FROM metric_types \
                    WHERE type_name = \'disk utilization\'')

    for i in cursor.fetchall():
        disk_metric_id = i[0]

    return disk_metric_id

def get_disk_metrics(cursor):
    cursor.execute('SELECT * \
                    FROM metrics \
                    WHERE device_id = %s \
                    AND metric_type_id = %s', [device_id, disk_metric_id])
    for i in cursor.fetchall():
        disks_obj[i[3]] = i[0]
  
def get_devices(cursor):
    devices = {}
    cursor.execute('SELECT id, device_name, inet_ntoa(ip), active \
                    FROM devices \
                    WHERE active = True')

    for i in cursor.fetchall():
        devices[i[0]] = {'name': i[1], 'ip': i[2]}

    return devices

def create_metric(conn, device_id, metric_type_id, descr = ""):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO METRICS(device_id, metric_type_id, description) VALUES(%s, %s, %s)', [device_id, metric_type_id, descr])
    conn.commit()


def create_metrics(conn, devices):
    cursor = conn.cursor()
    
    for i in devices:
        cursor.execute('SELECT metric_type_id \
                        FROM Metrics \
                        WHERE device_id = %s', [i])

        metrics = cursor.fetchall()

        if metrics != []:
            for j in metric_types_by_id:

                exists = False

                for k in metrics:
                    if j == k[0]:
                        exists = True
                        break
                
                if not exists:
                    create_metric(conn, i, k, "")

        else:
            for j in metric_types_by_id:
                create_metric(conn, i, j, "")




#Create cpu metric if it doesn't exist - done
#create mem metric if it doesn't exist - done
#create gpu metrics if they don't exist - done?
#create ping metric if it doesn't exist - done?
#create packet lost metric if it doesn't exist
#create disk metrics if they don't exist



'''
select existing metrics for the device from the metrics table into a hash

'''

conn = wmi.WMI()
db = None
cursor = None

try:
    db = psycopg2.connect("dbname='metrics' user='atlassian' host='localhost' password='atlassian'")
    cursor = db.cursor()
except:
    print("I am unable to connect to the database")

metric_types_by_id = {}
metric_types_by_name = {}
devices = {}

disk_metric_id = get_disk_metric(cursor)
metric_types_by_id, metric_types_by_name = get_metric_types(cursor) 
devices = get_devices(cursor)
create_metrics(db, devices)
get_disk_metrics(cursor)

disks = psutil.disk_partitions(all=False)
for i in disks:
    if not disks_obj.get(i[0], {}) != {}:
        cursor.execute('INSERT INTO metrics(device_id, metric_type_id, description) \
                        VALUES(%s, %s, %s)',[device_id, disk_metric_id, i[0]])

db.commit()


device_metrics = {}
cursor.execute('SELECT metric_types.type_name,  \
                       metrics.id, \
                       metrics.description \
                FROM metrics \
                JOIN metric_types \
                ON metric_types.id = metrics.metric_type_id \
                WHERE device_id = %s', [device_id])
for i in cursor.fetchall():
    device_metrics[i[0]] = {'id': i[1], 'descr': i[2]}

while True:
    start_time = calendar.timegm(time.gmtime())
    
    #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    utilizations = [cpu.LoadPercentage for cpu in conn.Win32_Processor()]
    #print(utilizations)
    if utilizations is not None:
        cpu_utilization = int(sum(utilizations) / len(utilizations))  # avg all cores/processors
    else:
        cpu_utilization = 0
    #print("cpu util: ", cpu_utilization) 


    mem_utilization = psutil.virtual_memory()[2]
    #print("mem util: ", mem_utilization)
    
    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['cpu utilization']['id'], cpu_utilization, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()
    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['memory utilization']['id'], mem_utilization, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()
    

    nvidia_smi.nvmlInit()
    handle = nvidia_smi.nvmlDeviceGetHandleByIndex(0)
    # card id 0 hardcoded here, there is also a call to get all available card ids, so we could iterate
    res = nvidia_smi.nvmlDeviceGetUtilizationRates(handle)
    #print (res.gpu, res.memory)


    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['gpu utilization']['id'], res.gpu, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()
    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['gpu mem utilization']['id'], res.memory, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()


    count = 0
    results = ping('www.google.com', timeout=1, count=10)


    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['ping avg response']['id'], results.rtt_avg_ms, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()
    cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                    VALUES(%s, %s, %s)", 
                    (device_metrics['ping packets lost']['id'], results.packets_lost, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
    db.commit()

    for i in disks_obj:
        usage = psutil.disk_usage(i)
        cursor.execute("INSERT INTO measurements (metric_id, value, from_date) \
                        VALUES(%s, %s, %s)", 
                        (disks_obj[i], usage[3], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
        db.commit()

    end_time = calendar.timegm(time.gmtime())

    sleep_time = 60 - (end_time - start_time)
    time.sleep( sleep_time if sleep_time > 0 else 60)
    

if cursor is not None:
    cursor.close()
if db is not None:
    db.close()





