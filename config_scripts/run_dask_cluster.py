
import os
import time

from dask.distributed import Client
from dask_yarn import YarnCluster

for f in ['dashboard_link.txt', 'client_link.txt']:
    if f in os.listdir():
        os.remove(f)

n_workers=5


cluster = YarnCluster(worker_vcores=1, worker_memory="12GB", n_workers=n_workers)
client = Client(cluster)

with open('/home/hadoop/dashboard_link.txt', 'w') as f:
    f.write(str(cluster.dashboard_link))

while ((client.status == "running") and (len(client.scheduler_info()["workers"]) < n_workers)):
    time.sleep(2.0)
    print("Waiting for workers...")

print("Client __repr__: ", client)
print("Dashboard link: ", cluster.dashboard_link)


time.sleep(1000000)
