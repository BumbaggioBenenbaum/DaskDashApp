ssh -i bdw.pem hadoop@ec2-3-80-163-25.compute-1.amazonaws.com "
sh -c 'aws s3 cp s3://daskdata21/assets/run_dask_cluster.py /home/hadoop/run_dask_cluster.py'; 
source /home/hadoop/daskenv/bin/activate;
python3 run_dask_cluster.py
"
