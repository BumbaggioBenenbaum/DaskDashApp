import json
import os
import subprocess
import time
from time import sleep

import selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from config import ClusterStatus, Config
from daskapp.root import ROOT

import warnings; warnings.simplefilter('ignore') #suppressing Selenium's future warnings

def aws_login_cred_update(config:Config):
    options = Options()
    options.add_argument('--headless')
    options.add_argument("--disable-popup-blocking")
    login_box_id = 'pseudonym_session_unique_id'
    pass_box_id = 'pseudonym_session_password'
    submit_login = '/html/body/div[2]/div[2]/div/div/div[1]/div/div/div/div/div/div[2]/form[1]/div[3]/div[2]/button'
    frame_id = "tool_content"
    start_lab_xpath = '//*[@id="launchclabsbtn"]'
    show_details_id = 'detailbtn2'
    show_creds_id = 'clikeyboxbtn'
    creds_box_id = 'clikeybox'

    driver = webdriver.Firefox(options=options)
    print("Selenium launched.")
    driver.get("https://awsacademy.instructure.com/login/canvas")
    time.sleep(2)
    driver.find_element_by_id(login_box_id).send_keys(config.awsacademy.login)
    driver.find_element_by_id(pass_box_id).send_keys(config.awsacademy.password)
    driver.find_element_by_xpath(submit_login).click()

    time.sleep(3)
    print("Succesfully logged in to AWS Academy.")

    driver.get(f"https://awsacademy.instructure.com/courses/{config.awsacademy.course_module}/modules/items/{config.awsacademy.course_item}")
    time.sleep(8)

    driver.switch_to.frame(driver.find_element_by_id(frame_id))

    driver.find_element_by_xpath(start_lab_xpath).click()
    time.sleep(8)
    print("Lab started.")

    try:
        driver.find_element_by_id(show_details_id).click()
    except selenium.common.exceptions.UnexpectedAlertPresentException:
        driver.find_element_by_id(show_details_id).click()
    time.sleep(12)

    driver.find_element_by_id(show_creds_id).click()

    time.sleep(2)
    creds = driver.find_element_by_id(creds_box_id).text.replace("Copy and paste the following into ~/.aws/credentials", "")
    split_creds = [x.split('=', 1) for x in creds.splitlines() if '=' in x]
    config.awsacademy.key = split_creds[0][1]
    config.awsacademy.secret = split_creds[1][1]
    config.awsacademy.token = split_creds[2][1]
    subprocess.Popen(
        f'''
            export AWS_ACCESS_KEY_ID={config.awsacademy.key};
            export AWS_SECRET_ACCESS_KEY={config.awsacademy.secret};
            export AWS_SESSION_TOKEN={config.awsacademy.token}
        
        ''',
        shell=True
    )
    print("Grabbed credentials.")

    with open("temp_creds.txt", 'w') as f:
        f.write(creds)

    save_creds = subprocess.Popen(f"mv ./temp_creds.txt ~/.aws/credentials", shell=True)
    print("Credentials moved to the ~/.aws directory")
    save_creds.wait()
    driver.quit()


def create_cluster(config:Config):
    create_cluster = subprocess.Popen(['bash', 'config_scripts/create_cluster.sh'], stdout=subprocess.PIPE)
    create_cluster_output = json.loads(create_cluster.stdout.read())
    config.emr.cluster_id = create_cluster_output.get("ClusterId")
    create_cluster.wait()
    print("Created cluster", config.emr.cluster_id)
    print("-"* 100, "\n")
    return config

def create_cluster(config:Config) -> None:
    create_cluster = subprocess.Popen(
        ['bash', 'config_scripts/create_cluster.sh'], 
        stdout=subprocess.PIPE
    )
    create_cluster_output = json.loads(create_cluster.stdout.read())
    config.emr.cluster_id = create_cluster_output.get("ClusterId")
    create_cluster.wait()



def check_status(config:Config):
    checker = 1 
    print("The cluster's status will be checked every 30 seconds and logged every 2 minutes.")
    while True:
        check_cluster_status = subprocess.Popen(
            f'aws emr describe-cluster --cluster-id {config.emr.cluster_id}', 
            stdout=subprocess.PIPE, 
            shell=True
        )

        cluster_status = json.loads(check_cluster_status.stdout.read())
        check_cluster_status.wait()
        status = cluster_status['Cluster']['Status']['State']

        if status == ClusterStatus.WAITING:
            print("Cluster ready.")
            config.emr.dns = cluster_status['Cluster']["MasterPublicDnsName"]
            break

        check_cluster_status.wait()
        if checker % 4 == 0:
            print(f"Cluster's status after {(checker-1) * 30 / 60}m ::: ", status)
        checker += 1
        sleep(5)


def clean_dir(config:Config):
    '''ensure the removal of previous logs'''
    clean_dir_script = f'''
    ssh -o StrictHostKeyChecking=no -i bdw.pem hadoop@{config.emr.dns} "
    sh -c 'aws s3 cp s3://{config.s3.bucket}/{config.s3.assets_path}/{config.s3.cleaner_script} /home/hadoop/{config.s3.cleaner_script}'; 
    python3 {config.s3.cleaner_script}"
    '''
    clean_dir = subprocess.Popen(
        clean_dir_script, 
        stdout=subprocess.PIPE, 
        shell=True
    )
    clean_dir.wait()


def copy_credentials(config:Config):
    copy_creds_script = f'''
    aws s3 cp {ROOT}/config.ini s3://{config.s3.bucket}/{config.s3.assets_path}/config.ini
    '''
    copy_creds = subprocess.Popen(
        copy_creds_script, 
        shell=True
    )


def copy_script(config: Config):
    '''Copy python dask cluster launching script from S3 to EMR & run it'''

    copy_script = f'''
    ssh -i bdw.pem hadoop@{config.emr.dns} "
    sh -c 'aws s3 cp s3://{config.s3.bucket}/{config.s3.assets_path}/{config.s3.cluster_init_script} /home/hadoop/{config.s3.cluster_init_script}'; 
    source /home/hadoop/daskenv/bin/activate;
    python3 {config.s3.cluster_init_script}
    "
    '''
    copy_script_and_run = subprocess.Popen(
        copy_script, 
        shell=True
    )


def copy_dashboard_link(config: Config):
    '''copy dashboard link files to local fs'''
    print("Copying dashboard links...")
    dashboard_link_script = f'''
    scp -i bdw.pem hadoop@{config.emr.dns}:/home/hadoop/{config.emr.dashboard_link_file} ./{config.emr.dashboard_link_file};
    '''
    while config.emr.dashboard_link_file not in os.listdir():
        dashboard_proc = subprocess.Popen(
            dashboard_link_script, 
            shell=True
        )
        dashboard_proc.wait()
        sleep(2)


def forward_ports(config:Config):

    with open(config.emr.dashboard_link_file, 'r') as dashboard_link_file:
        dask_dashboard_link = dashboard_link_file.read()
        config.dask.dashboard_link = dask_dashboard_link.split("/")[2]

    os.remove(config.emr.dashboard_link_file)

    subprocess.Popen(
        ['gnome-terminal', '-x', 'ssh', '-i', 'bdw.pem', '-L', 
         f'{config.dask.dashboard_port}:{config.emr.dns}:{config.dask.dashboard_link}', 
         f'hadoop@{config.emr.dns}'
        ])
    subprocess.Popen(
        ['gnome-terminal', '-x', 'ssh', '-i', 'bdw.pem', '-L', 
         f'{config.dask.client_port}:{config.emr.dns}:{config.dask.client_link}', 
         f'hadoop@{config.emr.dns}'
        ])


def launch_cluster(
    config: Config,
    regenerate_creds:bool=True
) -> None:
    print("Starting new EMR cluster...")
    if config.emr.dashboard_link_file in os.listdir():
        os.remove(config.emr.dashboard_link_file)

    if regenerate_creds:
        print("Visiting AWSAcademy to obtain new credentials.")
        aws_login_cred_update(config)

    create_cluster(config)
    copy_credentials(config)
    check_status(config)
    clean_dir(config)
    copy_script(config)
    copy_dashboard_link(config)
    forward_ports(config)


if __name__ == "__main__":
    config = Config.from_ini()
    launch_cluster(config)