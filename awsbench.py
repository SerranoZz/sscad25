#!/usr/bin/env python3

import json
import aws_bench.pricing_handler as aws
from aws_bench.constants import AWSConfig, SSHConfig, BenchmarkConfig

import boto3  # type: ignore
from botocore.exceptions import ClientError # type: ignore
import paramiko # type: ignore
import time
from datetime import datetime
from pathlib import Path
import argparse
import pandas as pd # type: ignore
import csv
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import socket
import subprocess

def process_fleet_response(response, filename="./results/fleet_attempts.csv"):
    attempts = []

    for error in response.get("Errors", []):
        overrides = error["LaunchTemplateAndOverrides"]["Overrides"]
        attempts.append({
            "InstanceType": overrides.get("InstanceType", ""),
            "SubnetId": overrides.get("SubnetId", ""),
            "Status": "error",
            "ErrorCode": error.get("ErrorCode", ""),
            "ErrorMessage": error.get("ErrorMessage", "")
        })

    for success in response.get("Instances", []):
        overrides = success["LaunchTemplateAndOverrides"]["Overrides"]
        attempts.append({
            "InstanceType": overrides.get("InstanceType", ""),
            "SubnetId": overrides.get("SubnetId", ""),
            "Status": "success",
            "ErrorCode": "",
            "ErrorMessage": ""
        })

    # Escreve no CSV, na ordem em que vieram no JSON
    with open(filename, mode="w", newline="") as csvfile:
        fieldnames = ["InstanceType", "SubnetId", "Status", "InstanceIds", "ErrorCode", "ErrorMessage"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(attempts)


def get_status_ip(region, instance_id):
    session = boto3.Session(region_name=region)
    ec2_client = session.client("ec2")

    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    
    for reservation in response['Reservations']:
        for maquina in reservation['Instances']:
            status = maquina.get('State', 'None')
            private_ip = maquina.get('PrivateIpAddress', 'No Private IP')
    
    return [status['Name'], private_ip]


def save_row(text, row, df, csv_file):
    
    data = {
        "Start_Time": row['Start_Time'],
        "End_Time": row['End_Time'],
        "Instance": row['Instance'],
        "InstanceID": row['InstanceID'],
        #"InstanceStatus": row["InstanceStatus"],
        "Market": row['Market'],
        "Price": row['Price'],        
        "Region": row['Region'],
        "Zone": row['Zone'],
        "Algorithm_Name": row["Algorithm_Name"],
        "Allocation_Strategy": row["Allocation_Strategy"],
        "Class": None,
        "Time_in_Seconds": None,
        "Total_Threads": None,
        "Available_Threads": None,
        "Mops_Total": None,
        "Mops_per_Thread": None,
        "Status": row['Status'],
    }
    regex_patterns = {
        "Class": re.compile(r"Class\s*=\s*(\S+)"),
        "Time_in_Seconds": re.compile(r"Time in seconds\s*=\s*([\d.]+)"),
        "Total_Threads": re.compile(r"Total threads\s*=\s*(\d+)"),
        "Available_Threads": re.compile(r"Avail threads\s*=\s*(\d+)"),
        "Mops_Total": re.compile(r"Mop/s\s*total\s*=\s*([\d.]+)", re.IGNORECASE),
        "Mops_per_Thread": re.compile(r"Mop/s/thread\s*=\s*([\d.]+)", re.IGNORECASE)

    }
    

    for key, pattern in regex_patterns.items():
        match = pattern.search(text)
        if match:
            data[key] = match.group(1)
        else:
            data[key] = None

    df.loc[len(df)] = data

    logging.info(f'Updating file: {csv_file}')
    df.to_csv(csv_file, index=False)

    return df


def wait_for_ssh(ip, port=22, timeout=300):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.create_connection((ip, port), timeout=5)
            sock.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(5)
    raise TimeoutError(f"SSH timeout: {ip}:{port} not reachable after {timeout} seconds.")


def _terminate_instance(instance):
    # if instance is spot, we have to remove its request
    client = boto3.client('ec2', region_name=instance.placement['AvailabilityZone'][:-1])

    instance_ids = [instance.id]

    if instance.instance_lifecycle == 'spot':
        client.terminate_instances(
            InstanceIds=instance_ids,
        )
                
    instance.terminate()
    instance.wait_until_terminated()
    logging.info(f"Instance {instance.id} has been terminated.")


def is_available(region, instance_type):

    session = boto3.Session(aws_access_key_id=AWSConfig.aws_acess_key_id,
                            aws_secret_access_key=AWSConfig.aws_acess_secret_key,
                            region_name=region)

    ec2 = session.client('ec2')
    try:
        response = ec2.describe_instance_types(InstanceTypes=[instance_type])
        return len(response['InstanceTypes']) > 0
    except ClientError as e:
        return False


def create_fleet(region, cluster_size, allocation_strategy, target_capacity):
    """
    Cria uma frota de instâncias EC2 usando `create_fleet`.

    :param region: Região AWS onde a frota será criada
    :param cluster_size: Número de instâncias a serem lançadas
    :param allocation_strategy: Estratégia de alocação (ex: 'lowest-price', 'capacity-optimized')
    :param target_capacity: Capacidade alvo da frota
    :return: Lista de instâncias iniciadas
    """
    instances = []
    for instance in AWSConfig.INSTANCES_BY_REGION[region]:
        for subnet in AWSConfig.SUBNET_IDS_BY_REGION[region]:
            instances.append({'InstanceType': instance.split("-")[0], 'SubnetId': subnet})
   
    session = boto3.Session(region_name=region)
    
    ec2_client = session.client("ec2")
    ec2_resource = session.resource("ec2")

    launch_template_config = [
        {
            "LaunchTemplateSpecification": {
                "LaunchTemplateName": "TCLaunchTemplate",
                "Version": "$Default"
            },
            "Overrides": instances  
        }
    ]

    fleet_config = {
        "LaunchTemplateConfigs": launch_template_config,
        "TargetCapacitySpecification": {
            "TotalTargetCapacity": target_capacity,
            "DefaultTargetCapacityType": "spot"
        },
        "SpotOptions": {
            "AllocationStrategy": allocation_strategy
            #"MaxTotalPrice": str(spot_price) if spot_price else None
        },
        "Type": "instant",
        "TagSpecifications" : [{
                'ResourceType': 'instance',
                'Tags':[{'Key': 'Name', 'Value': 'SpotFleet-SSCAD-FVBR'}]
        }]
    }

  
    response = ec2_client.create_fleet(**fleet_config)
    process_fleet_response(response=response)
    instance_ids = [inst for fleet in response.get("Instances", []) for inst in fleet["InstanceIds"]]

    
    if instance_ids:
        logging.info(f"Fleet created with instances: {instance_ids}")
    else:
        logging.warning("No instances were launched.")
        return []

    instances = list(ec2_resource.instances.filter(InstanceIds=instance_ids))
    for instance in instances:
        instance.wait_until_running()
        instance.reload()

    return instances
   

def run_via_ssh(cmd, instance, region):
    try:
        lifecycle = instance.instance_lifecycle if instance.instance_lifecycle else 'on-demand'

        instance_status, private_ip = get_status_ip(region=region, instance_id=instance.id)
        
        wait_for_ssh(private_ip)
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        

        c.connect(
            private_ip,
            #username="ubuntu",
            allow_agent=False,
            look_for_keys=True
        )

        logging.info(f"Running command: {cmd} in instance {instance.id} Region: {region} Market: {lifecycle}")

        stdin, stdout, stderr = c.exec_command(cmd)
        err = stderr.read().decode()
        output = stdout.read().decode()
        c.close()

        if err:
            print(f'Instância que deu erro: {instance.id} - {get_status_ip(region=region, instance_id=instance.id)}')
            return "", err
            
        return output, instance_status
    
    except Exception as e:
        logging.error(f"Error running command in instance {instance.id}: {e}")
        return "", e


def delayed_release(semaphore):
    print("Esperando 10 segundos..")
    time.sleep(10)
    semaphore.release()


def run_benchmark_on_instance(instance, app, region, is_spot, market, allocation_strategy, df, csv_file, lock, semaphore):
    instance_type = instance.instance_type
    thread_name = threading.get_ident()   

    semaphore.acquire()
    threading.Thread(target=delayed_release(semaphore=semaphore), daemon=True).start()
    print(f'Executando benchmark na instância {instance.id} na thread {thread_name}')
    
    for instance_verify in AWSConfig.INSTANCES_BY_REGION[region]:
        if instance_type in instance_verify:
            instance_core = instance_verify.split("-")[1]

    try:
        price = None 
        # if is_available(region, instance_type):
        #     price_spot = aws.get_price_spot(region, instance_type, region + AWSConfig.zone_letter)
        #     price_ondemand = aws.get_price_ondemand(region, instance_type)
        #     price = price_spot if is_spot else price_ondemand

        logging.info(f"Binding threads in cores, instance has {instance_core} cores")
        start_time = datetime.now()
        output, status = run_via_ssh(
            cmd=f'export OMP_PLACES=cores;export OMP_PROC_BIND=spread;export OMP_NUM_THREADS={instance_core};/u/fvbr/{app}', 
            instance=instance, 
            region=region)
    

        print('Execução Finalizada... Salvando no CSV!')

        row = {
            "Start_Time": start_time,
            "End_Time": datetime.now(),
            "Instance": instance_type,
            "InstanceID": instance.id,
            "Market": market,
            "Price": price,
            "Region": region,
            "Zone": instance.placement['AvailabilityZone'][-1:],
            "Algorithm_Name": app,
            "Allocation_Strategy": allocation_strategy,
            "Status": "SUCCESS" if output else f'FAILED:{status}'
        }

        with lock:
            save_row(output, row, df, csv_file)

    except Exception as e:
        logging.error(f"Erro ao executar benchmark na instância {instance.id}: {e}")
        row = {
            "Start_Time": start_time,
            "End_Time": datetime.now(),
            "Instance": instance_type,
            "InstanceID": None,
            "Market": market,
            "Price": None,
            "Region": region,
            "Zone": None,
            "Algorithm_Name": app,
            "Allocation_Strategy": allocation_strategy,
            "Status": 'FAILED'
        }
        _terminate_instance(instance)
        with lock:
            save_row('', row, df, csv_file)
    finally:
        _terminate_instance(instance)

    return None


def benchmark_par(args):
    region = args.region
    is_spot = True
    app = args.benchmark
    allocation_strategy = args.strategy
    nodes = int(args.nodes)

    benchmark_config = BenchmarkConfig()
    market = 'spot' if is_spot else 'ondemand'

    csv_file = Path(args.output_folder, f"./results/results_par_{region}_{allocation_strategy}.csv")

    if csv_file.exists():
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=benchmark_config.columns)

    
    instances = create_fleet(region, cluster_size=nodes, allocation_strategy=allocation_strategy, target_capacity=nodes)
    time.sleep(60)
     
    if not instances:
        logging.error("Falha ao iniciar instâncias.")
        return 0

    logging.info(f"Instâncias iniciadas: {instances}")
    
    csv_lock = threading.Lock()
    semaphore = threading.Semaphore(1)

    with ThreadPoolExecutor(max_workers=nodes, thread_name_prefix='Thread') as executor:
        futures = [
            executor.submit(
                run_benchmark_on_instance, instance, app, region, is_spot, market, allocation_strategy, df, csv_file, csv_lock, semaphore
            )
            for instance in instances
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Erro durante execução paralela: {e}")
                
    return None


def benchmark_seq(args):
    """
    :param region: Define the AWS region that the instance will be created
    :param availability_zone: Define the AWS availability zone that the instance will be created 
    :param repetitions: Number of executions inside the instance
    :return:
    """
    region = args.region
    #repetions = args.repetitions  
    is_spot = True
    app = args.benchmark
    #json_file = Path(args.json_file)
    allocation_strategy = args.strategy
    nodes = int(args.nodes)
    
    # if not json_file.exists():
    #     logging.error(f"File {json_file} not found")
    #     raise FileNotFoundError
    
    benchmark_config = BenchmarkConfig()
    market = 'spot' if is_spot else 'ondemand'    
    csv_file = Path(args.output_folder, f"./results/results_seq_{region}_{allocation_strategy}.csv")

    if csv_file.exists():
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=benchmark_config.columns)

    instances = []
    for i in range(nodes):
        instance = create_fleet(region,cluster_size=1, allocation_strategy=allocation_strategy, target_capacity=1)
        instances.append(instance[0])

    if instances:
        logging.info(f"Instâncias iniciadas: {instances}")
    else:
        logging.error("Falha ao iniciar instâncias.")
        return 0
   
    csv_lock = threading.Lock()
    semaphore = threading.Semaphore(1)

    with ThreadPoolExecutor(max_workers=nodes, thread_name_prefix='Thread') as executor:
        futures = [
            executor.submit(
                run_benchmark_on_instance, instance, app, region, is_spot, market, allocation_strategy, df, csv_file, csv_lock, semaphore
            )
            for instance in instances
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Erro durante execução paralela: {e}")
                
    return None
            



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmark AWS')
    parser.add_argument('region', type=str, help='AWS region')
    #parser.add_argument('json_file', type=str, help='Json file with instances configurations')
    parser.add_argument('strategy', type=str, default='lowest-price', choices=['lowest-price', 'diversified', 'capacity-optimized', 'capacity-optimized-prioritized', 'price-capacity-optimized'], help='Allocation Strategy')
    parser.add_argument('benchmark', type=str,default='ep.D.x', choices=['bt.A.x','bt.E.x','bt.C.x','bt.D.x','ua.D.x'])
    #parser.add_argument('--repetitions', type=int, default=5, help='Number of repetitions')
    parser.add_argument('--nodes', type=int, default=1, help='Number of repetitions')
    parser.add_argument('--output_folder', type=str, default='.', help='Output folder')
    #parser.add_argument('--spot', action='store_true', help='Use spot instances')
    parser.add_argument('--log', action='store_true', help='Log file')

    args = parser.parse_args()

    if args.log:
        # write the log in the stdout
        logging.basicConfig(level=logging.INFO,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                            format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
                            datefmt='%Y-%m-%d %H:%M:%S')
    else:
        logging.basicConfig(filename=f'{args.output_folder}/{args.region}_awsbench.log',  # Name of the log file
                            level=logging.INFO,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                            format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
                            datefmt='%Y-%m-%d %H:%M:%S')
        
    logging.info(f"Start execution in {args.region} Nodes={args.nodes} Benchmark={args.benchmark} Allocation Strategy={args.strategy}") 
    benchmark_par(args)
    
    #benchmark_seq(args)
    
    # try:
    #     subprocess.run(f'python terminate_all.py {args.region}', shell=True, check=True)
    #     logging.info(f"All instances have been terminated.")
    #     print("Terminated_all executed successfully.")
    # except subprocess.CalledProcessError as e:
    #     print(f"Error executing command: {e}")
