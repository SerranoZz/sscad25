#!/usr/bin/env python3

import aws_bench.pricing_handler as aws
from aws_bench.constants import AWSConfig, SSHConfig, BenchmarkConfig
from aws_bench import get_price
import boto3  # type: ignore
from botocore.exceptions import ClientError # type: ignore
import paramiko # type: ignore
import time
from datetime import datetime
from pathlib import Path
import argparse
import pandas as pd # type: ignore
import csv
from datetime import datetime
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import socket
import subprocess
from threading import Lock

def get_price_spot(region, instance, zone):
    session = boto3.Session(region_name=region)
    ec2_client = session.client("ec2")

    now = datetime.now()
    try:  
        response = ec2_client.describe_spot_price_history(
            StartTime=datetime(now.year, now.month, now.day, now.hour, now.minute, now.second),
            InstanceTypes=[instance],
            ProductDescriptions=['Linux/UNIX'],
            AvailabilityZone=zone
        )
        
        if response['SpotPriceHistory']:
            latest_price = response['SpotPriceHistory'][0]
            logging.info(f'{latest_price["InstanceType"]} - {latest_price["SpotPrice"]} - {latest_price["AvailabilityZone"]}')
            return latest_price["SpotPrice"]
        else:
            logging.warning("No spot price history found.")
            return None

    except Exception as e:
        logging.error(f"Error getting spot price: {e}")
        return None
    

def get_price_aws(region,zone):
    session = boto3.Session(region_name=region)
    ec2_client = session.client("ec2")

    instances = []
    now = datetime.now()
    for instance in AWSConfig.INSTANCES_BY_REGION[region]:
        instances.append(instance.split("-")[0])

    try:  
        response = ec2_client.describe_spot_price_history(
            StartTime=datetime(now.year, now.month, now.day, now.hour, now.minute, now.second),
            InstanceTypes=instances,
            AvailabilityZone=zone,
            ProductDescriptions=['Linux/UNIX']
        )

        if response['SpotPriceHistory']:
            result = [
                {
                    'InstanceType': item['InstanceType'],
                    'AvailabilityZone': item['AvailabilityZone'],
                    'SpotPrice': float(item['SpotPrice'])
                }
                for item in response['SpotPriceHistory']
            ]

            sorted_result = sorted(result, key=lambda x: x['SpotPrice'])

            # Caminho do CSV
            filename = f"./results/spot_prices_{region}.csv"
            with open(filename, mode='w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['InstanceType', 'AvailabilityZone', 'SpotPrice'])
                writer.writeheader()
                writer.writerows(sorted_result)

            logging.info(f"Spot price data saved to {filename}")
        else:
            logging.warning("No spot price history found.")
            return None

    except Exception as e:
        logging.error(f"Error getting spot price: {e}")
        return None
    

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


def heuristic(region):
    instance_price = {}
    instances_sa = AWSConfig.INSTANCES_BY_REGION['sa-east-1']
    instances_us = AWSConfig.INSTANCES_BY_REGION['us-east-1']


    for instance in instances_sa:
        type = instance.split("-")[0] 
        type_region = type + '-sa'
        price, price_az = get_price.update_price_pricing_server(type, 'sa-east-1', 'spot')
        if price is not None and price_az is not None:
            instance_price[type_region] = (price, price_az)

    for instance in instances_us:
        type = instance.split("-")[0] 
        type_region = type + '-us' 
        price, price_az = get_price.update_price_pricing_server(type, 'us-east-1', 'spot')
        if price is not None and price_az is not None:
            instance_price[type_region] = (price, price_az)

    if not instance_price:
        return None  


    sorted_instances = sorted(instance_price.items(), key=lambda item: item[1][0])


    #print(sorted_instances)

    return sorted_instances  # Lista de tuplas: (type, (price, price_az))


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


def create_fleet_aws(region, cluster_size, allocation_strategy, target_capacity):
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
    #process_fleet_response(response=response)
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


def create_fleet(instance_type, region, cluster_size, allocation_strategy, target_capacity):
    """
    Cria uma frota de instâncias EC2 usando `create_fleet`.

    :param region: Região AWS onde a frota será criada
    :param cluster_size: Número de instâncias a serem lançadas
    :param allocation_strategy: Estratégia de alocação (ex: 'lowest-price', 'capacity-optimized')
    :param target_capacity: Capacidade alvo da frota
    :return: Lista de instâncias iniciadas
    """
    instances = []
    for instance in instance_type:
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
    

    instance_ids = [inst for fleet in response.get("Instances", []) for inst in fleet["InstanceIds"]]

    errors = [error['ErrorCode'] for error in response.get('Errors', [])]
 
            
    if instance_ids:
        logging.info(f"Fleet created with instances: {instance_ids}")
    else:
        logging.warning("No instances were launched.")
        return [],errors

    instances = list(ec2_resource.instances.filter(InstanceIds=instance_ids))
    for instance in instances:
        instance.wait_until_running()
        instance.reload()

    return instances, errors
   

ssh_lock = Lock()


def run_via_ssh(cmd, instance, region, max_retries=5, delay=10, command_timeout=3600, keepalive_interval=30):
    """
    Executa um comando remoto via SSH de forma thread-safe com suporte a execuções longas
    
    Args:
        cmd (str): Comando a ser executado
        instance: Objeto da instância EC2
        region (str): Região AWS
        max_retries (int): Número máximo de tentativas
        delay (int): Atraso entre tentativas (segundos)
        command_timeout (int): Timeout para execução do comando
        keepalive_interval (int): Intervalo de keepalive SSH
        
    Returns:
        str: Saída do comando ou None em caso de falha
    """
    def read_ssh_output(stdout, stderr, read_timeout=30):
        """Função auxiliar para leitura robusta do output"""
        output = []
        start_time = time.time()
        
        while True:
            # Lê stdout se houver dados disponíveis
            if stdout.channel.recv_ready():
                output.append(stdout.channel.recv(65536).decode('utf-8', errors='replace'))
            
            # Verifica se o comando terminou
            if stdout.channel.exit_status_ready():
                # Lê qualquer dado remanescente
                while stdout.channel.recv_ready():
                    output.append(stdout.channel.recv(65536).decode('utf-8', errors='replace'))
                break
            
            # Timeout para evitar loops infinitos
            if time.time() - start_time > read_timeout:
                raise paramiko.SSHException(f"Timeout de leitura ({read_timeout}s) ao aguardar output")
            
            # Pequena pausa para evitar uso excessivo da CPU
            time.sleep(0.1)
        
        return ''.join(output)

    attempt = 0
    last_exception = None
    
    lifecycle = instance.instance_lifecycle if instance.instance_lifecycle else 'on-demand'
    instance_id = instance.id

    while attempt < max_retries:
        client = None
        try:
            # Obtém status e IP da instância
            instance_status, private_ip = get_status_ip(region=region, instance_id=instance_id)
            if not private_ip:
                raise ValueError(f"IP privado não disponível para instância {instance_id}")
                
            wait_for_ssh(private_ip)

            # Configuração do cliente SSH com lock para thread-safety
            with ssh_lock:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # Conexão com timeouts explícitos
                client.connect(
                    hostname=private_ip,
                    allow_agent=True,
                    look_for_keys=True,
                    timeout=30,
                    banner_timeout=30
                )
                
                # Configura keepalive para evitar timeout de conexão
                transport = client.get_transport()
                transport.set_keepalive(keepalive_interval)

            logging.info(f"Running command in {instance_id} ({region}, {lifecycle}): {cmd}")

            # Execução do comando com PTY para melhor tratamento de streams
            stdin, stdout, stderr = client.exec_command(
                command=cmd,
                timeout=command_timeout,
                get_pty=True
            )
            
            # Leitura robusta do output
            output = read_ssh_output(stdout, stderr, read_timeout=command_timeout)
            
            print(output)

            # Verifica o status de saída e erros
            exit_status = stdout.channel.recv_exit_status()
            err_output = read_ssh_output(stderr, stdout, read_timeout=5)  # Timeout menor para stderr

            if exit_status != 0 or err_output:
                logging.error(f"Command failed in {instance_id}. Exit status: {exit_status}, Error: {err_output}")
                return None
            
            # Limpeza de caracteres e verificação de integridade
            output = output.replace('\r\n', '\n').replace('\r', '\n')
            if  "SUCCESSFUL" not in output:
                logging.warning(f"Possible truncated output in {instance_id}")
            
            return output

        except Exception as e:
            last_exception = e
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for {instance_id}: {str(e)}")
            attempt += 1
            
            if attempt < max_retries:
                time.sleep(delay * attempt)  # Backoff exponencial
        finally:
            # Fecha conexão se ainda estiver aberta
            if client is not None:
                try:
                    if client.get_transport() and client.get_transport().is_active():
                        client.close()
                except Exception as e:
                    logging.warning(f"Error closing SSH connection for {instance_id}: {str(e)}")

    logging.error(f"All attempts failed for {instance_id}. Last error: {str(last_exception)}")
    return None


def benchmark(args):
    """
    :param region: Define the AWS region that the instance will be created
    :param availability_zone: Define the AWS availability zone that the instance will be created 
    :param repetitions: Number of executions inside the instance
    :return:
    """
    region = args.region
    #repetions = args.repetitions  
   
    app = args.benchmark
    allocation_strategy = args.strategy
    nodes = int(args.nodes)
    
        
    benchmark_config = BenchmarkConfig()
    market = 'spot'    
    csv_file = Path(args.output_folder, f"results_{region}.csv")

    if csv_file.exists():
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=benchmark_config.columns)

    # iterate over the instances
    
    start_time = datetime.now()

    
    price = None 

    fail = True
    index = 0
    while fail:
    
        instaces_sorted_by_price = heuristic(region=region) 

        print(instaces_sorted_by_price)


        instance_type = [instaces_sorted_by_price[index][0].split('-')[0]]
        region = [instaces_sorted_by_price[index][0].split('-')[1]]
        
        price = instaces_sorted_by_price[index][1][1]
        print(instance_type)
        print(region)



        instance, errors = create_fleet(instance_type=instance_type, region=region , cluster_size=nodes, allocation_strategy=allocation_strategy, target_capacity=nodes)

        

        for error in errors:
            row = { "Start_Time": start_time,
            "End_Time": datetime.now(),
            "Instance": instance_type,
            "InstanceID": None,
            "Market": market,
            "Price": price,                
            "Region": region,
            "Zone": None,
            "Algorithm_Name": 'NAS Benchmark',
            "Status": error}
            df = save_row('', row, df, csv_file) 

        # if instance is not None, we can run the benchmark
        if instance:
            instance = instance[0]
            fail = False
            time.sleep(5)
            execution_count = 0
            logging.info(f"Try Running!")

            #binding_threads = 'export GOMP_CPU_AFFINITY="' + ' '.join(str(i) for i in range(instance_core)) + '"'

            #logging.info(f"Execution {execution_count + 1} of {repetions}")
            output = run_via_ssh(cmd=f'/u/fvbr/{app}', instance=instance,
                                    region=region)
            
            row = {"Start_Time": start_time,
                    "End_Time": datetime.now(),
                    "Instance": instance_type,
                    "Market": market,
                    "Price":  price,                               
                    "Region": region,
                    "Zone": instance.placement['AvailabilityZone'][-1:],
                    "Algorithm_Name": 'NAS Benchmark',
                    "Status": 'SUCCESS'}
            
            print(row)
            
            df = save_row(output, row, df, csv_file)
            

        else:
            fail = True
            index += 1
            

def benchmark_parallel(args):
    region = args.region
    app = args.benchmark
    allocation_strategy = args.strategy
    nodes = int(args.nodes)

    benchmark_config = BenchmarkConfig()
    market = 'spot'
    csv_file = Path(args.output_folder, f"./results/results_par_{region}_{allocation_strategy}.csv")

    if csv_file.exists():
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=benchmark_config.columns)

    lock = threading.Lock()

    start_time = datetime.now()
    instances = []
    index = 0

    while len(instances) < nodes:

        instaces_sorted_by_price = heuristic(region=region)
       
       
        print(instaces_sorted_by_price)

        instance_type = [instaces_sorted_by_price[index][0].split('-')[0]]
        region = instaces_sorted_by_price[index][0].split('-')[1]
        
        price = instaces_sorted_by_price[index][1][1]

        #print(instance_type)

        if region == 'sa':
            region = 'sa-east-1'
        else:
            region = 'us-east-1' 
        
        #instance_type = [instaces_sorted_by_price[index][0]]
        #price = instaces_sorted_by_price[index][1][1]

        needed = nodes - len(instances)  
        logging.info(f"Faltam {needed} instâncias. Tentando criar {needed} do tipo {instance_type[0]}.")

        new_instances, errors = create_fleet(
            instance_type=instance_type,
            region=region,
            cluster_size=needed,
            allocation_strategy=allocation_strategy,
            target_capacity=needed
        )

    

        print(f'Number of instances launched now: {len(new_instances)}')

        for error in errors:
            row = {
                "Start_Time": start_time,
                "End_Time": datetime.now(),
                "Instance": instance_type,
                "InstanceID": None,
                "Market": market,
                "Price": price,
                "Region": region,
                "Zone": None,
                "Algorithm_Name": app,
                "Allocation_Strategy": allocation_strategy,
                "Status": error
            }
            with lock:
                df = save_row('', row, df, csv_file)

        if new_instances:
            instances.extend(new_instances)
            logging.info(f"Total de instâncias acumuladas: {len(instances)}/{nodes}")

        else:
            index += 1  # Se não conseguiu nada, tenta próximo tipo

    logging.info(f"{len(instances)} instâncias no total. Iniciando benchmark em paralelo...")

    def worker(instance):
        instance_start_time = datetime.now()
        output = run_via_ssh(cmd=f'/u/fvbr/{app}', instance=instance, region=region)

        print(output)
        
        if output is None:
            status = 'FAIL'
        else:
            if 'SUCCESSFUL' in output:
                status = 'SUCCESS'
            else:
                status = 'REVOCATION'
        row = {
            "Start_Time": instance_start_time,
            "End_Time": datetime.now(),
            "Instance": instance.instance_type,
            "Market": market,
            "Price": price,
            "Region": region,
            "Zone": instance.placement['AvailabilityZone'][-1:],
            "Algorithm_Name": 'NAS Benchmark',
            "Allocation_Strategy": allocation_strategy,
            "Status": status
        }
        with lock:
            save_row(output or '', row, df, csv_file)

    with ThreadPoolExecutor(max_workers=nodes) as executor:
        futures = [executor.submit(worker, instance) for instance in instances]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in worker: {e}")


def benchmark_parallel_aws(args):
    region = args.region
    app = args.benchmark
    allocation_strategy = args.strategy
    nodes = int(args.nodes)

    benchmark_config = BenchmarkConfig()
    market = 'spot'
    csv_file = Path(args.output_folder, f"./results/results_par_{region}_{allocation_strategy}.csv")

    if csv_file.exists():
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=benchmark_config.columns)

    lock = threading.Lock()


    instances = create_fleet_aws(region, cluster_size=nodes, allocation_strategy=allocation_strategy, target_capacity=nodes)

    logging.info(f"{len(instances)} instâncias no total. Iniciando benchmark em paralelo...")

    def worker(instance):

        instance_start_time = datetime.now()
        output = run_via_ssh(cmd=f'/u/fvbr/{app}', instance=instance, region=region)

        print(output)
        
        if output is None:
            status = 'FAIL'
        else:
            if 'SUCCESSFUL' in output:
                status = 'SUCCESS'
            else:
                status = 'REVOCATION'

        zone = region + instance.placement['AvailabilityZone'][-1:]
        price = get_price_spot(region=region,instance=instance.instance_type ,zone=zone)
        
        row = {
            "Start_Time": instance_start_time,
            "End_Time": datetime.now(),
            "Instance": instance.instance_type,
            "Market": market,
            "Price": price,
            "Region": region,
            "Zone": instance.placement['AvailabilityZone'][-1:],
            "Algorithm_Name": app,
            "Allocation_Strategy": allocation_strategy,
            "Status": status
        }
        with lock:
            save_row(output or '', row, df, csv_file)

    with ThreadPoolExecutor(max_workers=nodes) as executor:
        futures = [executor.submit(worker, instance) for instance in instances]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in worker: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmark AWS')
    parser.add_argument('region', type=str, help='AWS region')
    #parser.add_argument('json_file', type=str, help='Json file with instances configurations')
    parser.add_argument('strategy', type=str, default='lowest-price', choices=['lowest-price', 'diversified', 'capacity-optimized', 'capacity-optimized-prioritized', 'price-capacity-optimized'], help='Allocation Strategy')
    parser.add_argument('benchmark', type=str,default='ep.D.x', choices=['bt.A.x','bt.E.x','bt.C.x', 'bt.D.x'])
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
    
    
    benchmark_parallel(args)

    #get_price_aws(args.region)

    try:
        subprocess.run(f'python terminate_all.py sa-east-1', shell=True, check=True)
        subprocess.run(f'python terminate_all.py us-east-1', shell=True, check=True)
        logging.info(f"All instances have been terminated.")
        print("Terminated_all executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
