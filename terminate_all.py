import re
import boto3 # type: ignore
import subprocess
import argparse

def terminate_all(region):
    command = f'aws ec2 describe-instances --filters "Name=tag:Name,Values=SpotFleet-SSCAD" "Name=instance-state-name,Values=running" --query "Reservations[*].Instances[*].InstanceId" --output text'
    session = boto3.Session(region_name=region)
    ec2_client = session.client("ec2")

    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True)
        input_string = result.stdout.decode() if isinstance(result.stdout, bytes) else str(result.stdout)
        instances_ids = re.findall(r'\bi-[0-9a-f]{17}\b', input_string)

        if(instances_ids):
            print(f"Terminating {len(instances_ids)} instances...")
            response = ec2_client.terminate_instances(
                InstanceIds=instances_ids,
            )
            print(f"{len(instances_ids)} instances terminated")


        else:
            print(f'No running instances found.')
        
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmark AWS')
    parser.add_argument('region', type=str, help='AWS region')

    args = parser.parse_args()

    terminate_all(args.region)


