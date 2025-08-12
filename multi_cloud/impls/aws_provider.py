import logging
from multi_cloud.abstract_factory import AbstractCloudProvider
import boto3 # type: ignore

class AWSProvider(AbstractCloudProvider):

    SUBNET_IDS_BY_REGION = {
        'us-east-1': {
            'us-east-1a': "subnet-04947f6ee876adbee",
            'us-east-1b': "subnet-0801c911a8382154c",
            'us-east-1c':"subnet-05b8c880471ae8896",
            'us-east-1d':"subnet-00b0c725891d75bf3",
            'us-east-1e':"subnet-0d6e9e447317b1338",
            'us-east-1f':"subnet-07058ff843053f6cb"
            }
        ,
        'sa-east-1': {
            'sa-east-1a':"subnet-06ab42c6dc587e683",
            'sa-east-1b':"subnet-009fcfba8494f260f",
            'sa-east-1c':"subnet-0d8144caee75d637a"
        }
        
    }


    def create_fleet(self, region, instances, allocation_strategy, target_capacity, tag):
        session = boto3.Session(region_name=region)

        ec2_client = session.client("ec2")
        ec2_resource = session.resource("ec2")

        overrides = self._instance_template_config(instances)
        print('over',overrides)

        launch_template_config = [
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": "TCLaunchTemplate",
                    "Version": "$Default"
                },
                "Overrides": overrides  
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
                    'Tags':[{'Key': 'Name', 'Value': tag}]
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


    def _instance_template_config(self, instances):
        overrides = []

        if isinstance(instances, str):
            instances = [instances]
        elif isinstance(instances, tuple):
            instances = [instances]

        for inst_str in instances:
            inst_str = inst_str if isinstance(inst_str, str) else inst_str[0]
            instance_type,region,az = inst_str.split('-')

            region = 'sa-east-1' if region == 'sa' else 'us-east-1'
            region_az = region + az

            overrides.append({
                'InstanceType': instance_type,
                'SubnetId': self.SUBNET_IDS_BY_REGION[region][region_az]
            })

        return overrides
    