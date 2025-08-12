from multi_cloud.abstract_factory import AbstractCloudProvider

class AWSProvider(AbstractCloudProvider):

    def create_fleet(self, region, instances, allocation_strategy, target_capacity, tag):
        return