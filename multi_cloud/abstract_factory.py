from abc import ABC, abstractmethod

class AbstractCloudProvider(ABC):

    @abstractmethod
    def create_fleet(self, region, instances, allocation_strategy, target_capacity, tag):
        pass