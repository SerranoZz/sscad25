from multi_cloud.impls.aws_provider import AWSProvider

class CloudProviderFactory:
    
    @staticmethod
    def get_provider(provider_name: str):
        if provider_name.lower() == "aws":
            return AWSProvider()
        else:
            raise ValueError(f"Provider '{provider_name}' não suportado.")