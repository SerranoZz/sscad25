import requests # type: ignore
import logging


def update_price_pricing_server(type, region, market):
        '''
        Update the price per hour of the VM from the local pricing server
        '''
        try:
            #logging.info(f"Without Proxy test.3")
            session = requests.Session()
            session.trust_env = False
            # add timeout of 10 seconds
            response = session.get(f"http://midas.petrobras.com.br/pricing/get_price", params={
                'type': type,
                'region': region,
                'market': market,
                'provider': "aws"
            })
            #response = session.get(f"{Servers.pricing_server_url}/get_price", params={
            #    'type': self.type,
            #    'region': self.region,
            #    'market': self.market,
            #    'provider': self.provider
            #})
            
            if response.status_code == 200:
                data = response.json()
                price_per_hour = float(data['price'])
                # if VM is spot, get the price per AZ (dictionary)
                if market == 'spot' and 'prices_spot' in data:
                   
                    price_az = data['prices_spot']

                return price_per_hour, price_az
            else:
                logging.error(f"Failed to retrieve price for {type}/{market} from {region}. Status code: {response.status_code}")                
        except Exception as e:
            #logging.error(f"Error while retrieving price for {self.type}/{self.market} from {self.region} in pricing_server: {e}")
            raise e
        







