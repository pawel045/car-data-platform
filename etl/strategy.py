from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime
import json
import pandas as pd
import requests
from fake_useragent import UserAgent


class ETLStrategy(ABC):
    @abstractmethod
    def extarct(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self, data):
        pass


class OtoMotoETL(ETLStrategy):
    def _get_url(self, brand, model, page=1):
        '''
        Return url in 4th diffrent configurations.
        If given: 
            brand and model; 
            only brand; 
            only model;
            no brand and no model.
        :param brand: string with name of a car brand e.g. Opel
        :param model: string with model of the car e.g. Astra
        :param page: int
        :return url: string from www.ototmoto.pl with brand/model filters.

        '''
        base_url = f"https://www.otomoto.pl/osobowe"

        if brand and model:
            return f"{base_url}/{brand}/{model}?page={page}" 
        if brand:
            return f"{base_url}/{brand}?page={page}"
        if model:
            return f"{base_url}/{model}?page={page}"
        
        return base_url + f'?page={page}'

    def _get_headers(self):
        '''
        Prepare headers with random useragent for scraping 
        Check "Custom Headers"-> https://requests.readthedocs.io/en/latest/user/quickstart/ 
        :return headers: dictionary with configurations
        '''
        try:
            ua = UserAgent(platforms=['desktop', 'mobile'])
            user_agent = ua.random
        except Exception as e:
            raise RuntimeError(f"Failed to generate a random User-Agent: {e}")
                
        headers = {
            'User-Agent': user_agent,
        }
        return headers

    def _get_data_with_key(self, json_object, key):
        '''
        Json_object parser, depends of key.
        '''
        try:
            # Access nested JSON structure
            urql_state = json_object.get('props', {}).get('pageProps', {}).get('urqlState', {})
            
            if key not in urql_state:
                raise KeyError(f"Key '{key}' not found in 'urqlState'.")

            # Parse the JSON data associated with the key
            data_json = json.loads(urql_state[key]['data'])
            
            # Extract the required 'advertSearch' edges
            final_data = data_json.get('advertSearch', {}).get('edges', [])
            return final_data

        except (KeyError, TypeError) as e:
            # Handle missing keys or unexpected JSON structure
            raise KeyError(f"Error accessing data with key '{key}': {e}")
        except json.JSONDecodeError as e:
            # Handle JSON decoding errors
            raise ValueError(f"Error decoding JSON data for key '{key}': {e}")

    def _create_row_from_dict(self, input_dict):
        """
        Transforms an input dictionary into a structured row dictionary.
        :param input_dict: input data (dict) containing a 'node' key with relevant details.
        :return row: a flattened dictionary with extracted and mapped fields.
        """
        try:
            node_dict = input_dict.get('node', {})
            row = {}

            # Metadata
            row['scrape_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row['created_date'] = datetime.strptime(
                node_dict.get('createdAt', "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"
            )

            # Static mappings
            row['title'] = node_dict.get('title', "")
            row['short_description'] = node_dict.get('shortDescription', "")
            
            # Price and currency
            price_info = node_dict.get('price', {}).get('amount', {})
            row['price'] = price_info.get('units', 0)
            row['currency'] = price_info.get('currencyCode', "UNKNOWN")
            
            # Cepik verification
            row['cepik_verified'] = node_dict.get('cepikVerified', False)

            # Dynamic parameters
            parameters = node_dict.get('parameters', [])
            for param in parameters:
                key = param.get('key')
                value = param.get('value')
                if key:  # Only add valid keys
                    row[key] = value

            return row

        except Exception as e:
            # Log the error or handle it appropriately
            print(f"Error processing input_dict: {e}")
            return {}

    def _create_df_from_rows(self):
        pass

    def extarct(self, brand:str, model:str):
        # Code to scrape data from otomoto.pl

        # Prepare prerequisits
        headers = self._get_headers()
        url = self._get_url(brand, model)

        # Send a GET request to the URL
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to fetch page: {response.status_code}")
            return
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all car listings
        listings = soup.find(id='__NEXT_DATA__')

        # Check if listings were found
        if not listings:
            print("No car listings found. The website structure may have changed.")
            return
        
        # Extract data from listing
        json_object = json.loads(str(listings.text))

        last_key = list(json_object.get('props', {}).get('pageProps', {}).get('urqlState', {}).keys())[-1]
        first_key = list(json_object.get('props', {}).get('pageProps', {}).get('urqlState', {}).keys())[0]

        with open('test.txt', 'w') as f:
            f.write(str(json_object))

        try:
            final_data = self._get_data_with_key(json_object, last_key)
        except KeyError:
            try:
                final_data = self._get_data_with_key(json_object, first_key)
            except:
                print("No car data in here :(")

        # Save extracted data to df
        df = None
        for input_data in final_data:
            row = self._create_row_from_dict(input_data)
            if df is None:
                df = pd.DataFrame([row])
            else:
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

        return df
    
    def transform(self, data):
        # code to tranform data compliant schema
        return

    def load(self, data):
        # code to load transformed data to bigquery
        return
    

class ETLContext:
    def __init__(self, strategy: ETLStrategy):
        self.strategy = strategy

    def run(self):
        data = self.strategy.extarct()
        transformed_data = self.strategy(data)
        self.strategy(transformed_data)


if __name__=='__main__':
    from IPython.display import display

    etl = OtoMotoETL()
    df = etl.extarct(brand='porsche', model='cayenne')
    df.to_csv('extract_data.csv')
    display(df)
