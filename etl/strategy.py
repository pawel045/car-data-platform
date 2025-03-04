from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
from math import ceil
import pandas as pd
import re
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
    def _get_url(self, brand, model, page=1) -> str:
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
            return f"{base_url}/{brand}/{model}?search%5Border%5D=created_at_first%3Adesc&page={page}" 
        if brand:
            return f"{base_url}/{brand}?search%5Border%5D=created_at_first%3Adesc&page={page}"
        if model:
            return f"{base_url}/{model}?search%5Border%5D=created_at_first%3Adesc&page={page}"
        
        return base_url + f'?search%5Border%5D=created_at_first%3Adesc&page={page}'

    def _get_headers(self) -> dict:
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

    def _get_soup(self, url, headers) -> BeautifulSoup:
        """
        Fetches the HTML content from the given URL and parses it using BeautifulSoup.
        :param url: str, the URL to fetch.
        :param headers: dict, the headers to include in the request.
        :return: BeautifulSoup, parsed HTML content if successful, otherwise None.
        """
        # Send a GET request to the URL
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to fetch page: {response.status_code}")
            return None
        
        # Parse the HTML content using BeautifulSoup
        return BeautifulSoup(response.content, 'html.parser')

    def _get_data_with_key(self, json_object, key) -> dict:
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

    def _create_row_from_dict(self, input_dict) -> dict:
        """
        Transforms an input dictionary into a structured row dictionary.
        :param input_dict: input data (dict) containing a 'node' key with relevant details.
        :return row, creation_date: a flattened dictionary with extracted and mapped fields and just a creation date.
        """
        try:
            node_dict = input_dict.get('node', {})
            row = {}

            # Metadata - scrape data and creating advertisement
            row['scrape_date'] = datetime.now().strftime("%Y-%m-%d")
            created_date = node_dict.get('createdAt', "1900-01-01")
            row['created_date'] = datetime.strptime(created_date[0:10], "%Y-%m-%d")

            # Title and description
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

            return row, row['created_date']

        except Exception as e:
            # Log the error or handle it appropriately
            print(f"Error processing input_dict: {e}")
            return {}

    def _create_df_from_row(self, df, row) -> pd.DataFrame:
        '''
        Add a new row to DataFrame. If DataFrame doesnt exist create it.
        :param df: input DataFrame or None
        :param row: a flattened dictionary with extracted and mapped fields
        :return: DataFrame with added row.
        '''
        # Create a new DataFrame or append to the existing one
        if df is None:
            return pd.DataFrame([row])
        
        return pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    def _find_page_num(self, text:str) -> int:
        """
        Extracts the number of ads from the given text. Raises ValueError if not found.
        """
        match = re.search(r'Liczba ogłoszeń:\s*<!--\s*-->\s*<b>([\d\s]+)</b>', text)
        if not match:
            raise ValueError("Could not find the number of ads in the given text.")
        
        ad_num = int(match.group(1).replace(" ", ""))  # Remove spaces and convert to int
        return ceil(ad_num/32) # 32 ads on page

    def _is_stop_date_greater_than_creation_date(self, creation_date:datetime, stop_date:datetime) -> bool:
        return stop_date > creation_date

    def _n_days_ago(self, n_days: int) -> datetime:
        assert n_days>0, 'n_days must be a positive number'

        n_days_ago = datetime.now() - timedelta(days=n_days)
        return n_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
         
    def extarct(self, **kwargs) -> pd.DataFrame:
        # Code to scrape data from otomoto.pl
        assert 'brand' in kwargs, 'There is no definition of brand (if dont wanna brand, set as empty string).'
        assert 'model' in kwargs, 'There is no definition of model (if dont wanna model, set as empty string).'
        assert 'days_ago' in kwargs, 'There is no definition of days_ago (if dont wanna days_ago, set as -1).'

        brand = kwargs['brand']
        model = kwargs['model']
        days_ago = kwargs['days_ago']

        # decide if get all data or limited by date
        stop_date=None
        if days_ago>=0:
            stop_date = self._n_days_ago(days_ago)

        # Prepare prerequisits
        headers = self._get_headers()
        url = self._get_url(brand, model)
        df = None

        # Get init soup and number of pages
        soup = self._get_soup(url, headers)
        num_of_pages = self._find_page_num(str(soup))

        # Loop through pages
        for page_num in range(1, num_of_pages+1):
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Extracting data of: {brand.title()} {model.title()}. Page number: {page_num}")

            if page_num > 1: # If more than 1 page
                url = self._get_url(brand, model, page=page_num)
                soup = self._get_soup(url, headers)

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
            for input_data in final_data:
                row, creation_date = self._create_row_from_dict(input_data)

                # Stop for-loop if stop_date>creation_date
                if stop_date:
                    if self._is_stop_date_greater_than_creation_date(creation_date, stop_date):
                        return df

                df = self._create_df_from_row(df, row)

        return df
    
    def transform(self, df):
        # code to tranform data compliant schema
        '''
        * set datatypes: _set_dtype
        * create new rows e.g. Car_year, price_pln
        * solve NaN
        '''

        print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Transforming data.")

        # SET DTYPES PROPERLY
        col_dtype = {
            'title': 'str',  
            'short_description': 'str',
            'price': 'int',
            'currency': 'str',
            'cepik_verified': 'bool',
            'make': 'str',
            'fuel_type': 'str',
            'gearbox': 'str',
            'country_origin': 'str',
            'mileage': 'int',
            'engine_capacity': 'int',
            'engine_power': 'int',
            'model': 'str',
            'version': 'str',
            'year': 'int',
        }
        df = df.astype(col_dtype)
        df[['scrape_date', 'created_date']] = df[['scrape_date', 'created_date']].apply({
            'scrape_date': pd.to_datetime,
            'created_date': pd.to_datetime
        })
        
        # RENAME COLUMNS
        mapper = {
            'make': 'brand'
        }
        df.rename(mapper, axis=1, inplace=True)

        # CREATE ROWS
        df['car_age'] = df['scrape_date'].dt.year - df['year']

        return df

    def load(self, data):
        print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Loading data.")
        # save loccaly as csv, for now
        data.to_csv('extracted_data.csv')
        
        # code to load transformed data to bigquery
        return
    

class ContextManager:
    def __init__(self, strategy: ETLStrategy):
        self.strategy = strategy
        self.params = {}

    def set_params(self, **kwargs):
        for key, value in kwargs.items():
            self.params[key] = value

    def run(self):
        try:
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Starting process: {self.strategy.__class__.__name__}")
            data = self.strategy.extarct(**self.params)
            transformed_data = self.strategy.transform(data)
            self.strategy.load(transformed_data)
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] {self.strategy.__class__.__name__} process completed.")
        except Exception as err:
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Process interrupted: {err}")




if __name__=='__main__':
    params = {
        'brand': 'porsche',
        'model': 'cayenne',
        'days_ago': 1,
    }

    etl = OtoMotoETL()
    context = ContextManager(etl)
    context.set_params(**params)
    context.run()
