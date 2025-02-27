from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime
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
            return f"{base_url}/{brand}/{model}?page={page}" 
        if brand:
            return f"{base_url}/{brand}?page={page}"
        if model:
            return f"{base_url}/{model}?page={page}"
        
        return base_url + f'?page={page}'

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
        :return row: a flattened dictionary with extracted and mapped fields.
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

            return row

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

    def _set_dtype(self, df:pd.DataFrame, col:str, dtype:str):
        '''
        Change dtype in column's DataFrame. 
        If dtype is numeric but value in column is not numeric - change to NaN.
        :param df:
        :param col: str columna name of df
        :param dtype: accroding to https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
        '''
        try:
            df.astype({col: dtype})
        except ValueError:
            if dtype in ['int', 'float']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else: 
                raise ValueError
        return df

    def _find_page_num(self, text:str) -> int:
        """
        Extracts the number of ads from the given text. Raises ValueError if not found.
        """
        match = re.search(r'Liczba ogłoszeń:\s*<!--\s*-->\s*<b>([\d\s]+)</b>', text)
        if not match:
            raise ValueError("Could not find the number of ads in the given text.")
        
        ad_num = int(match.group(1).replace(" ", ""))  # Remove spaces and convert to int
        return ceil(ad_num/32) # 32 ads on page

    def extarct(self, brand:str, model:str):
        # Code to scrape data from otomoto.pl

        # Prepare prerequisits
        headers = self._get_headers()
        url = self._get_url(brand, model)
        df = None

        # Get init soup and number of pages
        soup = self._get_soup(url, headers)
        num_of_pages = self._find_page_num(str(soup))

        # Loop through pages
        for page_num in range(1, num_of_pages+1):
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Extract data for: {brand} {model}. Page number: {page_num}")

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
                row = self._create_row_from_dict(input_data)
                df = self._create_df_from_row(df, row)

        return df
    
    def transform(self, df):
        # code to tranform data compliant schema
        '''
        * set datatypes: _set_dtype
        * create new rows e.g. Car_year, price_pln, 
        * solve NaN
        '''

        return

    def load(self, data):
        # code to load transformed data to bigquery
        return
    

class ContextManager:
    def __init__(self, strategy: ETLStrategy):
        self.strategy = strategy

    def run(self):
        data = self.strategy.extarct()
        transformed_data = self.strategy(data)
        self.strategy(transformed_data)


if __name__=='__main__':
    
    from IPython.display import display
    etl = OtoMotoETL()
    df = etl.extarct(brand='opel', model='antara')
    display(df)
