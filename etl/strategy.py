from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from math import ceil
import numpy as np
import os
import pandas as pd
from random import randint
import re
import requests
from fake_useragent import UserAgent
from time import sleep


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
    def __init__(self):
        # Retrieve the values from environment variables
        load_dotenv()
        project_id = os.getenv('PROJECT_ID')
        dataset_id = os.getenv('DATASET_ID')
        table_id = os.getenv('TABLE_ID')
        key_path = os.getenv('SERVICE_ACCOUNT_KEY_PATH')

        # Get credentials
        credentials = service_account.Credentials.from_service_account_file(key_path)

        # Create BigQuery client and table
        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"

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

    def _get_max_id(self) -> int:
        # Run the query
        query = f"SELECT MAX(id) AS max_value FROM `{self.table_ref}`"
        query_job  = self.client.query(query)

        result = query_job.result()
        for row in result:
            max_value = row.max_value  # Access the max_value column
        
        if not max_value:
            return -1
        return max_value

    def _validate_dataframe(self, df, expected_columns):
        '''
        Dataframe validation - check if expected columns are in df.
        :param df: pd.DataFrame to validate,
        :param expected_columns: list,
        :return: dict
            {
            "missing_columns": list,
            "extra_columns": list,
            "correct_order": bool
            }
        '''


        actual_columns = df.columns.tolist()

        # Check if all expected columns are present
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        extra_columns = [col for col in actual_columns if col not in expected_columns]
        
        # Check if columns are in the correct order
        correct_order = actual_columns == expected_columns

        return {
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "correct_order": correct_order
        }

    def extarct(self, **kwargs):
        # Code to scrape data from otomoto.pl

        # external params (created at context menager)
        brand = kwargs.get('brand', '')
        model = kwargs.get('model', '')
        days_ago = kwargs.get('days_ago', -1)
        delay_scraping = kwargs.get('delay_scraping', False)

        # internal params (created in run_etl)
        page_num_start = kwargs['page_num_start'] # include
        page_num_stop = kwargs['page_num_stop'] # include
        headers = kwargs['headers']

        # decide if get all data or limited by date
        forced_stop = False
        stop_date=None
        if days_ago>=0:
            stop_date = self._n_days_ago(days_ago)

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

        # Loop through pages
        for page_num in range(page_num_start, page_num_stop+1):
            try:
                print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Extracting data of: {brand.title()} {model.title()}. Page number: {page_num}")

                # Pretend human
                if delay_scraping:
                    sleep(randint(1,5))

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
                            forced_stop = True
                            return df, forced_stop

                    df = self._create_df_from_row(df, row)
            except:
                print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Extracting of page {page_num} failed.")
                continue
        return df, forced_stop
    
    def transform(self, df: pd.DataFrame):
        # code to tranform data compliant schema
        '''
        * solve missing columns
        * set datatypes
        * create new rows e.g. Car_year, price_pln
        * assign id number
        '''
        print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Transforming data.")

        # REMOVE space FROM STR LOOKLIKE INT
        df['engine_capacity'] = df['engine_capacity'].str.replace(' ', '')
        df['engine_capacity'] = df['engine_capacity'].str.replace(',00', '')

        # SET DTYPES PROPERLY
        col_dtype = {
            'price': 'int',
            'cepik_verified': 'bool',
            'mileage': 'int',
            'engine_capacity': 'int',
            'engine_power': 'int',
            'year': 'int'}
        df[['price','mileage','engine_capacity','engine_power','year']] = df[['price','mileage','engine_capacity','engine_power','year']].fillna(-1)
        df = df.astype(col_dtype)
        df[['scrape_date', 'created_date']] = df[['scrape_date', 'created_date']].apply({
            'scrape_date': pd.to_datetime,
            'created_date': pd.to_datetime})
        
        # VALIDATE DATAFRAME - LEAVE ONLY REQUIRED COLUMNS
        init_expected_columns = ["scrape_date","created_date","title","short_description",
                                "price","currency","cepik_verified","make","fuel_type",
                                "gearbox","country_origin","mileage","engine_capacity",
                                "engine_power","model","year","version"]
        columns_info = self._validate_dataframe(df,init_expected_columns)
        if len(columns_info["missing_columns"]) == 0:
            for col in columns_info["missing_columns"]:
                df[col] = np.nan
        if not columns_info['correct_order']:
            df = df[init_expected_columns]

        # RENAME COLUMNS
        mapper = {
            'make': 'brand'}
        df.rename(mapper, axis=1, inplace=True)

        # CREATE ROWS
        df['car_age'] = df['scrape_date'].dt.year - df['year']

        return df

    def load(self, df, how_add='append'):
        assert how_add.upper()=='APPEND' or how_add.upper()=='TRUNCATE','Variable how_add can be only "append" or "truncate"'
        print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Loading data.")

        # Set up configuration to connect with BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=f"WRITE_{how_add}",  # Use 'WRITE_APPEND' if you want to append data
            autodetect=True
        )
        # Upload the DataFrame to BigQuery
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        job.result()
   
    def run_etl(self, **kwargs):
        '''
        ETL process self.extract -> self.transform -> self.load in loop for every 100 pages
        **kwargs:
            'brand': str, e.g. opel
            'model': str, e.g. astra
            'days_ago: int, how many days before wants to scrape
            'delay_scraping': bool, if true delay scraping every page by sleep(randint(1,5))
            'how_add': str, append/truncate, if append add to existing Table, if truncate - drop rows and add to empty table
        '''
        # Prerequisites
        kwargs['headers'] = self._get_headers()
        url = self._get_url(kwargs['brand'], kwargs['model'])
        soup = self._get_soup(url, kwargs['headers'])
        num_page = self._find_page_num(str(soup))
        iters = ceil(num_page/100) 
        
        # Save results of scraping every 100 pages
        for i in range(iters):
            kwargs['page_num_start'] = 100*i + 1
            kwargs['page_num_stop'] = 100*i + 100
            if i == iters-1:
                kwargs['page_num_stop'] = num_page

            # ETL process
            data, forced_stop = self.extarct(**kwargs)
            transformed_data = self.transform(data)
            how_add = kwargs.get('how_add', 'append')
            self.load(transformed_data,how_add=how_add)

            if forced_stop:
                break


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
            
            self.strategy.run_etl(**self.params)

            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] {self.strategy.__class__.__name__} process completed.")
        except Exception as err:
            print(f"[{datetime.now():%d-%m-%Y %H:%M:%S}] Process interrupted: {err}")


if __name__=='__main__':
    params = {
        'brand': 'opel',
        'model': 'meriva',
        'days_ago': 1,
        'delay_scraping': True,
        'how_add': 'append' # append/truncate
    }

    etl = OtoMotoETL()
    context = ContextManager(etl)
    context.set_params(**params)
    context.run()
