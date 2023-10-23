#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def extract_data():
    print("Extracting Data...")
    data_source = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    data = pd.read_json(data_source)
    print("Data Extracted Successfully!")
    return data

def transform_data(raw_data):
    print("Transforming Data...")
    transformed_data = raw_data.copy()
    
    transformed_data.fillna('Not Recorded', inplace=True)
    
    transformed_data.columns = [col.lower() for col in transformed_data.columns]

    animal_columns = ['animal_id', 'name', 'date_of_birth', 'animal_type', 'breed_id']
    outcome_event_columns = ['outcome_event_id', 'datetime', 'animal_id', 'outcome_type']
    fact_table_columns = ['outcome_event_id', 'animal_id', 'breed_id']

    transformed_data['outcome_event_id'] = transformed_data.index + 1

    outcome_events = transformed_data[outcome_event_columns]
    
    unique_breed_types = transformed_data[['breed']].drop_duplicates().reset_index(drop=True)
    unique_breed_types['breed_id'] = unique_breed_types.index + 1
    breed_types = unique_breed_types[['breed_id', 'breed']]

    breed_type_id_map = dict(zip(unique_breed_types['breed'], unique_breed_types['breed_id']))
    transformed_data['breed_id'] = transformed_data['breed'].map(breed_type_id_map)

    animal_data = transformed_data[animal_columns].drop_duplicates().reset_index(drop=True)

    outcome_events.reset_index(drop=True, inplace=True)

    fact_table = transformed_data[fact_table_columns]
    print('Data Transformed Successfully!')
    return fact_table, animal_data, outcome_events, breed_types

def load_data(transformed_data):
    print('Loading Data...')
    fact_table, animal_data, outcome_events, breed_types = transformed_data

    DATABASE_URL = "postgresql+psycopg2://scooby:pass@db:5432/shelter"
    engine = create_engine(DATABASE_URL)


    animal_data.to_sql('animal_data', engine, if_exists='append', index=False)
    breed_types.to_sql('breed_types', engine, if_exists='append', index=False)
    outcome_events.to_sql('outcome_events', engine, if_exists='append', index=False)
    fact_table.to_sql('fact_table', engine, if_exists='append', index=False)

    print('Data Loaded Successfully!')

if __name__ == '__main__':
    raw_data = extract_data()

    transformed_data = transform_data(raw_data)

    load_data(transformed_data)
