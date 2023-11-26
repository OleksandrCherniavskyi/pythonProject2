import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import sqlite3

#engine = create_engine('postgresql://tqupkkom:ivFQarmVJxpKnp4LYCCZ5FKvU0kGxmV5@balarama.db.elephantsql.com/tqupkkom')
#conn = engine.connect()
#conn.autocommit = True
## Create a cursor object
#connection = engine.raw_connection()
#cursor = connection.cursor()

# Create a connection to the database
# Load
engine = create_engine('sqlite:///justjoin.sqlite3', echo=True)
conn = sqlite3.connect('justjoin.sqlite3')
cursor = conn.cursor()



# QUERY

exiting_offers_query = "SELECT slug FROM offers;"
exiting_types_query = "SELECT * FROM types;"
exiting_skills_query = "SELECT * FROM skills;"
exiting_multilocation_query = "SELECT * FROM multilocation;"
ct_offers_life = """
CREATE TABLE IF NOT EXISTS offers_life (
    slug TEXT CHECK (LENGTH(slug) <= 255),
    published_at DATE
);

"""
ct_types = """
CREATE TABLE IF NOT EXISTS types (
    slug     TEXT CHECK (LENGTH(slug) <= 255),
    to_      BIGINT,
    from_    BIGINT,
    type     TEXT CHECK (LENGTH(type) <= 25),
    currency TEXT CHECK (LENGTH(currency) <= 3)
);
"""
ct_skills = """
CREATE TABLE IF NOT EXISTS skills (
    slug  TEXT CHECK (LENGTH(slug) <= 255),
    skill TEXT CHECK (LENGTH(skill) <= 50)
);
"""
ct_offers = """
CREATE TABLE IF NOT EXISTS offers (
    slug             TEXT PRIMARY KEY,
    title            TEXT CHECK (LENGTH(title) <= 120),
    workplace_type   TEXT CHECK (LENGTH(workplace_type) <= 10),
    experience_level TEXT CHECK (LENGTH(experience_level) <= 7),
    working_time     TEXT CHECK (LENGTH(working_time) <= 15),
    category_id      BIGINT,
    city             TEXT CHECK (LENGTH(city) <= 50),
    company_name     TEXT CHECK (LENGTH(company_name) <= 150),
    published_at     DATE
);
"""
ct_multilocation = """
CREATE TABLE IF NOT EXISTS multilocation (
    slug          TEXT CHECK (LENGTH(slug) <= 255),
    multicity     TEXT CHECK (LENGTH(multicity) <= 50), 
    location_slug TEXT CHECK (LENGTH(location_slug) <= 255)
);
"""
cursor.execute(ct_offers)
cursor.execute(ct_skills)
cursor.execute(ct_types)
cursor.execute(ct_multilocation)
cursor.execute(ct_offers_life)

url = 'https://justjoin.it/v2/user-panel/offers?'
r = requests.get(url)
#print(f'r_status: {r.status_code}')

webpage = bs(r.text, 'html.parser')
script_tag = webpage.find('script', id='__NEXT_DATA__')
script_content = script_tag.contents[0]
data = json.loads(script_content)
json_filename = 'extracted_data.json'

queries = data['props']['pageProps']["dehydratedState"]["queries"]
pages = queries[0]['state']['data']['pages']
data_in_json = pages[0]['data']
def unique():
    slug = []
    for item in data_in_json:
        # table offers
        o_slug = item.get("slug")
        slug.append(o_slug)
    my_dict = {
        'slug': slug}
    my_df = pd.DataFrame(my_dict, columns=['slug'])
    exiting_offers = pd.read_sql_query(exiting_offers_query, engine)
    unique_slug = my_df[~my_df['slug'].isin(exiting_offers['slug'])]
    unique_list = unique_slug['slug'].tolist()

    offers(unique_list)
    skills(unique_list)
    types(unique_list)
    multilocation(unique_list)
    offers_life()

def offers(unique_list):
    slug = []
    title = []
    workplace_type = []
    experience_level = []
    working_time = []
    category_id = []
    city = []
    company_name = []
    published_at = []
    for item in data_in_json:
        o_slug = item.get("slug")
        if o_slug not in unique_list:
            continue
        slug.append(o_slug)
        o_title = item.get("title")
        o_title = o_title.replace(' - Remote ', '')
        o_title = o_title.replace('Remote', '')
        o_title = o_title.replace(' (REMOTE)', '')
        o_title = o_title.replace(' (Remote)', '')
        o_title = o_title.replace('Junior ', '')
        o_title = o_title.replace('Junior/ ', '')
        o_title = o_title.replace('(Junior) ', '')
        o_title = o_title.replace(' (Junior)', '')
        o_title = o_title.replace('Mid ', '')
        o_title = o_title.replace('Mid-', '')
        o_title = o_title.replace('Mid /', '')
        o_title = o_title.replace('Mid / ', '')
        o_title = o_title.replace('Mid/', '')
        o_title = o_title.replace('Middle ', '')
        o_title = o_title.replace('Middle/', '')
        o_title = o_title.replace('Senior ', '')
        o_title = o_title.replace('Senior / ', '')
        o_title = o_title.replace('Senior/ ', '')
        o_title = o_title.replace('(Senior) ', '')
        o_title = o_title.replace(' (Senior)', '')
        o_title = o_title.replace('Expert ', '')
        o_title = o_title.replace('Lead ', '')
        o_title = o_title.replace('Lead, ', '')
        o_title = o_title.replace(' Lead', '')
        o_title = o_title.replace(' (Automotive)', '')
        o_title = o_title.replace(' (Azure)', '')
        o_title = o_title.replace(' (GCP)', '')
        o_title = o_title.replace(' (m/f/d)', '')
        o_title = o_title.replace('👉 ', '')
        o_title = o_title.replace('👉', '')
        o_title = o_title.replace(' (Mid / Senior)', '')
        title.append(o_title)
        o_workplace_type = item.get("workplaceType")
        workplace_type.append(o_workplace_type)
        o_experience_level = item.get("experienceLevel")
        experience_level.append(o_experience_level)
        o_working_time = item.get("workingTime")
        working_time.append(o_working_time)
        o_category_id = item.get("categoryId")
        category_id.append(o_category_id)
        o_city = item.get("city")
        city.append(o_city)
        o_company_name = item["companyName"]
        company_name.append(o_company_name)
        o_published_at = item["publishedAt"][:10]
        published_at.append(o_published_at)
    offers_dict = {
        'slug': slug,
        'title': title,
        'workplace_type': workplace_type,
        'experience_level': experience_level,
        'working_time': working_time,
        'category_id': category_id,
        'city': city,
        'company_name': company_name,
        'published_at': published_at
    }


    offers_df = pd.DataFrame(offers_dict,
        columns=['slug', 'title', 'workplace_type', 'experience_level', 'working_time',
                                      'category_id', 'city', 'company_name', 'published_at'])
    offers_df.to_sql("offers", engine, if_exists='append', index=False)

def skills(unique_list):
    slug = []
    skill = []
    for item in data_in_json:
        o_slug = item.get("slug")
        if o_slug not in unique_list:
            continue
        o_skills = item["requiredSkills"]
        for i_skill in o_skills:
            skill.append(i_skill)
            slug.append(o_slug)

    skills_dict = {
        'slug': slug,
        'skill': skill
    }
    skills_df = pd.DataFrame(skills_dict, columns=['slug', 'skill'])
    skills_df.to_sql("skills", engine, if_exists='append', index=False)


def types(unique_list):
    slug = []
    to_ = []
    from_ = []
    type = []
    currency = []
    for item in data_in_json:
        o_slug = item.get("slug")
        if o_slug not in unique_list:
            continue
        employment_types = item["employmentTypes"]
        for types in employment_types:
            o_to = types["to"]
            o_from_ = types["from"]
            o_type = types["type"]
            o_currency = types["currency"]
            slug.append(o_slug)
            to_.append(o_to)
            from_.append(o_from_)
            type.append(o_type)
            currency.append(o_currency)

    types_dict = {
        'slug': slug,
        'to_': to_,
        'from_': from_,
        'type': type,
        'currency': currency
    }
    types_df = pd.DataFrame(types_dict, columns=['slug', 'to_', 'from_', 'type', 'currency'])
    types_df.to_sql("types", engine, if_exists='append', index=False)


def multilocation(unique_list):
    slug = []
    multicity = []
    location_slug = []
    for item in data_in_json:
        o_slug = item.get("slug")
        if o_slug not in unique_list:
            continue
        multilocation = item["multilocation"]
        for location in multilocation:
            o_city = location["city"]
            o_location_slug = location["slug"]
            slug.append(o_slug)
            multicity.append(o_city)
            location_slug.append(o_location_slug)

    multilocation_dict = {
        'slug': slug,
        'multicity': multicity,
        'location_slug': location_slug
    }
    multilocation_df = pd.DataFrame(multilocation_dict, columns=['slug', 'multicity', 'location_slug'])
    multilocation_df.to_sql("multilocation", engine, if_exists='append', index=False)

def offers_life():
    slug = []
    published_at = []
    for item in data_in_json:
        o_slug = item.get("slug")
        slug.append(o_slug)
        o_published_at = item["publishedAt"][:10]
        published_at.append(o_published_at)

    offers_life_dict = {
        'slug': slug,
        'published_at': published_at
    }
    offers_life_df = pd.DataFrame(offers_life_dict, columns=['slug', 'published_at'])
    offers_life_df.to_sql("offers_life", engine, if_exists='append', index=False)


unique()