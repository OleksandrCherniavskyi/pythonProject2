import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine


engine = create_engine('postgresql://tqupkkom:ivFQarmVJxpKnp4LYCCZ5FKvU0kGxmV5@balarama.db.elephantsql.com/tqupkkom')
# Create a connection to the database
conn = engine.connect()
conn.autocommit = True
# Create a cursor object
connection = engine.raw_connection()
cursor = connection.cursor()

# QUERY

exiting_offers_query = "SELECT * FROM offers;"
exiting_types_query = "SELECT * FROM types;"
exiting_skills_query = "SELECT * FROM skills;"
exiting_multilocation_query = "SELECT * FROM multilocation;"

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

for item in data_in_json:
     #table offers
    def offers():
        slug = []
        title = []
        workplace_type = []
        experience_level = []
        working_time = []
        category_id = []
        city = []
        company_name = []
        published_at = []
        o_slug = item.get("slug")
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

        o_city = str(item["city"])
        city.append(item["city"])

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
        return offers_df


    try:
        exiting_offers = pd.read_sql_query(exiting_offers_query, engine)
        new_offers = offers()[~offers()['slug'].isin(exiting_offers['slug'])]
        new_offers.to_sql("offers", engine, if_exists='append', index=False)  # engine,
    except:
        print("Data already exists in the table offers")


    def skills():
        slug = []
        skill = []
        o_slug = item.get("slug")
        o_skills = item["requiredSkills"]
        for i_skill in o_skills:
            skill.append(i_skill)
            slug.append(o_slug)

        skills_dict = {
            'slug': slug,
            'skill': skill
        }
        skills_df = pd.DataFrame(skills_dict, columns=['slug', 'skill'])

        return skills_df
    skills()

    try:
        exiting_skills = pd.read_sql_query(exiting_skills_query, engine)
        new_skills = skills()[~skills()['slug'].isin(exiting_skills['slug'])]
        new_skills.to_sql("skills", engine, if_exists='append', index=False)  # engine,
    except:
        print("Data already exists in the table skills")

    # table employmentTypes
    def types():
        slug = []
        to_ = []
        from_ = []
        type = []
        currency = []
        o_slug = item.get("slug")
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
        return types_df
    types().to_sql("types", engine, if_exists='append', index=False)
    try:
        exiting_types = pd.read_sql_query(exiting_types_query, engine)
        new_types = types()[~types()['slug'].isin(exiting_types['slug'])]
        new_types.to_sql("types", engine, if_exists='append', index=False)
    except:
        print("Data already exists in the table types")

    def multilocation():
        slug = []
        multicity = []
        location_slug = []
        o_slug = item.get("slug")
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
        return multilocation_df
    try:
        exiting_multilocation = pd.read_sql_query(exiting_multilocation_query, engine)
        new_locations = multilocation()[~multilocation()['slug'].isin(exiting_multilocation['slug'])]
        new_locations.to_sql("multilocation", engine, if_exists='append', index=False)
    except:
        print("Data already exists in the table multilocation")

    def offers_life():
        slug = []
        published_at = []
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
        return offers_life_df
    offers_life()
