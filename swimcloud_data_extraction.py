# This script scraps www.swimcloud.com to extract swimmer event times from all meets listed.
#
# Copyright 2023, Tao Long

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import json
import concurrent.futures
from sqlalchemy import create_engine, text

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)

conn_url = 'mysql+pymysql://tao:hcWvXfcH055Z@dragon.duoinsight.com/swim'
conn = create_engine(conn_url)


def generate_swimcloud_root_urls():
    df = pd.read_sql('select swimmer_id from swimmers where active=1', conn)
    swimmer_ids = df['swimmer_id'].tolist()
    root_urls = [f'https://www.swimcloud.com/swimmer/{x}/meets/' for x in swimmer_ids]

    return root_urls

def parse_swimcloud_meet_data(url):
    # Extract swimmer ID
    segments = url.rstrip('/').split('/')
    swimmer_id = int(segments[-1])
    meet_id = int(segments[4])

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Search for the JavaScript object that contains the startDate, endDate and location value
    script_tag = soup.find("script", string=re.compile("startDate"))
    if script_tag:
        script_content = script_tag.string
        json_dict = json.loads(script_content)
        start_date_str = json_dict['startDate']
        end_date_str = json_dict['endDate']

        def date_str_to_datetime(date_str):
            # first, clean up date string to keep only first 3 letters of month
            parts = date_str.split()
            parts[0] = parts[0][:3]
            new_date_str = ' '.join(parts)

            # convert to datetime
            date = datetime.strptime(new_date_str, '%b %d, %Y')
            return date

        start_date = date_str_to_datetime(start_date_str)
        end_date = date_str_to_datetime(end_date_str)

        location = json_dict['location']['name']
        meet_name = json_dict['name']

    # Extract swimmer name
    name_element = soup.find('h3', class_='c-title')
    swimmer_name = name_element.a.text.strip()

    # Extract team name
    try:
        team_name = soup.find("a", {"href": lambda x: x and x.startswith('/results/') and '/team/' in x}).text.strip()
    except:
        team_name = 'Unattached'

    meet_params = dict(
        start_date=start_date,
        end_date=end_date,
        location=location,
        meet_name=meet_name,
        meet_id=meet_id,
    )

    # Find the table after the "Times" header
    times_table = soup.find("h3", string="Times").find_next("table")

    # Extract table headers
    headers = [header.text.strip() for header in times_table.find_all("th")]

    # Extract rows from table body
    rows = []
    for row in times_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        row_data = [cell.text.strip() for cell in cells]
        rows.append(row_data)

    # Create DataFrame
    df_times = pd.DataFrame(rows[1:], columns=headers).replace('–', pd.NA)
    df_times[['event_name', 'event_round']] = df_times['Event'].str.split('\n\s+\n', expand=True)
    df_times['event_number'] = df_times['№'].str[1:].astype(int, errors='ignore')
    df_times['meet_name'] = meet_name
    df_times['meet_id'] = meet_id
    df_times['swimmer_name'] = swimmer_name
    df_times['swimmer_id'] = swimmer_id
    df_times['team_name'] = team_name

    df_times.columns = df_times.columns.str.lower()
    df_times.rename(columns={'time': 'event_time', 'fina': 'points', 'pts': 'points'}, inplace=True)

    df_times = df_times[
        ['meet_name', 'meet_id', 'swimmer_name', 'swimmer_id', 'team_name', 'event_number', 'event_name', 'event_round',
         'heat', 'lane', 'event_time', 'points']]
    return df_times, meet_params


def scrape_meet_urls_from_page_url(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, "html.parser")

    meet_urls = []

    for link in soup.find_all('a'):
        url = link.get('href')
        if ('results' in url) and ('swimmer' in url):
            url = 'https://www.swimcloud.com' + url
            meet_urls.append(url)

    return meet_urls


def get_meet_urls(root_url):
    """
    This function gets all meet URLs from swimcloud.com.
    :param root_url: Root URL for a swimmer's meet page. E.g., "https://www.swimcloud.com/swimmer/1822492/meets/"
    :return: List of meet URLs.
    """
    global conn

    response = requests.get(root_url)
    soup = BeautifulSoup(response.text, "html.parser")

    pages = []
    for link in soup.find_all('a'):
        url = link.get('href')
        if '?page=' in url:
            pages.append(url)

    # remove duplicate elements
    pages = list(set(pages))
    pages = ['?page=1'] + pages

    meet_urls = []

    # Create a ThreadPoolExecutor to run the scraping function concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Construct the complete page URLs
        page_urls = [root_url + page for page in pages]

        # Use the map method to execute the scrape_meet_urls function concurrently
        meet_urls = list(executor.map(scrape_meet_urls_from_page_url, page_urls))

    # Flatten the list of lists into a single list
    meet_urls = [url for sublist in meet_urls for url in sublist]

    # remove duplicate elements
    meet_urls = list(set(meet_urls))

    # remove existing (meet, swimmer) URLs
    known_meet_swimmer = pd.read_sql('select distinct meet_id, swimmer_id from times', conn)
    known_meet_swimmer = list(zip(known_meet_swimmer['meet_id'], known_meet_swimmer['swimmer_id']))

    meet_urls = [x for x in meet_urls if (int(x.split('/')[4]), int(x.split('/')[6])) not in known_meet_swimmer]

    return meet_urls


def clean_up_times(df_times):
    df = df_times.reset_index(drop=True).copy()

    # drop all na
    df.dropna(subset=['event_time'], inplace=True)

    # for DQ and NS, annotate notes field
    non_numeric_time = ['DQ', 'NS']
    cond1 = df['event_time'].isin(non_numeric_time)
    cond2 = True if 'notes' not in df else df['notes'].isna()
    df.loc[cond1 & cond2, 'notes'] = df.loc[cond1 & cond2, 'event_time']
    df.loc[cond1 & ~cond2, 'notes'] = df.loc[cond1 & ~cond2].apply(lambda x: f'{x["notes"]}; {x["event_time"]}', axis=1)

    # after annotating notes field, set DQ and NS to null
    df['event_time'] = df['event_time'].replace(non_numeric_time, pd.NA)

    # Define a function to format the time strings
    def format_time(time_str):
        if time_str is not pd.NA:
            # Split the time string by ':' to determine the number of components
            components = time_str.split(':')

            # If there's only seconds, prepend "0:0:"
            if len(components) == 1:
                return '0:0:' + time_str
            # if missing hours, prepend "0:"
            elif len(components) == 2:
                return '0:' + time_str
            else:
                return time_str  # Time string already has hours and minutes
        else:
            return pd.NA  # Return None for null values

    df['event_time'] = df['event_time'].apply(format_time)

    return df


def update_db(meets, times):
    global conn

    known_meets = pd.read_sql('select * from meets', conn)
    known_swimmers = pd.read_sql('select * from swimmers', conn)
    known_times = pd.read_sql('select * from times', conn)
    known_events = pd.read_sql('select * from events', conn)
    known_teams = pd.read_sql('select * from teams', conn)

    # prune meets, and add new ones to meets table
    df = meets[~meets['meet_id'].isin(known_meets['meet_id'].tolist())].copy()
    df['meet_id'] = df['meet_id'].astype(int)
    if not df.empty:
        df.to_sql(con=conn, name='meets', if_exists='append', index=False)
        print('Found new meets')
        print(df)
    else:
        print('No new meets found')

    # prune swimmers, and add new ones to swimmers table
    df = times[['swimmer_name', 'swimmer_id']].drop_duplicates()
    df['swimmer_id'] = df['swimmer_id'].astype(int)
    df = df[~df['swimmer_id'].isin(known_swimmers['swimmer_id'].tolist())]
    if not df.empty:
        df['first_name'] = df['swimmer_name'].apply(lambda x: x.split(' ')[0])
        df['last_name'] = df['swimmer_name'].apply(lambda x: x.split(' ')[-1])
        df.rename(columns={'swimmer_name': 'full_name'}, inplace=True)
        df.to_sql(con=conn, name='swimmers', if_exists='append', index=False)
        print('Found new swimmers')
        print(df)
    else:
        print('No new swimmers found')

    # prune teams, and add new ones to teams table
    df = times[['team_name']].drop_duplicates()
    df = df[~df['team_name'].isin(known_teams['team_name'])]
    if not df.empty:
        df.to_sql(con=conn, name='teams', if_exists='append', index=False)
        known_teams = pd.read_sql('select * from teams', conn)
        print('Found new teams')
        print(df)
    else:
        print('No new teams found')

    # prune times, and add new records to times table
    df = times.merge(known_events[['event_name', 'event_id']], on='event_name', how='left')
    df = df.merge(known_teams[['team_name', 'team_code']])

    # in case team_code is unknown, reflect team_name in notes
    cond1 = df['team_code'].isna()
    cond2 = df['notes'].isna()
    df.loc[cond1 & cond2, 'notes'] = df['team_name']
    df.loc[cond1 & ~cond2, 'notes'] = df.apply(lambda x: f'{x["team_name"]}; {x["notes"]}', axis=1)

    df = df.merge(known_times[['meet_id', 'swimmer_id', 'event_id']], how='outer',
                  on=['meet_id', 'swimmer_id', 'event_id'], indicator=True)
    df = df[df['_merge'] == 'left_only']

    # relay events do not have event_id. drop them.
    df.dropna(subset=['event_id'], inplace=True)

    # upload data to times table
    if not df.empty:
        times_cols = pd.read_sql('show columns from times', conn)['Field'].tolist()
        df = df[list(set(times_cols).intersection(df.columns))]

        df.to_sql(con=conn, name='times', if_exists='append', index=False)
        print(f'Found {len(df)} new time records')
    else:
        print('No new times found')

    conn.dispose()


def main():

    root_urls = generate_swimcloud_root_urls()

    meet_urls = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Process each root URL concurrently using threads
        results = executor.map(get_meet_urls, root_urls)
        for result in results:
            meet_urls.extend(result)

    meets = []
    times = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(parse_swimcloud_meet_data, url): url for url in meet_urls}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            print(url)
            try:
                df_times, meet_params = future.result()
                times.append(df_times)
                meets.append(meet_params)
            except Exception as e:
                print(f"An error occurred while processing {url}: {e}")

    # remove duplicates and convert to dataframe
    df_meets = pd.DataFrame(meets).drop_duplicates()
    df_meets['meet_id'] = df_meets['meet_id'].astype(int)

    # concat times dataframe
    df_times = pd.concat(times)
    df_times = clean_up_times(df_times)

    # update database
    update_db(df_meets, df_times)

    print('Done')


if __name__ == "__main__":
    main()
