import requests
from bs4 import BeautifulSoup
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import streamlit as st
import plotly.express as px

# Configure pandas display settings
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)

st.set_page_config(page_title="Dragon Handy Swim Time Tool", layout="wide")
st.subheader('Dragon Handy Swim Time Tool')
st.write('© 2023 Tao Long. All rights reserved.')

with open('Data/urls.txt', 'r') as file:
    urls = file.read().split("\n")
    
    # Remove the trailing comments
    urls = [url.split("#")[0].strip() for url in urls if url.strip() != ""]

# JO and FW time standards
jofw_df = pd.read_csv('Data/time_standard.csv')

# First, let's define the function to apply the abbreviations

def abbreviate_event(event):
    event = event.replace(" Yd ", " Y ")
    event = event.replace(" M ", " L ")
    event = event.replace("Backstroke", "Back")
    event = event.replace("Freestyle", "Free")
    event = event.replace("Butterfly", "Fly")
    event = event.replace("Breaststroke", "Breast")
    event = event.replace("Individual Medley", "IM")
    return event

def format_time(seconds):
    if seconds is None:
        x = ''
    else:
        minutes, sec = divmod(seconds, 60)
        if int(minutes) == 0:
            x = "{:05.2f}".format(sec)
        else:
            x = "{:1}:{:05.2f}".format(int(minutes), sec)
    return x

# Function to convert time string to seconds
def time_to_seconds(time_str):
    if time_str[0] == '-':
        sign = -1
        time_str = time_str[1:]
    else:
        sign = 1

    if ':' in time_str:
        minutes, seconds = map(float, time_str.split(':'))
        return sign * (minutes * 60 + seconds)
    else:
        return sign * float(time_str)

@st.cache_data
def swim_times():
    # Create a list to store the dataframes
    dataframes = []

    # Iterate over each URL
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Get the swimmer's name and club from the page title
        title = soup.find('title').text
        swimmer, club, _ = title.split('|')

        # Find all tables in the page
        tables = soup.find_all("table")

        # Drop the first table
        tables = tables[1:]

        # Iterate over each table
        for i, table in enumerate(tables):
            # Find all rows in the table
            rows = table.find_all("tr")

            # Skip tables that do not have at least three rows
            if len(rows) < 3:
                continue

            # Get the meet name from the first header row
            meet = ' '.join([cell.text for cell in rows[0].find_all("th")]).strip()

            # Split the second header row into date and age
            second_header = ' '.join([cell.text for cell in rows[1].find_all("th")])
            if "Age" in second_header:
                date, age = second_header.split("Age", 1)
                age = "Age" + age
            else:
                date = second_header
                age = ""

            # Use the third table header row as the column header
            column_headers = [cell.text for cell in rows[2].find_all("th")]

            # Initialize an empty list to store the data
            data = []

            # Iterate over each subsequent row
            for row in rows[3:]:
                # Find all columns in the row
                cells = row.find_all("td")

                # Get the text from each cell
                cell_contents = [cell.text for cell in cells]

                # Append the meet, date, and age to the beginning of the row data
                cell_contents = [meet, date, age] + cell_contents

                # Append the row data to the data list
                data.append(cell_contents)

            # Create a dataframe from the data
            df = pd.DataFrame(data, columns=['Meet', 'Date', 'Age'] + column_headers)

            # Rename the last column to "Improvement"
            df.rename(columns={df.columns[-1]: 'Improvement'}, inplace=True)

            # Add the "Personal Best" column
            df['Personal Best'] = df['Improvement'].str.contains('Personal Best|PB').astype(int)

            # Trim leading and trailing whitespace from the "Improvement" field values
            df['Improvement'] = df['Improvement'].str.strip()

            df['Improvement'] = df['Improvement'].apply(lambda x: time_to_seconds(x.split('(')[0].replace(' seconds','').strip()) if x.split('(')[0].replace(' seconds','').strip() else None)

            # Add the "Swimmer" and "Club" columns
            df['Swimmer'] = swimmer
            df['Club'] = club

            # Convert the "Time" field to seconds
            df['Time'] = df['Time'].apply(time_to_seconds)

            # Update the "Age" field by removing "Age " string and convert the remaining string to integer
            df['Age'] = df['Age'].str.replace('Age ', '').astype(int)

            # Add the dataframe to the list
            dataframes.append(df)

    # Concatenate all dataframes into a single dataframe
    final_df = pd.concat(dataframes, ignore_index=True)
    final_df = final_df.sort_values(['Event', 'Time'])

    # Create a dataframe for the personal best times
    personal_best_df = final_df.loc[final_df.groupby(['Swimmer', 'Event'])['Time'].idxmin()][['Swimmer', 'Event', 'Time']]

    # re-sort final_df by event Date
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    final_df = final_df.sort_values(['Date', 'Event', 'Time'])

    return final_df, personal_best_df

def main():
    final_df, personal_best_df = swim_times()
    final_df['Event'] = final_df['Event'].apply(abbreviate_event)
    personal_best_df['Event'] = personal_best_df['Event'].apply(abbreviate_event)
    personal_best_df['Swimmer'] = personal_best_df['Swimmer'].apply(lambda x: x.split(' ')[0])

    # Apply the formatting function to the 'Time' column of final_df and personal_best_df
    final_df_str = final_df.copy()
    personal_best_df_str = personal_best_df.copy()
    
    final_df_str['Time'] = final_df_str['Time'].apply(format_time)
    final_df_str['Date'] = final_df_str['Date'].dt.strftime('%Y-%m-%d')
    personal_best_df_str['Time'] = personal_best_df['Time'].apply(format_time)
    
    # Prepare the pivot table
    personal_best_pivot = personal_best_df_str.pivot(index='Event', columns='Swimmer', values='Time')
    personal_best_pivot.index.name = "Event"
    personal_best_pivot = personal_best_pivot.reset_index()

    # Split 'Event' into three temporary fields for sorting
    personal_best_pivot[['Temp1', 'Temp2', 'Temp3']] = personal_best_pivot['Event'].str.extract(r'(\d+)\s(\w)\s(.+)', expand=True)
    personal_best_pivot['Temp1'] = personal_best_pivot['Temp1'].astype(int)

    # Sort by the temporary fields and then remove them
    personal_best_pivot = personal_best_pivot.sort_values(['Temp1', 'Temp2', 'Temp3'])
    personal_best_pivot = personal_best_pivot.drop(columns=['Temp1', 'Temp2', 'Temp3'])

    # Merge with JO and FW time standards
    personal_best_pivot = personal_best_pivot.merge(jofw_df, on='Event', how='left')

    personal_best_pivot.fillna('', inplace=True)

    # Display a button to force clear cache
    if st.button("Refresh data"):
        # Clear values from *all* all in-memory and on-disk data caches:
        # i.e. clear values from both square and cube
        st.cache_data.clear()

    # Display the pivot table
    st.write("Personal Best Times", unsafe_allow_html=True)
    grid_height = 35*len(personal_best_pivot)+38
    gb = GridOptionsBuilder.from_dataframe(personal_best_pivot)
    gb.configure_default_column(groupable=True, value=True, 
                                enableRowGroup=True, aggFunc='sum', 
                                editable=True, filter=True)
    gb.configure_column("Event", value=True, enableRowGroup=False, aggFunc='sum', editable=False)
    gb.configure_grid_options(domLayout='autoHeight')

    gridOptions = gb.build()
    
    # Calculate the width for the 'Event' column based on the maximum string length
    event_col_width = max(personal_best_pivot['Event'].str.len()) * 10  # Approximate width per character

    for col in gridOptions['columnDefs']:
        if col['field'] == 'Event':
            col['maxWidth'] = event_col_width
            col['minWidth'] = int(event_col_width * 0.8)
        else:
            col['maxWidth'] = 80  # Set width to 100px for all other columns
            col['minWidth'] = 80

        if col['field'] != 'Event':
            col['cellClassRules'] = {
                'highlight': JsCode("function(params) { return params.value.toString() == Math.min(...Object.values(params.data).filter(val => !isNaN(parseFloat(val)) && isFinite(val)).map(val => val.toString())).toString(); }")
            }
            col['cellStyle'] = {"textAlign": "right"}  # Align numeric columns to the right

    AgGrid(personal_best_pivot, 
           gridOptions=gridOptions, 
           height=grid_height,
           width='100%',
           fit_columns_on_grid_load=True,
           allow_unsafe_jscode=True)


    # Add a dropdown for event selection
    event_list = final_df['Event'].unique().tolist()
    event_list  = sorted(event_list, key=lambda x: (int(x.split()[0]), x.split()[1], x.split()[2]))
    selected_event = st.selectbox('Select an event:', event_list)

    # Filter the final_df dataframe based on the selected event
    filtered_df = final_df[final_df['Event'] == selected_event]

    # Convert the 'Time' column to numeric for plotting
    filtered_df['Time'] = pd.to_numeric(filtered_df['Time'], errors='coerce')

    # Create a line graph using Plotly
    fig = px.line(filtered_df, x='Date', y='Time', color='Swimmer',
                  title=f'Time progression for {selected_event}',
                  log_y=True, labels={'Time': 'Time (log scale)'})
    fig.update_traces(mode='markers+lines')
    
    # Display the line graph
    # Create a range of y tick values
    y_range = filtered_df['Time'].max() - filtered_df['Time'].min()
    ytickstep = int(y_range/10) + 1
    ytickvals = list(range(0, int(filtered_df['Time'].max())+2, ytickstep))
    
    # Create the corresponding y tick labels
    yticktext = [format_time(val) for val in ytickvals]
    
    # Update y-axis ticks
    fig.update_yaxes(tickvals=ytickvals, ticktext=yticktext)
    
    # Display the line graph
    st.plotly_chart(fig)    


    # Display the final_df_str dataframe
    with st.expander("All Times", expanded=False):
        final_df_str = final_df_str[['Event', 'Swimmer', 'Time', 'Date', 'Meet']]
        gb = GridOptionsBuilder.from_dataframe(final_df_str)
        gb.configure_default_column(groupable=True, value=True, filter=True)
        gb.configure_grid_options(domLayout='autoHeight')
        gb.configure_column('Time', cellStyle={"textAlign": "right"})
        gridOptions = gb.build()
        
        # Set the maximum width for the columns
        for col in gridOptions['columnDefs']:
            if col['field'] == 'Event':
                col['maxWidth'] = event_col_width  # Previously calculated width based on max string length
                col['minWidth'] = int(event_col_width * 0.8)
            elif col['field'] == 'Swimmer':
                col['maxWidth'] = 120
                col['minWidth'] = 100
            elif col['field'] == 'Time':
                col['maxWidth'] = 80
                col['minWidth'] = 80
            elif col['field'] == 'Date':
                col['maxWidth'] = 120
                col['minWidth'] = 100
            elif col['field'] == 'Meet':
                col['maxWidth'] = 300  # Set maxWidth to 150px
                col['minWidth'] = 200
        
            if col['field'] == 'Time':
                col['cellStyle'] = {"textAlign": "right"}  # Align numeric columns to the right

        AgGrid(final_df_str,
           gridOptions=gridOptions,
           height=600,
           width='100%',
           fit_columns_on_grid_load=True)


if __name__ == "__main__":
    main()
