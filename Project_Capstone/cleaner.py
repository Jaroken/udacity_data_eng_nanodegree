import numpy as np
import pandas as pd
import pyspark
##### Immigration data #####

# Reduce variables to the variables of interest
def clean_immig_data(df_immig):
    """
    cleans the immigration spark dataset and returns a reduced and relabeled dataset.
    parameters:
    df_immg: spark dataset of the immigration dataset
    """
    immig_fact = df_immig[['cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 
                                       'i94bir','i94visa', 'i94mode', 'airline']]
    # Rename columns 
    immig_fact = immig_fact.toDF(*['cicid','year', 'month', 'port', 'addr', 'arrival_date',
                                      'resp_age', 'visa','mode', 'airline']) 
    return(immig_fact)

# Do not need drop duplicate values and I will not be controlling for missing values in any way

##### Airport data #####
def clean_airport_data(df_airport):
    """
    cleans the airport dataset and returns a cleaned pandas dataset.
    parameters:
    df_airport: the pandas dataframe of the airport datasert
    """
    # remove non-US airports
    airport_dim = df_airport[df_airport.iso_country == 'US']
    # remove irrelevant airports
    airport_dim = airport_dim[[i in ['small_airport', 'medium_airport', 'large_airport'] for i in airport_dim.type]] 
    # Remove the null cases for local_code
    airport_dim = airport_dim[airport_dim.local_code.isna()==False] 
    # split coordinates to longitude and latitude
    airport_dim['longitude']=[float(i.split(',')[0]) for i in airport_dim['coordinates']]
    airport_dim['latitude']=[float(i.split(',')[1]) for i in airport_dim['coordinates']]
    # extract state_code from iso_region
    airport_dim['state_code'] = [i.split('-')[1] for i in airport_dim['iso_region']]

    # include only the relevant columns I want to include
    airport_dim = airport_dim[['type', 'name', 'elevation_ft', 'state_code',
                             'iso_country', 'municipality', 'local_code', 'longitude', 'latitude']] 
    # rename local_code to port to match fact table
    airport_dim.rename(columns={'local_code':'port',
                               'municipality':'city'}, inplace=True)
    return(airport_dim)


##### Tempurature data #####
def clean_temp_data(df_temp):
    """
    cleaned the temperature dataset and returns a reduced and cleaned dataset.
    parameters:
    df_temp: pandas dataframe of the temperature data
    """
    # Filter out non-US data
    temp_dim = df_temp[df_temp.Country == 'United States'] 
    # Extract year 
    temp_dim['year'] = [int(i[:4]) for i in temp_dim.dt] 
    # Extract month
    temp_dim['month'] = [int(i[5:7]) for i in temp_dim.dt]
    # Rename relevant columns
    temp_dim.rename(columns = {'AverageTemperature':'avg_temp',
                             'AverageTemperatureUncertainty':'avg_temp_uncertainty',
                             'City':'city'}, inplace = True) 
    # Aggregate data by city and month and year
    temp_dim = temp_dim[temp_dim.year >=2010].groupby(['city','month']).mean().reset_index()[['city','month','avg_temp','avg_temp_uncertainty']]
    return(temp_dim)

##### Demographic Data #####
def clean_demo_data(df_demo):
    """
    cleaning the demo data and exporting a clean pandas dataset.
    parameters:
    df_demo: pandas dataframe of the demographic data
    """
    demo_dim = df_demo.copy()
    # Change all columns to lower case
    demo_dim.columns = [i.lower() for i in demo_dim.columns]
    # set integer columns to integer
    demo_dim['male population']=  [int(i) for i in np.nan_to_num(demo_dim['male population'])]
    demo_dim['female population']=  [int(i) for i in np.nan_to_num(demo_dim['female population'])]
    demo_dim['total population']= [int(i) for i in np.nan_to_num(demo_dim['total population'])]
    demo_dim['number of veterans']= [int(i) for i in np.nan_to_num(demo_dim['number of veterans'])]
    demo_dim['foreign-born']=  [int(i) for i in np.nan_to_num(demo_dim['foreign-born'])]
    # replace whitespace and '-' with '_'
    demo_dim.columns = [i.replace(' ', '_').replace('-','_') for i in demo_dim.columns]
    # drop irrelevant columns
    demo_dim.drop(columns = 'count', inplace=True)
    return(demo_dim)

##### City ID Table #####
def clean_city_data(airport_dim, demo_dim, temp_dim):
    """
    ingests the airport, demo, and temp cleaned datasets and extracts the city information and records the unique citys across the three datasets
    parameters:
    airport_dim: cleaned pandas dataframe of airport data 
    demo_dim: cleaned pandas dataframe of demo data 
    temp_dim cleaned pandas dataframe of temp data 
    """
    # make a city table
    a_list = list(airport_dim.city.unique())
    d_list = list(demo_dim.city.unique())
    t_list = list(temp_dim.city.unique())
    city_list = (set.union(set(t_list), set(d_list), set(a_list)))
    city_dim =pd.DataFrame({'city_name':list(city_list)})
    return(city_dim)

#### Port ID Table #####
def clean_port_data(immig_fact, airport_dim):
    """
    ingests the immigration and airport cleaned datasets and extracts the port information and records the unique ports across the two datasets
    parameters:
    immig_fact: cleaned pyspark dataframe of immigration data
    airport_dim: cleaned pandas dataframe of airport data 
    """
    imm_dist_port =immig_fact.select('port').distinct().collect()
    airport_port = airport_dim[['port']]
    portlist = list(set.union(set([i.port for i in imm_dist_port]), set(airport_port)))
    port_dim = pd.DataFrame({'port_name':portlist})
    return(port_dim)