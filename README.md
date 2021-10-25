# Citi-Bike-Rider-Engagement-Analysis-SQL-

# Created in Snowflake using AWS


  
     
    
   --Create Citibike Trips Table
   
    create or replace table trips
    (tripduration integer,
     starttime timestamp,
     stoptime timestamp,
     start_station_id integer,
     start_station_name string,
     start_station_latitude float,
     start_station_longitude float,
     end_station_id integer,
     ends_tation_name string,
     end_station_latitude float,
     end_station_longitude float,
     bikeid integer,
     membership_type string,
     usertype string,
     birth_year integer,
     gender integer);
     
     -- 3.2 - Create staging 
     
     create or replace stage citibike_trips url ='s3://snowflake-workshop-lab/citibike-trips';
     
     list @citibike_trips;
     
     create or replace file format csv type='csv'
        compression ='auto' field_delimiter = ','record_delimiter = '\n'
        skip_header=0 field_optionally_enclosed_by = '\042' trim_space = false
        error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
        date_format ='auto' timestamp_format = 'auto' null_if =('');
    
    
     copy into trips from @citibike_trips file_format = csv;
    
     
     
     truncate table trips;

    
 
--3.2 resize WH to Large using UI

    copy into trips from @citibike_trips file_format=csv;
    


-- Create  & Use WH for Analytics

    
    
    use warehouse analytics_wh;
    
    select* from trips limit 20;
    
    select date_trunc('hour',starttime) as "dates",
    count(*) as "num trips",
    avg(tripduration)/60 as "avg duration (mins)",
    avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
    from trips
    group by 1 order by 1;
    
    select monthname(starttime) as "month",
        count(*) as "num trips"
    from trips
    group by 1 order by 2 desc;
    
    
    --Create Clone for weather analysis
    
    create table trips_dev clone trips;
    
    create or replace database weather;
    
    use warehouse load_wh;
    use schema weather.public;
    
    create table json_weather_data (v variant);
    
    
    --Create Stage
    
    create stage nyc_weather url = 's3://snowflake-workshop-lab/weather-nyc';
    
    list @nyc_weather;
    
    copy into json_weather_data
    from @nyc_weather
    file_format = (type=json);
    
    Select * from json_weather_data limit 10;
    
    
    --Create a view of Json data as table
    
    create or replace view json_weather_data_view as
    select
        v:time::timestamp as observation_time,
        v:city.id::int as city_id,
        V:city.name::string as city_name,
        v:city.country as country, 
        v:city.coord.lat::float as city_lat,
        v:city.coord.lon::float as city_lon,
        v:clouds.all::int as clouds,
        (v:main.temp::float) -273.15 as temp_avg,
        (v:main.temp_min::float) -273.15 as temp_min,
        (v:main.temp_max::float) -273.15 as temp_max,
        v:weather[0].main::string as weather,
        v:weather[0].description::string as weather_desc,
        v:weather[0].icon::string as weather_icon,
        v:wind.deg::float as wind_dir,
        v:wind.speed::float as wind_speed
      from json_weather_data
      where city_id = 5128638;
      
    select * from json_weather_data_view
    where date_trunc('month',observation_time) = '2018-01-01'
    limit 20;
      
     select weather as conditions
    ,count(*) as num_trips
    from citibike.public.trips
    left outer join json_weather_data_view
        on date_trunc('hour', observation_time) = date_trunc('hour',starttime)
        where conditions is not null
        group by 1 order by 2 desc;
        
  
  
  
        
    
    
    

    
    
    

    
    
    
    





     
    

