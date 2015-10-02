datos = LOAD '/Users/inigo/proyectos/pigWeather/EasyWeather.txt' USING PigStorage('\t') AS (No:int, Time:chararray, Interval:chararray, Indoor_Humidity:chararray, Indoor_Temperature:chararray, Outdoor_Humidity:chararray, Outdoor_Temperature:int, Absolute_Pressure:chararray, WindGust:chararray, Direction:chararray, Relative_Pressure:chararray, Dewpoint:chararray, Windchill:chararray, Hour_Rainfall:chararray, veinticuatro_hour_Rainfall:chararray, Week_Rainfall:chararray, Month_Rainfall:chararray, Total_Rainfall:chararray, Wind_Level:chararray, Gust_Level:chararray);

fechaYTemperatura = FOREACH datos GENERATE Outdoor_Temperature, SUBSTRING(Time,0,10);

agrupados = GROUP fechaYTemperatura BY $1;

maximos = FOREACH agrupados GENERATE group, MAX(fechaYTemperatura.Outdoor_Temperature);
minimos = FOREACH agrupados GENERATE group, MIN(fechaYTemperatura.Outdoor_Temperature);
media = FOREACH agrupados GENERATE group, AVG(fechaYTemperatura.Outdoor_Temperature);

STORE maximos INTO '/Users/inigo/proyectos/pigWeather/maximos' USING PigStorage('\t');
STORE minimos INTO '/Users/inigo/proyectos/pigWeather/minimos' USING PigStorage('\t');
STORE media INTO '/Users/inigo/proyectos/pigWeather/media' USING PigStorage('\t');

 --Load files into relations
    month1 = LOAD 'hdfs:/user/root/data/input/201412hourly.txt' USING PigStorage(',')
    	AS (wban: int,date: chararray,time: chararray,stationtype: int,
            skycondition: chararray,skyconditionflg: int,visibility: float,visibilityflg: int,weather_type: chararray,weathertypeflg: int,dryTemp: int);;
    month2 = LOAD 'hdfs:/user/root/data/input/201501hourly.txt' USING PigStorage(',')
    	AS (wban: int,date: chararray,time: chararray,stationtype: int,
            skycondition: chararray,skyconditionflg: int,visibility: float,visibilityflg: int,weather_type: chararray,weathertypeflg: int,dryTemp: int);;
 
    --Combine relations
    months = UNION month1, month2;

    /* Splitting relations
    SPLIT months INTO 
            splitMonth1 IF SUBSTRING(date, 4, 6) == '01',
            splitMonth2 IF SUBSTRING(date, 4, 6) == '02',
            splitMonth3 IF SUBSTRING(date, 4, 6) == '03',
            splitRest IF (SUBSTRING(date, 4, 6) == '04' OR SUBSTRING(date, 4, 6) == '04');
    */

    /*  Joining relations

    stations = LOAD 'hdfs:/data/big/data/QCLCD201211/stations.txt' USING PigStorage() AS (id:int, name:chararray)

    JOIN months BY wban, stations by id;

    */

    --filter out unwanted data
    clearWeather = FILTER months BY skyCondition == 'CLR';

    --Transform and shape relation
    shapedWeather = FOREACH clearWeather GENERATE date, SUBSTRING(date, 0, 4) as year, SUBSTRING(date, 4, 6) as month, SUBSTRING(date, 6, 8) as day, skyCondition, dryTemp;

    --Group relation specifying number of reducers
    groupedByMonthDay = GROUP shapedWeather BY (month, day) PARALLEL 10;
    
    --Aggregate relation
        aggedResults = FOREACH groupedByMonthDay GENERATE group as MonthDay, AVG(shapedWeather.dryTemp), MIN(shapedWeather.dryTemp), MAX(shapedWeather.dryTemp), COUNT(shapedWeather.dryTemp) PARALLEL 10;
    
        --Sort relation
        sortedResults = ORDER aggedResults BY $1 DESC;
    
        --Store results in HDFS
    STORE sortedResults INTO 'hdfs:/user/root/data/weatherpigresults' USING PigStorage(':');
    