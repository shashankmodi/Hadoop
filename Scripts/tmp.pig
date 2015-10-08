A = LOAD '/user/root/data/weatherperf.csv' using PigStorage(',');
B = FOREACH A GENERATE $1 as station, (int) SUBSTRING($2,0,4) as year , (int) $4 as tmax, (int) $5 as tmin;
C1 = FILTER B by tmax >-9999 and tmin >-9999; /* C1 = FILTER B by tmax >0;*/
D1 = ORDER C1 BY year;
STORE D1 into '/user/root/data/input/Weather_pig1' using PigStorage(',');
