-- Load input from the file named Mary, and call the single 
-- field in the record 'line'.
input1 = load '/user/root/data/warandpeace.txt' as (line);

-- TOKENIZE splits the line into a field for each word.
-- flatten will take the collection of records returned by
-- TOKENIZE and produce a separate record for each one, calling the single
-- field in the record word.
words = foreach input1 generate flatten(TOKENIZE(UPPER(line))) as word;

-- Now group them together by each word.
grpd  = group words by word;

-- Count them.
cntd  = foreach grpd generate group, COUNT(words);
-- Print out the results.
dump cntd;

%default TODAYS_DATE `date +%Y-%m-%d`;
%default V30_DAYS_AGO `date -d "$TODAYS_DATE - 30 day" +%Y/%m/%d`;
--unintended_walks.pig
--fs -put /media/sf_Data/Hadoop/DataSets/baseball.txt /user/root/data/
fs -ls /user/root/data/
fs -du -h /user/root/data/
player = load '/user/root/data/baseball.txt'
	as (name:chararray, team:chararray,
	    pos:bag{t:(p:chararray)}, bat:map[]);
describe player;
player10 = LIMIT player 10;
dump player10;
inintended =  foreach player generate bat#'base_on_balls' - bat#'ibbs';
--inintended =  foreach player generate 
--	(bat#'base_on_balls' - bat#'ibbs' is null ? 0 : (bat#'base_on_balls' - bat#'ibbs'));
playercnt =  foreach (group player all) 
	generate COUNT($1),$TODAYS_DATE,$V30_DAYS_AGO,
		ToString(SubtractDuration(CurrentTime(),'P30D'), 'yyyy-MM-dd');
playercnt1 =  foreach (group player all) 
	generate COUNT($1),
		ToString(CurrentTime(),'yyyy-MM-dd'),CurrentTime();
inintended10 = LIMIT inintended 10;
dump playercnt;
dump playercnt1;
dump inintended10;
inintendedf = FILTER inintended by $0 is not null;
--inintendedf = FILTER inintended by ($0 matches '(Jason|Scott).*') AND $1 >0;
inintendedf10 = LIMIT inintendedf 10;
dump inintendedf10;


--register.pig
register '/media/sf_Data/piggybank.jar';
dividends = load '/user/root/data/dividends/dividends.csv'  using PigStorage(',')
		as (exchange1:chararray, symbol:chararray,
            date:chararray, dividend:float);
backwards = foreach dividends generate symbol,
                org.apache.pig.piggybank.evaluation.string.Reverse(symbol);
backwards10 = LIMIT backwards 10;
dump backwards10;

define reverse org.apache.pig.piggybank.evaluation.string.Reverse();
backwards = foreach dividends generate symbol,
                reverse(symbol);

--set pig.enable.plan.serialization=false on /etc/pig/conf/pig.properties
-- To run ILLUSTRATE set the above parameters

--invoker.pig
define hex InvokeForString('java.lang.Integer.toHexString', 'int');
daily = LOAD '/user/root/data/stocks/stocks.csv' using PigStorage(',')
  	AS (exchange1:chararray, symbol:chararray,
            date:chararray, open:float, high:float, low:float, close:float,
            volume:int, adj_close:float);
nonnull = filter daily by volume is not null;
inhex = foreach nonnull generate symbol,volume, hex((int)volume);
inhex10 = LIMIT inhex 10;
dump inhex10;

--flatten.pig
player = load '/user/root/data/baseball.txt' 
	  as (name:chararray, team:chararray,
		pos:bag{t:(p:chararray)},bat:map[]);
/*player = load '/user/root/data/baseball.txt'
	as (name:chararray, team:chararray,
	    pos:bag{t:(p:chararray)}, bat:map[]);  */  
player1 = LIMIT player 1;
dump player1;

pos     = foreach player generate name, flatten(pos) as position;
bypos   = group pos by position;
bypos1 = LIMIT bypos 1;
dump bypos1;

