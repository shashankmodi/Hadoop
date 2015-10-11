A = LOAD '/user/root/data/stocks/stocks.csv' using PigStorage(',')
  	AS (exchange1:chararray, symbol:chararray,
            date:chararray, open:float, high:float, low:float, close:float,
            volume:int, adj_close:float);
B = ORDER A BY exchange1,symbol;
C1 = FILTER B BY exchange1 == 'NASDAQ' and symbol == 'AAPL';
C2 = FILTER B BY exchange1 == 'NASDAQ' and symbol == 'INTC';
C3 = FILTER B BY exchange1 == 'NYSE' and symbol == 'GE';
C4 = FILTER B BY exchange1 == 'NYSE' and symbol == 'IBM';
STORE C1 into '/user/root/data/input/AAPL' using PigStorage(',');
STORE C2 into '/user/root/data/input/INTC' using PigStorage(',');
STORE C3 into '/user/root/data/input/GE' using PigStorage(',');
STORE C4 into '/user/root/data/input/IBM' using PigStorage(',');
