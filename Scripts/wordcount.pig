lines = LOAD '/user/root/data/warandpeace.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group as word, COUNT(words) as cnt;
wordsSorted = ORDER wordcount BY word DESC;
store wordsSorted into 'hdfs:/user/root/data/bookoutpig';
wordsSortedbycnt = ORDER wordcount BY cnt DESC;
topwords = LIMIT wordsSortedbycnt 10;
DUMP topwords;