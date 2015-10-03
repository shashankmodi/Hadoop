/*
#-------------------------------------------------------------------------------------- 
# Pig Script 
# NVL2UDFDemo.pig 
# pig /media/sf_Data/Hadoop/Scripts/NVL2UDFDemo.pig 1>1 2&1 &
#-------------------------------------------------------------------------------------- 
*/
register /media/sf_Data/eclipse/quintiles.jar; 
define NVL2 com.quintiles.hadoop.pig.NVL2; 
define qLower com.quintiles.hadoop.pig.qLower;

rawDept = load '/user/root/data/dept.txt' using PigStorage('\t') 
as (deptNo:chararray, deptName:chararray); 
dump rawDept;
transformedDept1 = foreach rawDept generate $0, qLower($1); 
dump transformedDept1;
transformedDept = foreach rawDept generate $0, NVL2($1,$1,'No Dept'); 
dump transformedDept;