create procedure insert_data_rfm (in report_date date)
	begin
insert into data_mart.customer_rfm select * from (
with CTE1 as ( 
select CustomerID, 
       datediff('2022-09-01', max(Purchase_Date)) as Recency, 
       count(distinct(Purchase_Date))/(datediff('2022-09-01', created_date)/365) as Frequency, 
       sum(GMV)/(datediff('2022-09-01', created_date)/365) as Monetary, 
       row_number () over (order by datediff('2022-09-01', max(Purchase_Date))) as rn_recency, 
       row_number () over (order by count(distinct(Purchase_Date))/(datediff('2022-09-01', created_date)/365)) as rn_frequency, 
       row_number () over (order by sum(GMV)/(datediff('2022-09-01', created_date)/365)) as rn_monetary 
from customer_analytics.customer_registered cr  
join customer_analytics.customer_transaction ct on cr.ID=ct.CustomerID
where CustomerID <> 0 and stopdate is null
group by CustomerID), 
CTE2 as ( 
select CustomerID, Recency, Frequency, Monetary, 
 	   case 
  		when rn_recency >= (select min(rn_recency) from CTE1) and rn_recency < (select count(rn_recency)*0.25 from CTE1) then 4 
  		when rn_recency >= (select count(rn_recency)*0.25 from CTE1) and rn_recency < (select count(rn_recency)*0.5 from CTE1) then 3 
  		when rn_recency >= (select count(rn_recency)*0.5 from CTE1) and rn_recency < (select count(rn_recency)*0.75 from CTE1) then 2 
  		else 1 
 	   end as R, 
 	  case 
  		when rn_frequency >= (select min(rn_frequency) from CTE1) and rn_frequency < (select count(rn_frequency)*0.25 from CTE1) then 1 
  		when rn_frequency >= (select count(rn_frequency)*0.25 from CTE1) and rn_frequency < (select count(rn_frequency)*0.5 from CTE1) then 2 
  		when rn_frequency >= (select count(rn_frequency)*0.5 from CTE1) and rn_frequency < (select count(rn_frequency)*0.75 from CTE1) then 3 
  		else 4 
 	  end as F, 
 	  case 
  		when rn_monetary >= (select min(rn_monetary) from CTE1) and rn_monetary < (select count(rn_monetary)*0.25 from CTE1) then 1 
  		when rn_monetary >= (select count(rn_monetary)*0.25 from CTE1) and rn_monetary < (select count(rn_monetary)*0.5 from CTE1) then 2 
  		when rn_monetary >= (select count(rn_monetary)*0.5 from CTE1) and rn_monetary < (select count(rn_monetary)*0.75 from CTE1) then 3 
  		else 4 
 	  end as M 
from CTE1 
group by CustomerID) 
select CustomerID,
	   Recency,
	   Frequency,
	   Monetary,
	   R,
	   F,
	   M,
	   concat(R, F, M) as RFM,
       case 
     	when concat(R, F, M) in (444, 443, 344, 343) then 'VIP' 
     	when concat(R, F, M) in (442, 441, 424, 423, 342, 341) then 'Loyal Customers' 
     	when concat(R, F, M) in (422, 421, 414, 413, 412, 411, 324, 323, 322, 321, 314, 313) then 'Potential Loyalist' 
     	when concat(R, F, M) in (312, 311) then 'Recent Customers' 
     	when concat(R, F, M) in (244, 243, 242, 241, 224, 223, 222, 221, 214, 213, 212, 211) then 'At Risk' 
     	else 'Lost'
       end as customer_type,
       report_date as reportdate
from CTE2) A;
	end

create procedure auto_insert_data_rfm ()
	begin
set @date = current_date;
call insert_data_rfm (@date);
	end

set global event_scheduler = on

create event insertdata
on schedule every 1 day
do call auto_insert_data_rfm ()