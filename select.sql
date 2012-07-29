copy(
select date, max(r) from 
 (select r1.date, row_number() over (order by r1.date) as r from 
  (select a.date, c.citer, c.citee from 
   articles a, cites c where a.recid = c.citer) 
  as r1,
  articles b where r1.citee = b.recid) 
 as r2 	
 group by date)
to '/tmp/select.txt'
