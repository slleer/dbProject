-- CS 457/657 PA4

-- This script includes the commands to be executed by two processes, P1 and P2


-- On P2:
USE CS457_PA4;
select * from Flights;
begin transaction;
update flights set status = 1 where seat = 22;
commit; --there should be nothing to commit; it's an "abort"
select * from Flights;

