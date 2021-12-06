-- CS 457/657 PA4

-- This script includes the commands to be executed by two processes, P1 and P2

-- On P1:
CREATE DATABASE CS457_PA4;
USE CS457_PA4;
create table Flights (seat int, status int);
insert into Flights values (22,0); -- seat 22 is available
insert into Flights values (23,1); -- seat 23 is occupied
begin transaction;
update flights set status = 1 where seat = 22;

