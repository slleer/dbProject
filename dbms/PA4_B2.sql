-- CS 457/657 PA4

-- This script includes the commands to be executed by two processes, P1 and P2



-- On P2:
select * from Flights;

---------------------
-- Expected output --
---------------------

-- On P1:
-- Database CS457_PA4 created.
-- Using database CS457_PA4.
-- Table Flights created.
-- 1 new record inserted.
-- 1 new record inserted.
-- Transaction starts.
-- 1 record modified.
-- Transaction committed.
-- seat int|status int
-- 22|1
-- 23|1


-- On P2:
-- Using database CS457_PA4.
-- seat int|status int
-- 22|0
-- 23|1
-- Transaction starts.
-- Error: Table Flights is locked!
-- Transaction abort.
-- seat int|status int
-- 22|0
-- 23|1
-- seat int|status int
-- 22|1
-- 23|1