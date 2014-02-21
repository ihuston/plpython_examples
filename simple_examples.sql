-- Some quick tests of the capability of PL/Python on PostgreSQL and Greenplum DB
-- Create a schema to do some work in
CREATE SCHEMA plp;

-- Simple SQL User Defined Function to get started
CREATE FUNCTION plp.times2(INT)
RETURNS INT
AS $$
SELECT 2 * $1;
$$ LANGUAGE sql;

--Try it out
SELECT plp.times2(10);

-- Test using Python code
CREATE FUNCTION plp.pymax (a integer, b integer)
RETURNS integer
AS $$
if a > b:
    return a
return b
$$ LANGUAGE plpythonu;

--Test Python code
SELECT plp.pymax(10, 5);


-- Create a composite return type
DROP TYPE IF EXISTS plp.named_value;
CREATE TYPE plp.named_value AS (
  name  text,
  value  integer
);

--Simple function which returns a composite object
CREATE OR REPLACE FUNCTION plp.make_pair (name text, value integer)
RETURNS plp.named_value
AS $$
return [ name, value ]
  # or alternatively, as tuple: return ( name, value )
  # or as dict: return { "name": name, "value": value }
  # or as an object with attributes .name and .value
$$ LANGUAGE plpythonu;

--Try out the function
SELECT plp.make_pair('Zozimus', 1);

--Using NumPy inside a PL/Python function
CREATE OR REPLACE FUNCTION plp.make_pair (name text)
RETURNS plp.named_value
AS $$
import numpy as np
a = np.arange(100)
return [name, a[2]]
$$ LANGUAGE plpythonu;

--Try it out
SELECT plp.make_pair('Horatio');

--Returning a set of results using SETOF
CREATE OR REPLACE FUNCTION plp.make_pair_sets (name text)
RETURNS SETOF plp.named_value
AS $$
import numpy as np
return ((name, i) for i in np.arange(3))
$$ LANGUAGE plpythonu;

--Try it out
SELECT plp.make_pair_sets('Gerald');


--Set up some data to show parallelisation
DROP TABLE IF EXISTS plp.test_data;

CREATE TABLE plp.test_data AS
SELECT 'a'::text AS name, generate_series(0,1000000)::float AS x, generate_series(0,1000000)/100.0 AS y
DISTRIBUTED BY (name);

INSERT INTO plp.test_data 
SELECT 'b'::text AS name, generate_series(0,1000000)::float AS x, sin(generate_series(0,1000000)/100.0) AS y;

INSERT INTO plp.test_data 
SELECT 'c'::text AS name, generate_series(0,1000000)::float AS x, 100.0 + sin(generate_series(0,1000000)/100.0) AS y;

-- Create a function to find the mean of some numbers
DROP FUNCTION IF EXISTS plp.np_mean(double precision[]);
CREATE OR REPLACE FUNCTION plp.np_mean(value_array double precision[])
RETURNS float
AS $$
import numpy as np
return np.mean(value_array)
$$ LANGUAGE plpythonu;

-- Need to pass the numbers as an array using array_agg
SELECT plp.np_mean(array_agg(y)) FROM plp.test_data;

-- Now try to do this for each type of data in parallel by grouping
SELECT name, plp.np_mean(array_agg(y)) FROM plp.test_data GROUP BY name ORDER BY name;

-- Now try do something even more interesting
DROP FUNCTION IF EXISTS plp.linregr(double precision[]);
CREATE OR REPLACE FUNCTION plp.linregr(x double precision[], y double precision[])
RETURNS float[]
AS $$
from scipy import stats
return stats.linregress(x, y)
$$ LANGUAGE plpythonu;

-- Do linear regression for all data
SELECT plp.linregr(array_agg(x), array_agg(y)) 
FROM plp.test_data;

-- Now do it separately for each 'name'
SELECT name, plp.linregr(array_agg(x), array_agg(y)) 
FROM plp.test_data 
GROUP BY name ORDER BY name;