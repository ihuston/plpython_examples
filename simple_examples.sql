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
SELCT plp.pymax(10, 5);


-- Create a composite return type
CREATE TYPE plp.named_value AS (
  name  text,
  value  integer
);

--Simple function which returns a composite object
CREATE OR REPLACE FUNCTION plp.make_pair (name text, value integer)
RETURNS named_value
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
RETURNS named_value
AS $$
import numpy as np
a = np.arange(100)
return [name, a[2]]
$$ LANGUAGE plpythonu;

--Try it out
SELECT make_pair('Horatio');

--Returning a set of results using SETOF
CREATE OR REPLACE FUNCTION make_pair_sets (name text)
RETURNS SETOF named_value
AS $$
import numpy as np
return ((name, i) for i in np.arange(3))
$$ LANGUAGE plpythonu;

--Try it out
SELECT make_pair_sets('Gerald');


