-- One record per person in the table persondemographic, it contains the extremes of their
-- life events of birth, immigration, emigration, and death (when known).
-- Uniquely identifier by uliabphn
SELECT
	a0.*
FROM
	ab_hzrd_rts_anlys.persondemographic a0;

-- We pivot the demographic data to two record, one for the longest possible life span and
-- one for the shortest life span.
-- Uniquely identified by uliabphn, cornercase
SELECT
	a0.*
FROM
	ab_hzrd_rts_anlys.personsurveillance a0;

-- Given each extremum record for each person we can generate the fiscal census,
-- sub-partitioned on the birthday anniversary. This forms the denominator.
-- Uniquely identifed by uliabphn, cornercase, interval start, interval end
SELECT
	a0.*
FROM
	ab_hzrd_rts_anlys.personcensus a0;

-- The numerator is all the utilization counts in each time interval when there is one
-- Uniquely identifed by uliabphn, cornercase, interval start, interval end,
-- however intervals with no utilization are not in this table
SELECT
	a0.*
FROM
	ab_hzrd_rts_anlys.personutilization a0;

-- Finally we can pivot the utilization records to one record per measure.
-- Uniquely identifed by uliabphn, cornercase, interval start, interval end,
-- measureidentifier
SELECT
	a0.*
FROM
	ab_hzrd_rts_anlys.personmeasure a0;