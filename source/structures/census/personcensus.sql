ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW personcensus NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a1, 1) */
	a0.uliabphn,
	a0.cornercase,
	a1.*
FROM
	personsurveillance a0
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a0.extremumstart, a0.extremumend, a0.birthdate)) a1;

COMMENT ON MATERIALIZED VIEW personcensus IS 'For every person that at any time was covered by Alberta Healthcare Insurance partition the surviellance interval by the intersections of fiscal years and age years, rectified by the start and end of the surveillance interval.';
COMMENT ON COLUMN personcensus.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personcensus.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personcensus.censusstart IS 'Closed start of the fiscal year, April 1.';
COMMENT ON COLUMN personcensus.censusend IS 'Closed end of the fiscal year, March 31.';
COMMENT ON COLUMN personcensus.agestart IS 'Closed start of the age interval.';
COMMENT ON COLUMN personcensus.ageend IS 'Closed end of the age interval, the next birthday minus one day.';
COMMENT ON COLUMN personcensus.intervalstart IS 'Closed start of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personcensus.intervalend IS 'Closed end of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personcensus.durationstart IS 'Closed start of the interval, rectified to the start of the surveillance interval.';
COMMENT ON COLUMN personcensus.durationend IS 'Closed end of the interval, rectified to the end of the surveillance interval.';
COMMENT ON COLUMN personcensus.durationdays IS 'Duration of the interval in days, an integer starting at 1, using the convention that the interval is closed so that the duration is end minus start plus one day.';
COMMENT ON COLUMN personcensus.intervalage IS 'Age in years at the start of the interval, an integer starting at 0.';
COMMENT ON COLUMN personcensus.agecoincideinterval IS 'Interval starts on the birthday: 1 yes, 0 no';
COMMENT ON COLUMN personcensus.agecoincidecensus IS 'Birthday is the start of the fical year (April 1): 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.evententry IS 'First interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.eventexit IS 'Last interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalcount IS 'Number of sub-intervals in the partition of the surveillance interval.';
COMMENT ON COLUMN personcensus.intervalorder IS 'Ascending ordinal of the sub-interval in the partition of the surveillance interval.';