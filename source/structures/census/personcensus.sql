CREATE MATERIALIZED VIEW personcensus NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a1.censusstart AS DATE) censusstart,
	CAST(a1.censusend AS DATE) censusend,
	CAST(a1.agestart AS DATE) agestart,
	CAST(a1.ageend AS DATE) ageend,
	CAST(a1.intervalstart AS DATE) intervalstart,
	CAST(a1.intervalend AS DATE) intervalend,
	CAST(a1.durationstart AS DATE) durationstart,
	CAST(a1.durationend AS DATE) durationend,
	CAST(a1.durationdays AS INTEGER) durationdays,
	CAST(a1.intervalage AS INTEGER) intervalage,
	CAST(a1.agecoincideinterval AS INTEGER) agecoincideinterval,
	CAST(a1.agecoincidecensus AS INTEGER) agecoincidecensus,
	CASE
		WHEN a1.durationstart <= a0.birthdate THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalbirth,
	CASE
		WHEN a0.deceaseddate <= a1.durationend THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervaldeceased,
	CASE
		WHEN a1.durationstart <= a0.immigratedate THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalimmigrate,
	CASE
		WHEN a0.emigratedate <= a1.durationend THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalemigrate,
	CAST(a1.intervalfirst AS INTEGER) intervalfirst,
	CAST(a1.intervallast AS INTEGER) intervallast,
	CAST(a1.intervalcount AS INTEGER) intervalcount,
	CAST(a1.intervalorder AS INTEGER) intervalorder
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
COMMENT ON COLUMN personcensus.intervalbirth IS 'Census interval starts on the persons birth: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervaldeceased IS 'Census interval ends on the persons death: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalimmigrate IS 'Census interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalemigrate IS 'Census interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalfirst IS 'First interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervallast IS 'Last interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalcount IS 'Number of sub-intervals in the partition of the surveillance interval.';
COMMENT ON COLUMN personcensus.intervalorder IS 'Ascending ordinal of the sub-interval in the partition of the surveillance interval.';