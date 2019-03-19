CREATE MATERIALIZED VIEW censuscaremanagement NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all care management events
	eventdata AS
	(
		SELECT
		
			/*+ cardinality(a2, 1) */
			a0.uliabphn,
			a0.cornercase,
			a2.intervalstart,
			a2.intervalend,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1 + least(a0.extremumend, a2.durationend) - greatest(a0.extremumstart, a2.durationstart)
				ELSE
					0
			END manageddays,
			CASE
				WHEN a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END allocationcount,
			CASE
				WHEN a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END releasecount,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END intersectingmanagement
		FROM
			personsurveillance a0
			INNER JOIN
			TABLE(continuing_care.home_care.get_coordination) a1
			ON
				a0.uliabphn = a1.uli_ab_phn
				AND
				a1.coordination_inferred = 'N'
				AND
				COALESCE(a1.release_to_date, TRUNC(SYSDATE, 'MM')) BETWEEN a1.allocate_from_date AND TRUNC(SYSDATE, 'MM')
				AND
				COALESCE(a1.birth_date, a1.allocate_from_date) <= COALESCE(a1.release_to_date, TRUNC(SYSDATE, 'MM'))
			CROSS JOIN
			TABLE
			(
				hazardutilities.generatecensus
				(
					a1.allocate_from_date,
					COALESCE(a1.release_to_date, TRUNC(SYSDATE, 'MM')),
					a0.birthdate
				)
			) a2
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a0.intervalstart AS DATE) intervalstart,
	CAST(a0.intervalend AS DATE) intervalend,
	CAST(SUM(a0.manageddays) AS INTEGER) manageddays,
	CAST(SUM(a0.allocationcount) AS INTEGER) allocationcount,
	CAST(SUM(a0.releasecount) AS INTEGER) releasecount,
	CAST(SUM(a0.intersectingmanagement) AS INTEGER) intersectingmanagement
FROM
	eventdata a0
WHERE
	a0.manageddays > 0
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a0.intervalstart,
	a0.intervalend;

COMMENT ON MATERIALIZED VIEW censuscaremanagement IS 'Utilization of any care, case, transition, or placement managment or coordination, as measured by allocations of professionals to conduct those activities.';
COMMENT ON COLUMN censuscaremanagement.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censuscaremanagement.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censuscaremanagement.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuscaremanagement.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuscaremanagement.manageddays IS 'Naive sum of days of professionals allocated to provide care, case, transition, or placement managment or coordination, that intersected with the census interval, including overlapping allocations.';
COMMENT ON COLUMN censuscaremanagement.allocationcount IS 'Allocations of professionals to provide care, case, transition, or placement managment or coordination.';
COMMENT ON COLUMN censuscaremanagement.releasecount IS 'Release of professionals from providing care, case, transition, or placement managment or coordination.';
COMMENT ON COLUMN censuscaremanagement.intersectingmanagement IS 'Allocations of professionals providing care, case, transition, or placement managment or coordination that intersected with the census interval.';