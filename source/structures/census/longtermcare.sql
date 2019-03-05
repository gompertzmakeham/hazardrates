CREATE MATERIALIZED VIEW censuslongtermcare NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all long term care events
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
			END staydays,
			CASE
				WHEN a2.durationstart BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervalfirst
				ELSE
					0
			END admissioncount,
			CASE
				WHEN a2.durationend BETWEEN a0.extremumstart AND a0.extremumend THEN
					a2.intervallast
				ELSE
					0
			END dischargecount,
			CASE
				WHEN greatest(a0.extremumstart, a2.durationstart) <= least(a0.extremumend, a2.durationend) THEN
					1
				ELSE
					0
			END intersectingstays
		FROM
			personsurveillance a0
			INNER JOIN
			TABLE(continuing_care.accis.get_adt) a1
			ON
				a0.uliabphn = a1.uli_ab_phn
				AND
				a1.current_client_type_code = '10'
				AND
				COALESCE(a1.discharge_date, TRUNC(SYSDATE, 'MM')) BETWEEN a1.admit_date AND TRUNC(SYSDATE, 'MM')
				AND
				COALESCE(a1.birth_date, a1.admit_date) <= COALESCE(a1.discharge_date, TRUNC(SYSDATE, 'MM'))
			CROSS JOIN
			TABLE
			(
				hazardutilities.generatecensus
				(
					a1.admit_date,
					COALESCE(a1.discharge_date, TRUNC(SYSDATE, 'MM')),
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
	CAST(SUM(a0.staydays) AS INTEGER) staydays,
	CAST(SUM(a0.admissioncount) AS INTEGER) admissioncount,
	CAST(SUM(a0.dischargecount) AS INTEGER) dischargecount,
	CAST(SUM(a0.intersectingstays) AS INTEGER) intersectingstays
FROM
	eventdata a0
WHERE
	a0.staydays > 0
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a0.intervalstart,
	a0.intervalend;

COMMENT ON MATERIALIZED VIEW censuslongtermcare IS 'Utilization of long term care in census intervals of each person.';
COMMENT ON COLUMN censuslongtermcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censuslongtermcare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censuslongtermcare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslongtermcare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslongtermcare.staydays IS 'Naive sum of long term care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censuslongtermcare.admissioncount IS 'Long term care admissions in the census interval.';
COMMENT ON COLUMN censuslongtermcare.dischargecount IS 'Long term care discharges in the census interval.';
COMMENT ON COLUMN censuslongtermcare.intersectingstays IS 'Long term care stays intersecting with the census interval.';