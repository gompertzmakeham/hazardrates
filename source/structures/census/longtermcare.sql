CREATE MATERIALIZED VIEW censuslongtermcare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a2.durationdays) AS INTEGER) staydays,
	CAST(SUM(a2.evententry) AS INTEGER) admissioncount,
	CAST(SUM(a2.eventexit) AS INTEGER) dischargecount,
	CAST(COUNT(*) AS INTEGER) intersectingstays
FROM
	personsurveillance a0
	INNER JOIN
	TABLE(continuing_care.accis.get_adt) a1
	ON
		a0.uliabphn = a1.uli_ab_phn
		AND
		a1.current_client_type_code = '10'
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatecensus
		(
			greatest(a0.extremumstart, a1.admit_date),
			least(a0.extremumend, COALESCE(a1.discharge_date, a0.extremumend)),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censuslongtermcare IS 'Utilization of long term care in census intervals of each person.';
COMMENT ON COLUMN censuslongtermcare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censuslongtermcare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censuslongtermcare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslongtermcare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuslongtermcare.staydays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censuslongtermcare.admissioncount IS 'Admissions in the census interval.';
COMMENT ON COLUMN censuslongtermcare.dischargecount IS 'Discharges in the census interval.';
COMMENT ON COLUMN censuslongtermcare.intersectingstays IS 'Stays intersecting with the census interval.';