CREATE MATERIALIZED VIEW censussupportiveliving NOLOGGING NOCOMPRESS PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
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
	TABLE(continuing_care.home_care.get_residency) a1
	ON
		a0.uliabphn = a1.uli_ab_phn
		AND
		a1.delivery_setting_affiliation IN ('SUPPORTIVE LIVING LEVEL 3', 'SUPPORTIVE LIVING LEVEL 4', 'SUPPORTIVE LIVING LEVEL 4 DEMENTIA')
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatecensus
		(
			greatest(a0.extremumstart, a1.entry_from_date),
			least(a0.extremumend, COALESCE(a1.exit_to_date, a0.extremumend)),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censussupportiveliving IS 'Utilization of designated supportive living levels 3, 4, and 4 dementia in census intervals of each person.';
COMMENT ON COLUMN censussupportiveliving.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censussupportiveliving.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censussupportiveliving.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censussupportiveliving.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censussupportiveliving.staydays IS 'Naive sum of stay days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN censussupportiveliving.admissioncount IS 'Admissions in the census interval.';
COMMENT ON COLUMN censussupportiveliving.dischargecount IS 'Discharges in the census interval.';
COMMENT ON COLUMN censussupportiveliving.intersectingstays IS 'Stays intersecting with the census interval.';