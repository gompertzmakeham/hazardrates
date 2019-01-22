CREATE MATERIALIZED VIEW censussupportiveliving NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a2.durationdays) durationdays,
	SUM(CASE WHEN a1.entry_from_date BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) durationadmisions,
	SUM(CASE WHEN a1.exit_to_date BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) durationdischarges,
	MIN(a0.birthequipoise) birthequipoise,
	MIN(a0.deceasedequipoise) deceasedequipoise,
	MAX(a2.ageinterval) ageinterval,
	MAX(a2.agecensus) agecensus
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