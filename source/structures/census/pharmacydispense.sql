CREATE MATERIALIZED VIEW censuspharmacydispense NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a2.durationdays) durationdays,
	SUM(CASE WHEN hazardutilities.cleandate(a1.admitdate) BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) durationadmisions,
	SUM(CASE WHEN hazardutilities.cleandate(a1.disdate) BETWEEN a2.intervalstart AND a2.intervalend THEN 1 ELSE 0 END) durationdischarges,
	MIN(a0.birthequipoise) birthequipoise,
	MIN(a0.deceasedequipoise) deceasedequipoise,
	MAX(a2.ageinterval) ageinterval,
	MAX(a2.agecensus) agecensus
FROM
	personsurveillance a0
	INNER JOIN
	ahsdata.pin_dspn a1
	ON
		a0.uliabphn = hazardutilities.cleanphn(a1.phn)
		AND
		a1.admitcat = 'U'
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatecensus
		(
			greatest(a0.extremumstart, hazardutilities.cleandate(a1.admitdate)),
			least(a0.extremumend, hazardutilities.cleandate(a1.disdate)),
			a0.birthdate
		)
	) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;