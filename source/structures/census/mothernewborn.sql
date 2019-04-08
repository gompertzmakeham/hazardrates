CREATE MATERIALIZED VIEW censusmothernewborn NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS

-- Count live newborns delivered by each mother
SELECT

	/*+ cardinality(a2, 1) */
	CAST(a1.uliabphn AS INTEGER) uliabphn,
	CAST(a1.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(COUNT(*) AS INTEGER) livenewborns
FROM
	personsurveillance a0
	INNER JOIN
	personsurveillance a1
	ON
		a0.cornercase = a1.cornercase
		AND
		a0.maternalphn = a1.uliabphn
		AND
		a0.birthdate BETWEEN a1.surveillancestart AND a1.surveillanceend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a0.birthdate, a1.birthdate)) a2
GROUP BY
	a1.uliabphn,
	a1.cornercase,
	a2.intervalstart,
	a2.intervalend;

COMMENT ON MATERIALIZED VIEW censusmothernewborn IS 'Delivery of live newborns in the census intervals of each mother.';
COMMENT ON COLUMN censusmothernewborn.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusmothernewborn.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusmothernewborn.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusmothernewborn.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusmothernewborn.livenewborns IS 'Naive count of live newborns delivered by the mother in the census interval, minimal plausibility checks.';