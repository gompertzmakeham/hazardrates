CREATE MATERIALIZED VIEW persondemographic NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest all sources of person events
	ingestevents AS
	(
		SELECT a0.* FROM surveyambulatorycare a0 UNION ALL
		SELECT a0.* FROM surveyannualregistry a0 UNION ALL
		SELECT a0.* FROM surveycontinuingcare a0 UNION ALL
		SELECT a0.* FROM surveyinpatientcare a0 UNION ALL
		SELECT a0.* FROM surveylaboratorycollection a0 UNION ALL
		SELECT a0.* FROM surveypharmacydispense a0 UNION ALL
		SELECT a0.* FROM surveyprimarycare a0 UNION ALL
		SELECT a0.* FROM surveyvitalstatistics a0
	),

	-- Digest to one record per person
	digestevents AS
	(
		SELECT
			a0.uliabphn,
			COALESCE(MIN(a0.sex), 'O') sex,
			COALESCE(MAX(a0.firstnations), 0) firstnations,
			MIN(a0.leastbirth) leastbirth,
			MAX(a0.greatestbirth) greatestbirth,
			MIN(a0.leastdeceased) leastdeceased,
			MAX(a0.greatestdeceased) greatestdeceased,
			MIN(a0.leastservice) leastservice,
			MAX(a0.greatestservice) greatestservice,
			MIN(a0.leastsurveillancestart) leastsurveillancestart,
			MIN(a0.leastsurveillanceend) leastsurveillanceend,
			MAX(a0.greatestsurveillancestart) greatestsurveillancestart,
			MAX(a0.greatestsurveillanceend) greatestsurveillanceend,
			COALESCE(MAX(a0.surveillancebirth), 0) surveillancebirth,
			COALESCE(MAX(a0.surveillancedeceased), 0) surveillancedeceased,
			COALESCE(MAX(a0.surveillanceimmigrate), 0) surveillanceimmigrate,
			COALESCE(MAX(a0.surveillanceemigrate), 0) surveillanceemigrate,
			COALESCE(MAX(a0.albertacoverage), 0) albertacoverage,
			MAX(a0.censoreddate) censoreddate
		FROM
			ingestevents a0
		GROUP BY
			a0.uliabphn
	)

-- Rectify life events
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.sex AS VARCHAR2(1)) sex,
	CAST(a0.firstnations AS INTEGER) firstnations,
	CAST(a1.leastbirth AS DATE) leastbirth,
	CAST(a1.greatestbirth AS DATE) greatestbirth,
	CAST(a1.leastdeceased AS DATE) leastdeceased,
	CAST(a1.greatestdeceased AS DATE) greatestdeceased,
	CAST(a1.leastimmigrate AS DATE) leastimmigrate,
	CAST(a1.greatestimmigrate AS DATE) greatestimmigrate,
	CAST(a1.leastemigrate AS DATE) leastemigrate,
	CAST(a1.greatestemigrate AS DATE) greatestemigrate,
	CAST(a1.surveillancestart AS DATE) surveillancestart,
	CAST(a1.surveillanceend AS DATE) surveillanceend,
	CAST(a1.birthdateequipoise AS INTEGER) birthdateequipoise,
	CAST(a1.deceaseddateequipoise AS INTEGER) deceaseddateequipoise,
	CAST(a1.birthobservationequipoise AS INTEGER) birthobservationequipoise,
	CAST(a1.deceasedobservationequipoise AS INTEGER) deceasedobservationequipoise,
	CAST(a1.immigratedateequipoise AS INTEGER) immigratedateequipoise,
	CAST(a1.emigratedateequipoise AS INTEGER) emigratedateequipoise,
	CAST(a1.immigrateobservationequipoise AS INTEGER) immigrateobservationequipoise,
	CAST(a1.emigrateobservationequipoise AS INTEGER) emigrateobservationequipoise,
	CAST(a1.surveillancestartequipoise AS INTEGER) surveillancestartequipoise,
	CAST(a1.surveillanceendequipoise AS INTEGER) surveillanceendequipoise,
	CAST(a1.ageequipoise AS INTEGER) ageequipoise,
	CAST(a0.censoreddate AS DATE) censoreddate
FROM
	digestevents a0
	INNER JOIN
	TABLE
	(
		hazardutilities.generatedemographic
		(
			a0.leastbirth,
			a0.greatestbirth,
			a0.leastdeceased,
			a0.greatestdeceased,
			a0.leastservice,
			a0.greatestservice,
			a0.leastsurveillancestart,
			a0.leastsurveillanceend,
			a0.greatestsurveillancestart,
			a0.greatestsurveillanceend,
			a0.surveillancebirth,
			a0.surveillancedeceased,
			a0.surveillanceimmigrate,
			a0.surveillanceemigrate
		)
	) a1
	ON
		a0.albertacoverage = 1
		AND
		greatest
		(
			a1.surveillancestart,
			a1.leastbirth,
			COALESCE(a1.leastimmigrate, a1.surveillancestart)
		) <= 
		least
		(
			a1.surveillanceend,
			COALESCE(a1.greatestdeceased, a1.surveillanceend),
			COALESCE(a1.greatestemigrate, a1.surveillanceend)
		)
		AND
		greatest
		(
			a1.surveillancestart,
			a1.greatestbirth,
			COALESCE(a1.greatestimmigrate, a1.surveillancestart)
		) <= 
		least
		(
			a1.surveillanceend,
			COALESCE(a1.leastdeceased, a1.surveillanceend),
			COALESCE(a1.leastemigrate, a1.surveillanceend)
		);

COMMENT ON MATERIALIZED VIEW persondemographic IS 'For every person that at any time was covered by Alberta Healthcare Insurance Plan report the extremum dates on events of birth, death, immigation, emigration, surveillance start, and end.';
COMMENT ON COLUMN persondemographic.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN persondemographic.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN persondemographic.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.leastbirth IS 'Lower bound on the date of birth.';
COMMENT ON COLUMN persondemographic.greatestbirth IS 'Upper bound on the date of birth.';
COMMENT ON COLUMN persondemographic.leastdeceased IS 'Lower bound on the deceased date, null when unknown.';
COMMENT ON COLUMN persondemographic.greatestdeceased IS 'Upper bound on the deceased date, null when unknown.';
COMMENT ON COLUMN persondemographic.leastimmigrate IS 'Lower bound on the immigration date, null when unknown.';
COMMENT ON COLUMN persondemographic.greatestimmigrate IS 'Upper bound on the immigration date, null when unknown.';
COMMENT ON COLUMN persondemographic.leastemigrate IS 'Lower bound on the emigration date, null when unknown.';
COMMENT ON COLUMN persondemographic.greatestemigrate IS 'Upper bound on the emigration date, null when unknown.';
COMMENT ON COLUMN persondemographic.birthdateequipoise IS 'Lower and upper estimates of birth date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.deceaseddateequipoise IS 'Lower and upper estimates of deceased date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.birthobservationequipoise IS 'Extremums agree on occurrence of birth event: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.deceasedobservationequipoise IS 'Extremums agree on occurrence of deceased event: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.immigratedateequipoise IS 'Lower and upper estimates of immigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.emigratedateequipoise IS 'Lower and upper estimates of emigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.immigrateobservationequipoise IS 'Extremums agree on occurrence of immigration event: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.emigrateobservationequipoise IS 'Extremums agree on occurrence of emigration event: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.surveillancestartequipoise IS 'Extremums start dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.surveillanceendequipoise IS 'Extremums end dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.ageequipoise IS 'Extremums have the same age in the fiscal year: 1 yes, 0 no.';
COMMENT ON COLUMN persondemographic.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN persondemographic.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN persondemographic.censoreddate IS 'First day of the month of the last refresh of the data.';