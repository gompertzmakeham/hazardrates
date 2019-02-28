CREATE OR REPLACE VIEW personsurveillance AS
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a1.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a1.birthdate AS DATE) birthdate,
	CAST(a1.deceaseddate AS DATE) deceaseddate,
	CAST(a1.immigratedate AS DATE) immigratedate,
	CAST(a1.emigratedate AS DATE) emigratedate,
	CAST(a1.extremumstart AS DATE) extremumstart,
	CAST(a1.extremumend AS DATE) extremumend
FROM
	persondemographic a0
	CROSS JOIN
	TABLE
	(
		hazardutilities.generatesurveillance
		(
			a0.leastbirth,
			a0.greatestbirth,
			a0.leastdeceased,
			a0.greatestdeceased,
			a0.leastimmigrate,
			a0.greatestimmigrate,
			a0.leastemigrate,
			a0.greatestemigrate,
			a0.surveillancestart,
			a0.surveillanceend
		)
	) a1
WITH READ ONLY;

COMMENT ON TABLE personsurveillance IS 'For every person that at any time was covered by Alberta Healthcare Insurance list two surveillance intervals for each person, representing the corner cases of possible values for the birth and death dates. In the case of observational equipose the two records are identical.';
COMMENT ON COLUMN personsurveillance.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personsurveillance.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personsurveillance.birthdate IS 'Best estimate of the birth date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case.';
COMMENT ON COLUMN personsurveillance.deceaseddate IS 'Best estimate of the deceased date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.immigratedate IS 'Best estimate of the immigration date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.emigratedate IS 'Best estimate of the emigration date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.extremumstart IS 'Start date of the observation bounds of the person rectified by the immigration and birth dates.';
COMMENT ON COLUMN personsurveillance.extremumend IS 'End date of the observation bounds of the person rectified by the emigration and deceased dates.';