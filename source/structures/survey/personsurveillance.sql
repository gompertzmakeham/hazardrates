CREATE OR REPLACE VIEW personsurveillance AS
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.sex AS VARCHAR2(1)) sex,
	CAST(a0.firstnations AS INTEGER) firstnations,
	CAST(a1.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a1.birthdate AS DATE) birthdate,
	CAST(a1.deceaseddate AS DATE) deceaseddate,
	CAST(a1.immigratedate AS DATE) immigratedate,
	CAST(a1.emigratedate AS DATE) emigratedate,
	CAST(a0.surveillancestart AS DATE) surveillancestart,
	CAST(a0.surveillanceend AS DATE) surveillanceend,
	CAST(a1.extremumstart AS DATE) extremumstart,
	CAST(a1.extremumend AS DATE) extremumend,
	CAST(a0.birthdateequipoise AS INTEGER) birthdateequipoise,
	CAST(a0.deceaseddateequipoise AS INTEGER) deceaseddateequipoise,
	CAST(a0.birthobservationequipoise AS INTEGER) birthobservationequipoise,
	CAST(a0.deceasedobservationequipoise AS INTEGER) deceasedobservationequipoise,
	CAST(a0.immigratedateequipoise AS INTEGER) immigratedateequipoise,
	CAST(a0.emigratedateequipoise AS INTEGER) emigratedateequipoise,
	CAST(a0.immigrateobservationequipoise AS INTEGER) immigrateobservationequipoise,
	CAST(a0.emigrateobservationequipoise AS INTEGER) emigrateobservationequipoise,
	CAST(a0.surveillancestartequipoise AS INTEGER) surveillancestartequipoise,
	CAST(a0.surveillanceendequipoise AS INTEGER) surveillanceendequipoise,
	CAST(a0.ageequipoise AS INTEGER) ageequipoise,
	CAST(a0.censoreddate AS DATE) censoreddate
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
COMMENT ON COLUMN personsurveillance.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN personsurveillance.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personsurveillance.birthdate IS 'Best estimate of the birth date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case.';
COMMENT ON COLUMN personsurveillance.deceaseddate IS 'Best estimate of the deceased date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.immigratedate IS 'Best estimate of the immigration date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.emigratedate IS 'Best estimate of the emigration date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN personsurveillance.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN personsurveillance.extremumstart IS 'Start date of the observation bounds of the person rectified by the immigration and birth dates.';
COMMENT ON COLUMN personsurveillance.extremumend IS 'End date of the observation bounds of the person rectified by the emigration and deceased dates.';
COMMENT ON COLUMN personsurveillance.birthdateequipoise IS 'Lower and upper estimates of birth date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.deceaseddateequipoise IS 'Lower and upper estimates of deceased date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.birthobservationequipoise IS 'Extremums agree on occurrence of birth event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.deceasedobservationequipoise IS 'Extremums agree on occurrence of deceased event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.immigratedateequipoise IS 'Lower and upper estimates of immigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.emigratedateequipoise IS 'Lower and upper estimates of emigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.immigrateobservationequipoise IS 'Extremums agree on occurrence of immigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.emigrateobservationequipoise IS 'Extremums agree on occurrence of emigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.surveillancestartequipoise IS 'Extremums start dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.surveillanceendequipoise IS 'Extremums end dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.ageequipoise IS 'Extremums have the same age in the fiscal year: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.censoreddate IS 'First day of the month of the last refresh of the data.';