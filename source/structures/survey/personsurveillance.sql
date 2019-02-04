ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW personsurveillance NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT
	a0.*
FROM
	TABLE(surveillanceutilities.generateoutput) a0;

COMMENT ON MATERIALIZED VIEW personsurveillance IS 'For every person that at any time was covered by Alberta Healthcare Insurance list two surveillance intervals for each person, representing the corner cases of possible values for the birth and death dates. In the case of observational equipose the two records are identical.';
COMMENT ON COLUMN personsurveillance.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personsurveillance.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN personsurveillance.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personsurveillance.birthdateequipoise IS 'Lower and upper estimates of birth date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.deceaseddateequipoise IS 'Lower and upper estimates of deceased date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.birthequipoise IS 'Extremums agree on occurrence of birth event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.deceasedequipoise IS 'Extremums agree on occurrence of deceased event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.immigrateequipoise IS 'Extremums agree on occurrence of immigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.emigrateequipoise IS 'Extremums agree on occurrence of emigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.startequipoise IS 'Extremums start dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.endequipoise IS 'Extremums end dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.ageequipoise IS 'Extremums have the same age in the fiscal year: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.birthdate IS 'Best estimate of the birth date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case.';
COMMENT ON COLUMN personsurveillance.deceaseddate IS 'Best estimate of the deceased date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personsurveillance.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN personsurveillance.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN personsurveillance.extremumstart IS 'Start date of the observation bounds of the person rectified by the birth date.';
COMMENT ON COLUMN personsurveillance.extremumend IS 'End date of the observation bounds of the person rectified by the deceased date.';
COMMENT ON COLUMN personsurveillance.surveillancebirth IS 'Surveillance interval starts on the persons birth: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.surveillancedeceased IS 'Surveillance interval ends on the persons death: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.surveillanceimmigrate IS 'Surveillance interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.surveillanceemigrate IS 'Surveillance interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN personsurveillance.censoreddate IS 'First day of the month of the last refresh of the data.';