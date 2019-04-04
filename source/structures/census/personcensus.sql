CREATE OR REPLACE VIEW personcensus AS
SELECT

	/*+ cardinality(a1, 1) */
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.sex AS VARCHAR2(1)) sex,
	CAST(a0.firstnations AS INTEGER) firstnations,
	CAST(a0.maternalphn AS INTEGER) maternalphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a0.birthdate AS DATE) birthdate,
	CAST(a0.deceaseddate AS DATE) deceaseddate,
	CAST(a0.immigratedate AS DATE) immigratedate,
	CAST(a0.emigratedate AS DATE) emigratedate,
	CAST(a0.surveillancestart AS DATE) surveillancestart,
	CAST(a0.surveillanceend AS DATE) surveillanceend,
	CAST(a0.extremumstart AS DATE) extremumstart,
	CAST(a0.extremumend AS DATE) extremumend,
	CAST(a1.censusstart AS DATE) censusstart,
	CAST(a1.censusend AS DATE) censusend,
	CAST(a1.agestart AS DATE) agestart,
	CAST(a1.ageend AS DATE) ageend,
	CAST(a1.intervalstart AS DATE) intervalstart,
	CAST(a1.intervalend AS DATE) intervalend,
	CAST(a1.durationstart AS DATE) durationstart,
	CAST(a1.durationend AS DATE) durationend,
	CAST(a1.durationdays AS INTEGER) durationdays,
	CAST(a1.intervalage AS INTEGER) intervalage,
	CAST(a1.agecoincideinterval AS INTEGER) agecoincideinterval,
	CAST(a1.agecoincidecensus AS INTEGER) agecoincidecensus,
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
	CASE
		WHEN a1.durationstart <= a0.birthdate THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalbirth,
	CASE
		WHEN a0.deceaseddate <= a1.durationend THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervaldeceased,
	CASE
		WHEN a1.durationstart <= a0.immigratedate THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalimmigrate,
	CASE
		WHEN a0.emigratedate <= a1.durationend THEN
			CAST(1 AS INTEGER)
		ELSE
			CAST(0 AS INTEGER)
	END intervalemigrate,
	CAST(a1.intervalfirst AS INTEGER) intervalfirst,
	CAST(a1.intervallast AS INTEGER) intervallast,
	CAST(a1.intervalcount AS INTEGER) intervalcount,
	CAST(a1.intervalorder AS INTEGER) intervalorder,
	CAST(a0.censoreddate AS DATE) censoreddate
FROM
	personsurveillance a0
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a0.extremumstart, a0.extremumend, a0.birthdate)) a1
WITH READ ONLY;

COMMENT ON TABLE personcensus IS 'For every person that at any time was covered by Alberta Healthcare Insurance partition the surviellance interval by the intersections of fiscal years and age years, rectified by the start and end of the surveillance interval.';
COMMENT ON COLUMN personcensus.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personcensus.sex IS 'Biological sex for use in physiological and metabolic determinants of health, not self identified gender: F female, M male.';
COMMENT ON COLUMN personcensus.firstnations IS 'Presence of adminstrative indications of membership or status in first nations, aboriginal, indigenous, Metis, or Inuit communities: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.maternalphn IS 'When known the unique lifetime identifier of the mother, Alberta provincial healthcare number, null otherwise.';
COMMENT ON COLUMN personcensus.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personcensus.birthdate IS 'Best estimate of the birth date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case.';
COMMENT ON COLUMN personcensus.deceaseddate IS 'Best estimate of the deceased date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personcensus.immigratedate IS 'Best estimate of the immigration date from all adminstrative records, either least (U) or greatest (L) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personcensus.emigratedate IS 'Best estimate of the emigration date from all adminstrative records, either least (L) or greatest (U) bound, depending on the corner case, null when unknown.';
COMMENT ON COLUMN personcensus.surveillancestart IS 'Start date of the observation bounds of the person.';
COMMENT ON COLUMN personcensus.surveillanceend IS 'End date of the observation bounds of the person.';
COMMENT ON COLUMN personcensus.extremumstart IS 'Start date of the observation bounds of the person rectified by the immigration and birth dates.';
COMMENT ON COLUMN personcensus.extremumend IS 'End date of the observation bounds of the person rectified by the emigration and deceased dates.';
COMMENT ON COLUMN personcensus.censusstart IS 'Closed start of the fiscal year, April 1.';
COMMENT ON COLUMN personcensus.censusend IS 'Closed end of the fiscal year, March 31.';
COMMENT ON COLUMN personcensus.agestart IS 'Closed start of the age interval.';
COMMENT ON COLUMN personcensus.ageend IS 'Closed end of the age interval, the next birthday minus one day.';
COMMENT ON COLUMN personcensus.intervalstart IS 'Closed start of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personcensus.intervalend IS 'Closed end of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personcensus.durationstart IS 'Closed start of the interval, rectified to the start of the surveillance interval.';
COMMENT ON COLUMN personcensus.durationend IS 'Closed end of the interval, rectified to the end of the surveillance interval.';
COMMENT ON COLUMN personcensus.durationdays IS 'Duration of the interval in days, an integer starting at 1, using the convention that the interval is closed so that the duration is end minus start plus one day.';
COMMENT ON COLUMN personcensus.intervalage IS 'Age in years at the start of the interval, an integer starting at 0.';
COMMENT ON COLUMN personcensus.agecoincideinterval IS 'Interval starts on the birthday: 1 yes, 0 no';
COMMENT ON COLUMN personcensus.agecoincidecensus IS 'Birthday is the start of the fical year (April 1): 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.birthdateequipoise IS 'Lower and upper estimates of birth date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.deceaseddateequipoise IS 'Lower and upper estimates of deceased date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.birthobservationequipoise IS 'Extremums agree on occurrence of birth event: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.deceasedobservationequipoise IS 'Extremums agree on occurrence of deceased event: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.immigratedateequipoise IS 'Lower and upper estimates of immigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.emigratedateequipoise IS 'Lower and upper estimates of emigrate date are equal: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.immigrateobservationequipoise IS 'Extremums agree on occurrence of immigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.emigrateobservationequipoise IS 'Extremums agree on occurrence of emigration event: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.surveillancestartequipoise IS 'Extremums start dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.surveillanceendequipoise IS 'Extremums end dates agree: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.ageequipoise IS 'Extremums have the same age in the fiscal year: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalbirth IS 'Census interval starts on the persons birth: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervaldeceased IS 'Census interval ends on the persons death: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalimmigrate IS 'Census interval starts on the persons immigration: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalemigrate IS 'Census interval ends on the persons emigration: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalfirst IS 'First interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervallast IS 'Last interval in the partition: 1 yes, 0 no.';
COMMENT ON COLUMN personcensus.intervalcount IS 'Number of sub-intervals in the partition of the surveillance interval.';
COMMENT ON COLUMN personcensus.intervalorder IS 'Ascending ordinal of the sub-interval in the partition of the surveillance interval.';
COMMENT ON COLUMN personcensus.censoreddate IS 'First day of the month of the last refresh of the data.';