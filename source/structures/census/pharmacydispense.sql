ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censuspharmacydispense NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	COUNT(DISTINCT a1.dspn_date) alldays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN NULL ELSE a1.dspn_date END) standarddays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN a1.dspn_date ELSE NULL END) controlleddays,
	COUNT(DISTINCT a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD')) allsitedays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN NULL ELSE a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') END) standardsitedays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') ELSE NULL END) controlledsitedays,
	COUNT(*) alltherapeutics,
	SUM(CASE a1.drug_triplicate_boo WHEN 'T' THEN 0 ELSE 1 END) standardtherapeutics,
	SUM(CASE a1.drug_triplicate_boo WHEN 'T' THEN 1 ELSE 0 END) controlledtherapeutics
FROM
	personsurveillance a0
	INNER JOIN
	ahsdata.pin_dspn a1
	ON
		a0.uliabphn = a1.rcpt_uli
		AND
		a1.prscb_prid IS NOT NULL
		AND
		a1.supp_drug_atc_code IS NOT NULL
		AND
		a1.nhp_boo = 'F'
		AND
		a1.dspn_prod_id_tp_code = 'DIN'
		AND
		a1.fac_key_di360 IS NOT NULL
		AND
		a1.dspn_act_tp_code <> 'Z'
		AND
		a1.dspn_date BETWEEN a0.extremumstart AND a0.extremumend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.dspn_date, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;
	
COMMENT ON MATERIALIZED VIEW censuspharmacydispense IS 'Utilization of community pharmacy dispensing of physician prescriptions of behind the counter therapeutics in census intervals of each person.';
COMMENT ON COLUMN censuspharmacydispense.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censuspharmacydispense.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censuspharmacydispense.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuspharmacydispense.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censuspharmacydispense.alldays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censuspharmacydispense.standarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.allsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censuspharmacydispense.standardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alltherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN censuspharmacydispense.alltherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alltherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';