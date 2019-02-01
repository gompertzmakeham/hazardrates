ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censuspharmacydispense NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest pharmacy dispensing
	eventdata AS
	(
		SELECT
			a0.rcpt_uli uliabphn,
			a0.dspn_date dispensedate,
			a0.fac_key_di360 siteidentifier,
			CASE a1.dspn_triplicate_boo WHEN 'T' THEN 0 ELSE 1 END standardtherapeutic,
			CASE a1.dspn_triplicate_boo WHEN 'T' THEN 1 ELSE 0 END controlledtherapeutic
		FROM
			ahsdata.pin_dspn a1
		WHERE
			hazardutilties.cleanprid(a1.prscb_prid) IS NOT NULL
			AND
			a1.supp_drug_atc_code IS NOT NULL
			AND
			a1.drug_prod_id = a1.drug_din
			AND
			a1.drug_triplicate_boo = a1.dspn_triplicate_boo
			AND
			a1.nhp_boo = 'F'
			AND
			a1.dspn_prod_id_tp_code = 'DIN'
			AND
			a1.fac_key_di360 IS NOT NULL
			AND
			a1.dspn_act_tp_code <> 'Z'
	),

	-- Digest to one record per person per day per pharmacy
	sitedata AS
	(
		SELECT
			a0.uliabphn,
			a0.dispensedate,
			a0.siteidentifier,
			MAX(a0.standardtherapeutic) standardtherapeutic,
			MAX(a0.controlledtherapeutic) controlledtherapeutic,
			SUM(a0.standardtherapeutic) standardtherapeutics,
			SUM(a0.controlledtherapeutic) controlledtherapeutics,
			COUNT(*) alltherapeutics
		FROM
			eventdata a0
		GROUP BY
			a0.uliabphn,
			a0.dispensedate,
			a0.siteidentifier
	),

	-- Digest to one record per patient per day
	daydata AS
	(
		SELECT
			a0.uliabphn,
			a0.dispensedate,
			MAX(a0.standardtherapeutic) standardtherapeutic,
			MAX(a0.controlledtherapeutic) controlledtherapeutic,
			SUM(a0.standardtherapeutics) standardtherapeutics,
			SUM(a0.controlledtherapeutics) controlledtherapeutics,
			SUM(a0.alltherapeutics) alltherapeutics,
			SUM(a0.standardtherapeutic) standardsites,
			SUM(a0.controlledtherapeutic) controlledsites,
			COUNT(*) allsites
		FROM
			sitedata a0
		GROUP BY
			a0.uliabphn,
			a0.dispensedate
	)

-- Digest to one record per person per census interval partitioning the surveillance span
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	SUM(a0.standardtherapeutics) standardtherapeutics,
	SUM(a0.controlledtherapeutics) controlledtherapeutics,
	SUM(a0.alltherapeutics) alltherapeutics,
	SUM(a0.standardsites) standardsitedays,
	SUM(a0.controlledsites) controlledsitedays,
	SUM(a0.allsites) allsitedays,
	SUM(a0.standardtherapeutic) standarddays,
	SUM(a0.controlledtherapeutic) controlleddays,
	COUNT(*) alldays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
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
COMMENT ON COLUMN censuspharmacydispense.standardtherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlledtherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alltherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN censuspharmacydispense.standardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.allsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censuspharmacydispense.standarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alldays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';