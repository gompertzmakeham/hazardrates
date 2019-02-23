CREATE MATERIALIZED VIEW censuspharmacydispense NOLOGGING NOCOMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Ingest pharmacy dispensing
	eventdata AS
	(
		SELECT
			hazardutilities.cleanphn(a0.rcpt_uli) uliabphn,
			a0.dspn_date dispensedate,
			a0.fac_key_di360 siteidentifier,
			a0.dspn_day_supply_qty dailydoses,
			CASE a0.dspn_triplicate_boo WHEN 'T' THEN 0 ELSE 1 END standardtherapeutic,
			CASE a0.dspn_triplicate_boo WHEN 'T' THEN 1 ELSE 0 END controlledtherapeutic
		FROM
			ahsdrrconform.cf_pin_dspn a0
		WHERE
			hazardutilities.cleanphn(a0.rcpt_uli) IS NOT NULL
			AND
			a0.supp_drug_atc_code IS NOT NULL
			AND
			hazardutilities.cleanprid(a0.prscb_prid) IS NOT NULL
			AND
			a0.dspn_prod_id = a0.drug_din
			AND
			a0.drug_triplicate_boo = a0.dspn_triplicate_boo
			AND
			a0.nhp_boo = 'F'
			AND
			a0.dspn_prod_id_tp_code = 'DIN'
			AND
			a0.fac_key_di360 IS NOT NULL
			AND
			a0.dspn_act_tp_code <> 'Z'
			AND
			a0.dspn_amt_qty > 0
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
			SUM(a0.standardtherapeutic * a0.dailydoses) standarddailydoses,
			SUM(a0.controlledtherapeutic * a0.dailydoses) controlleddailydoses,
			SUM(a0.dailydoses) alldailydoses,
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
			SUM(a0.standarddailydoses) standarddailydoses,
			SUM(a0.controlleddailydoses) controlleddailydoses,
			SUM(a0.alldailydoses) alldailydoses,
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
	CAST(a0.uliabphn AS INTEGER) uliabphn,
	CAST(a0.cornercase AS VARCHAR2(1)) cornercase,
	CAST(a2.intervalstart AS DATE) intervalstart,
	CAST(a2.intervalend AS DATE) intervalend,
	CAST(SUM(a1.standarddailydoses) AS INTEGER) standarddailydoses,
	CAST(SUM(a1.controlleddailydoses) AS INTEGER) controlleddailydoses,
	CAST(SUM(a1.alldailydoses) AS INTEGER) alldailydoses,
	CAST(SUM(a1.standardtherapeutics) AS INTEGER) standardtherapeutics,
	CAST(SUM(a1.controlledtherapeutics) AS INTEGER) controlledtherapeutics,
	CAST(SUM(a1.alltherapeutics) AS INTEGER) alltherapeutics,
	CAST(SUM(a1.standardsites) AS INTEGER) standardsitedays,
	CAST(SUM(a1.controlledsites) AS INTEGER) controlledsitedays,
	CAST(SUM(a1.allsites) AS INTEGER) allsitedays,
	CAST(SUM(a1.standardtherapeutic) AS INTEGER) standarddays,
	CAST(SUM(a1.controlledtherapeutic) AS INTEGER) controlleddays,
	CAST(COUNT(*) AS INTEGER) alldays
FROM
	personsurveillance a0
	INNER JOIN
	daydata a1
	ON
		a0.uliabphn = a1.uliabphn
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.dispensedate, a0.birthdate)) a2
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
COMMENT ON COLUMN censuspharmacydispense.standarddailydoses IS 'Number of daily doses dispensed of therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlleddailydoses IS 'Number of daily doses dispensed of therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alldailydoses IS 'Number of daily doses dispensed of all therapeutics.';
COMMENT ON COLUMN censuspharmacydispense.standardtherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlledtherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alltherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN censuspharmacydispense.standardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.allsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censuspharmacydispense.standarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.controlleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censuspharmacydispense.alldays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';