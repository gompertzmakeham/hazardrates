CREATE MATERIALIZED VIEW censuspharmacydispense NOLOGGING NOCOMPRESS PARALLEL BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT

	/*+ cardinality(a2, 1) */
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend,
	COUNT(DISTINCT a1.dspn_date) alldays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN NULL ELSE a1.dspn_date END) standarddays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN a1.dspn_date ELSE NULL END) triplicatedays,
	COUNT(DISTINCT a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD')) allsitedays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN NULL ELSE a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') END) standardsitedays,
	COUNT(DISTINCT CASE a1.drug_triplicate_boo WHEN 'T' THEN a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') ELSE NULL END) triplicatesitedays
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