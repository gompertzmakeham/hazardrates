ALTER SESSION FORCE PARALLEL DDL PARALLEL 8;
ALTER SESSION FORCE PARALLEL DML PARALLEL 8;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8;
CREATE MATERIALIZED VIEW censusprimarycare NOLOGGING NOCOMPRESS PARALLEL 8 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
WITH
	eventdata AS
	(
		SELECT
			COALESCE(a0.prvd_prid, a1.prvd_prid) provideridentifier,
			a0.se_start_date visitdate,
			a0.pers_capb_prvd_spec_ad providerspecialty,
			a0.pers_capb_prvd_skill_code_cls providerskill,
			a0.prvd_skill_type_cls providerdesignation,
			a0.prvd_role_type_cls providerrole,
			a0.delv_site_fac_id_cls,
			a0.delv_site_functr_type_code,
			a0.delv_site_functr_code_cls,
			a0.delv_site_type_cls,
			a0.delv_site_fac_type_code,
			a0.delv_site_unreg_type_code,
			a1.*,
			a0.*
		FROM
			ahsdata.ab_claims a0
			LEFT JOIN
			ahsdata.ab_claim_ah_ref_rnd_id a1
			ON
				a0.rnd_id = a1.rnd_id
		WHERE
			COALESCE(a0.pgm_app_ind, 'F') = 'F'
			AND
			a0.fre_actual_paid_amt > 0
			AND
			a0.prvd_in_prov_ind_ad = 'I'
			AND
			hsp_displn_type_cls = 'MEDDS'
			AND
			a0.pgm_subtype_cls IN ('BASCMEDC', 'BASCMEDE', 'BASCMEMS')
			AND
			(
				a0.delv_site_functr_type_code = 'POFF'
				OR
				a0.delv_site_functr_code_cls = 'FCC'
				OR
				(a0.delv_site_functr_type_code = 'AMBU' AND a0.delv_site_functr_code_cls = 'CLNC')
				OR
				(a0.delv_site_functr_type_code = 'DGTS' AND a0.delv_site_functr_code_cls IN ('CLAB', 'DIMG', 'ELEC', 'OLAB'))
			)
	)
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
	COUNT(DISTINCT CASE a1.dspn_triplicate_boo WHEN 'T' THEN NULL ELSE a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') END) standardsitedays,
	COUNT(DISTINCT CASE a1.dspn_triplicate_boo WHEN 'T' THEN a1.fac_key_di360 || '-' || to_char(a1.dspn_date, 'YYYYMMDD') ELSE NULL END) controlledsitedays,
	COUNT(*) alltherapeutics,
	SUM(CASE a1.dspn_triplicate_boo WHEN 'T' THEN 0 ELSE 1 END) standardtherapeutics,
	SUM(CASE a1.dspn_triplicate_boo WHEN 'T' THEN 1 ELSE 0 END) controlledtherapeutics
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
		AND
		a1.dspn_date BETWEEN a0.extremumstart AND a0.extremumend
	CROSS JOIN
	TABLE(hazardutilities.generatecensus(a1.dspn_date, a0.birthdate)) a2
GROUP BY
	a0.uliabphn,
	a0.cornercase,
	a2.intervalstart,
	a2.intervalend;
	
COMMENT ON MATERIALIZED VIEW censusprimarycare IS 'Utilization of community pharmacy dispensing of physician prescriptions of behind the counter therapeutics in census intervals of each person.';
COMMENT ON COLUMN censusprimarycare.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN censusprimarycare.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN censusprimarycare.intervalstart IS 'Start date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.intervalend IS 'End date of the census interval which intersects with the event.';
COMMENT ON COLUMN censusprimarycare.alldays IS 'Number of unique days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censusprimarycare.standarddays IS 'Number of unique days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censusprimarycare.controlleddays IS 'Number of unique days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censusprimarycare.allsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed any prescription.';
COMMENT ON COLUMN censusprimarycare.standardsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN censusprimarycare.controlledsitedays IS 'Number of unique combinations of pharmacies and days in the census interval when the person was dispensed a prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN censusprimarycare.alltherapeutics IS 'Number of distinct dispensed therapeutics.';
COMMENT ON COLUMN censusprimarycare.alltherapeutics IS 'Number of distinct dispensed therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN censusprimarycare.alltherapeutics IS 'Number of distinct dispensed therapeutics subject to controlled substances regulations.';

CASE
WHEN delv_site_functr_type_code = 'POFF' THEN 1
WHEN delv_site_functr_type_code = 'AMBU' AND delv_site_functr_code_cls = 'CLNC' THEN 1
ELSE 0
END AS primaryc_flag,
CASE
when pers_capb_prvd_spec_ad IN ('GP') then 1
ELSE 0
END AS gp_flag 