-- Specialities, designations, skills, and competencies
SELECT
	a0.pers_capb_prvd_spec_ad,
	a0.pers_capb_prvd_skill_code_cls,
	a0.prvd_skill_type_cls,
	a0.prvd_role_type_cls,
	a0.hsp_displn_type_cls,
	COUNT(*) claims
FROM
	ahsdata.ab_claims a0
GROUP BY
	a0.pers_capb_prvd_spec_ad,
	a0.pers_capb_prvd_skill_code_cls,
	a0.prvd_skill_type_cls,
	a0.prvd_role_type_cls,
	a0.hsp_displn_type_cls;

-- Sites, locations, and delivery settings
SELECT
	a0.delv_site_functr_type_code,
	a0.delv_site_functr_code_cls,
	a0.delv_site_type_cls,
	a0.delv_site_fac_type_code,
	a0.delv_site_unreg_type_code,
	COUNT(*) claims
FROM
	ahsdata.ab_claims a0
GROUP BY
	a0.delv_site_functr_type_code,
	a0.delv_site_functr_code_cls,
	a0.delv_site_type_cls,
	a0.delv_site_fac_type_code,
	a0.delv_site_unreg_type_code;