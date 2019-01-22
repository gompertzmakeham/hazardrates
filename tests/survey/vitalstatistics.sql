-- Tabulate inconsistent records ingested from vital statistics
WITH
	assertiondata AS
	(
		SELECT
			'Birth 2000-2016' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			0 deceasedinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.oop_death_) <  hazardutilities.cleandate(a0.birth_date) THEN
					1
				ELSE
					0
			END birthdeceasedinconsistent,
			0 deceasedageinconsistent
		FROM
			vital_stats_dsp.ex_ah_bth_2000_2016 a0
		UNION ALL
		SELECT
			'Birth 2015-2017' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			0 deceasedinconsistent,
			0 birthdeceasedinconsistent,
			0 deceasedageinconsistent
		FROM
			vital_stats_dsp.ahs_bth_2015_2017 a0
		UNION ALL
		SELECT
			'Birth 2000-2015' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			0 deceasedinconsistent,
			0 birthdeceasedinconsistent,
			0 deceasedageinconsistent
		FROM
			vital_stats_dsp.ex_vs_bth_2000_2015 a0
		UNION ALL
		SELECT
			'Deceased 2010-2016' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.dethdate) IS NULL THEN
					1
				ELSE
					0
			END deceasedinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) > hazardutilities.cleandate(a0.dethdate) THEN
					1
				ELSE
					0
			END birthdeceasedinconsistent,
			CASE
				WHEN hazardutilities.ageyears(hazardutilities.cleandate(a0.birth_date), hazardutilities.cleandate(a0.dethdate)) > COALESCE(a0.age_yrs, a0.age) THEN
					1
				ELSE
					0
			END deceasedageinconsistent
		FROM
			vital_stats_dsp.ex_ah_dth_2010_2016 a0
		UNION ALL
		SELECT
			'Deceased 2015-2017' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.dethdate) IS NULL THEN
					1
				ELSE
					0
			END deceasedinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) > hazardutilities.cleandate(a0.dethdate) THEN
					1
				ELSE
					0
			END birthdeceasedinconsistent,
			CASE
				WHEN hazardutilities.ageyears(hazardutilities.cleandate(a0.birth_date), hazardutilities.cleandate(a0.dethdate)) > COALESCE(a0.age_yrs, a0.age) THEN
					1
				ELSE
					0
			END deceasedageinconsistent
		FROM
			vital_stats_dsp.ahs_dth_2015_2017 a0
		UNION ALL
		SELECT
			'Deceased' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.dethdate) IS NULL THEN
					1
				ELSE
					0
			END deceasedinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) > hazardutilities.cleandate(a0.dethdate) THEN
					1
				ELSE
					0
			END birthdeceasedinconsistent,
			CASE
				WHEN hazardutilities.ageyears(hazardutilities.cleandate(a0.birth_date), hazardutilities.cleandate(a0.dethdate)) > COALESCE(a0.age_yrs, a0.age) THEN
					1
				ELSE
					0
			END deceasedageinconsistent
		FROM
			vital_stats_dsp.ex_vs_deaths_phn a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.deceasedinconsistent,
	a0.birthdeceasedinconsistent,
	a0.deceasedageinconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.deceasedinconsistent,
	a0.birthdeceasedinconsistent,
	a0.deceasedageinconsistent
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from vital statistics
WITH
	assertiondata AS
	(
		SELECT
			'Vital Statistics' sourcesystem,
			CASE
				WHEN a0.leastbirth > a0.greatestbirth THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN a0.leastdeceased > a0.greatestdeceased THEN
					1
				ELSE
					0
			END deceasedinconsistent,
			CASE
				WHEN a0.servicestart > a0.serviceend THEN
					1
				ELSE
					0
			END serviceinconsistent,
			CASE
				WHEN a0.surveillancestart > a0.servicestart THEN
					1
				ELSE
					0
			END surveyserviceinconsistent,
			CASE
				WHEN a0.serviceend > a0.surveillanceend THEN
					1
				ELSE
					0
			END servicesurveyinconsistent,
			CASE
				WHEN a0.surveillanceend < a0.leastbirth THEN
					1
				ELSE
					0
			END birthsurveyinconsistent,
			CASE
				WHEN a0.greatestdeceased < a0.surveillancestart THEN
					1
				ELSE
					0
			END deceasedsurveyinconsistent
		FROM
			surveycontinuingcare a0
	)
SELECT
	a0.birthinconsistent,
	a0.deceasedinconsistent,
	a0.serviceinconsistent,
	a0.surveyserviceinconsistent,
	a0.servicesurveyinconsistent,
	a0.birthsurveyinconsistent,
	a0.deceasedsurveyinconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.birthinconsistent,
	a0.deceasedinconsistent,
	a0.serviceinconsistent,
	a0.surveyserviceinconsistent,
	a0.servicesurveyinconsistent,
	a0.birthsurveyinconsistent,
	a0.deceasedsurveyinconsistent
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	6 ASC NULLS FIRST,
	7 ASC NULLS FIRST;

-- Vital statistics province identifiers
SELECT
	'Birth 2000-2016' sourcesystem,
	UPPER(a0.prov_lived) provincelived,
	substr(UPPER(a0.m_usual_pc), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ex_ah_bth_2000_2016 a0
GROUP BY
	UPPER(a0.prov_lived),
	substr(UPPER(a0.m_usual_pc), 1, 1)
UNION ALL
SELECT
	'Birth 2015-2017' sourcesystem,
	NULL provincelived,
	substr(UPPER(a0.m_usual_pc), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ahs_bth_2015_2017 a0
GROUP BY
	substr(UPPER(a0.m_usual_pc), 1, 1)
UNION ALL
SELECT
	'Birth 2000-2015' sourcesystem,
	NULL provincelived,
	substr(UPPER(a0.m_usual_pc), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ex_vs_bth_2000_2015 a0
GROUP BY
	substr(UPPER(a0.m_usual_pc), 1, 1)
UNION ALL
SELECT
	'Deaths 2010-2016' sourcesystem,
	UPPER(a0.prov_lived) provincelived,
	substr(UPPER(a0.postcode), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ex_ah_dth_2010_2016 a0
GROUP BY
	UPPER(a0.prov_lived),
	substr(UPPER(a0.postcode), 1, 1)
UNION ALL
SELECT
	'Deaths 2015-2017' sourcesystem,
	UPPER(a0.prov_lived) provincelived,
	substr(UPPER(a0.postcode), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ahs_dth_2015_2017 a0
GROUP BY
	UPPER(a0.prov_lived),
	substr(UPPER(a0.postcode), 1, 1)
UNION ALL
SELECT
	'Deaths' sourcesystem,
	UPPER(a0.prov_lived) provincelived,
	substr(UPPER(a0.postcode), 1, 1) postalprovincial,
	COUNT(*) recordcount
FROM
	vital_stats_dsp.ex_vs_deaths_phn a0
GROUP BY
	UPPER(a0.prov_lived),
	substr(UPPER(a0.postcode), 1, 1)
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST;