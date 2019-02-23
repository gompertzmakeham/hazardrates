WITH
	coveragedata AS
	(
		SELECT
			a0.uliabphn,
			MIN(a0.albertacoverage) albertacoverage
		FROM
			surveylaboratorycollection a0
		GROUP BY
			a0.uliabphn
	)
SELECT
	COUNT(*) persons,
	a0.albertacoverage
FROM
	coveragedata a0
GROUP BY
	a0.albertacoverage;

-- Tabulate inconsistent records ingested from laboratory collections
WITH
	assertiondata AS
	(
		SELECT
			'Fusion' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clct_dt) IS NULL THEN
					1
				ELSE
					0
			END collectioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) > hazardutilities.cleandate(a0.clct_dt) THEN
					1
				ELSE
					0
			END birthcollectioninconsistent
		FROM
			ahsdata.lab_lf a0
		UNION ALL
		SELECT
			'Meditech' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clct_dt) IS NULL THEN
					1
				ELSE
					0
			END collectioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) > hazardutilities.cleandate(a0.clct_dt) THEN
					1
				ELSE
					0
			END birthcollectioninconsistent
		FROM
			ahsdata.lab_mt a0
		UNION ALL
		SELECT
			'Millenium' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clct_dt) IS NULL THEN
					1
				ELSE
					0
			END collectioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) > hazardutilities.cleandate(a0.clct_dt) THEN
					1
				ELSE
					0
			END birthcollectioninconsistent
		FROM
			ahsdata.lab_ml a0
		UNION ALL
		SELECT
			'Sunquest' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clct_dt) IS NULL THEN
					1
				ELSE
					0
			END collectioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.clnt_birth_dt) > hazardutilities.cleandate(a0.clct_dt) THEN
					1
				ELSE
					0
			END birthcollectioninconsistent
		FROM
			ahsdata.lab_sq a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.collectioninconsistent,
	a0.birthcollectioninconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.collectioninconsistent,
	a0.birthcollectioninconsistent
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from laboratory collection
WITH
	assertiondata AS
	(
		SELECT
			'Laboratory Collection' sourcesystem,
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
			surveylaboratorycollection a0
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

-- Billing and refering service
SELECT
	'LF' sourcesystem,
	UPPER(a0.clnt_type) clnt_type,
	UPPER(a0.clnt_bill_cd) clnt_bill_cd,
	substr(UPPER(clnt_postal_code), 1, 1) postal_province,
	COUNT(*) assaycount
FROM
	ahsdata.lab_lf a0
GROUP BY
	UPPER(a0.clnt_type),
	UPPER(a0.clnt_bill_cd),
	substr(UPPER(clnt_postal_code), 1, 1)
UNION ALL
SELECT
	'ML' sourcesystem,
	UPPER(a0.clnt_type) clnt_type,
	UPPER(a0.clnt_bill_cd) clnt_bill_cd,
	substr(UPPER(clnt_postal_code), 1, 1) postal_province,
	COUNT(*) assaycount
FROM
	ahsdata.lab_ml a0
GROUP BY
	UPPER(a0.clnt_type),
	UPPER(a0.clnt_bill_cd),
	substr(UPPER(clnt_postal_code), 1, 1)
UNION ALL
SELECT
	'MT' sourcesystem,
	UPPER(a0.clnt_type) clnt_type,
	UPPER(a0.clnt_bill_cd) clnt_bill_cd,
	substr(UPPER(clnt_postal_code), 1, 1) postal_province,
	COUNT(*) assaycount
FROM
	ahsdata.lab_ml a0
GROUP BY
	UPPER(a0.clnt_type),
	UPPER(a0.clnt_bill_cd),
	substr(UPPER(clnt_postal_code), 1, 1)
UNION ALL
SELECT
	'SQ' sourcesystem,
	UPPER(a0.clnt_type) clnt_type,
	UPPER(a0.clnt_bill_cd) clnt_bill_cd,
	substr(UPPER(clnt_postal_code), 1, 1) postal_province,
	COUNT(*) assaycount
FROM
	ahsdata.lab_sq a0
GROUP BY
	UPPER(a0.clnt_type),
	UPPER(a0.clnt_bill_cd),
	substr(UPPER(clnt_postal_code), 1, 1)
ORDER BY
	1 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST;