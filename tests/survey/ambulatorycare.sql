-- Tabulate inconsistent records ingested from ambulatory care
WITH
	assertiondata AS
	(
		SELECT
			'AACRS' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birthdate) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.visdate) IS NULL THEN
					1
				ELSE
					0
			END visitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disdate) IS NULL THEN
					1
				ELSE
					0
			END dispositioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.visdate) < hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
			END birthvisitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disdate) < hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
			END birthdispositioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disdate) < hazardutilities.cleandate(a0.visdate) THEN
					1
				ELSE
					0
			END visitdispositioninconsistent
		FROM
			ahsdata.ahs_ambulatory a0
		UNION ALL
		SELECT
			'NACRS' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birthdate) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.visit_date) IS NULL THEN
					1
				ELSE
					0
			END visitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disp_date) IS NULL THEN
					1
				ELSE
					0
			END dispositioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.visit_date) < hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
				END birthvisitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disp_date) < hazardutilities.cleandate(a0.birthdate) THEN
					1
				ELSE
					0
			END birthdispositioninconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disp_date) < hazardutilities.cleandate(a0.visit_date) THEN
					1
				ELSE
					0
			END visitdispositioninconsistent
		FROM
			ahsdata.ahs_nacrs_main a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.visitinconsistent,
	a0.dispositioninconsistent,
	a0.birthvisitinconsistent,
	a0.birthdispositioninconsistent,
	a0.visitdispositioninconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.visitinconsistent,
	a0.dispositioninconsistent,
	a0.birthvisitinconsistent,
	a0.birthdispositioninconsistent,
	a0.visitdispositioninconsistent
ORDER BY
	1 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	7 ASC NULLS FIRST,
	6 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from ambulatory care
WITH
	assertiondata AS
	(
		SELECT
			'Ambulatory' sourcesystem,
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
			surveyambulatorycare a0
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