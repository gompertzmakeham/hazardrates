WITH
	coveragedata AS
	(
		SELECT
			a0.uliabphn,
			MIN(a0.albertacoverage) albertacoverage
		FROM
			surveycontinuingcare a0
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
	
-- Tabulate inconsistent records ingested from continuing care
WITH
	assertiondata AS
	(
		SELECT
			'Continuing Care' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.first_appear_date) IS NULL THEN
					1
				ELSE
					0
			END startinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.last_known_date) IS NULL THEN
					1
				ELSE
					0
			END endinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) > hazardutilities.cleandate(a0.deceased_date) THEN
					1
				ELSE
					0
			END birthdeceasedinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) > hazardutilities.cleandate(a0.last_known_date) THEN
					1
				ELSE
					0
			END birthendinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.deceased_date) < hazardutilities.cleandate(a0.first_appear_date) THEN
					1
				ELSE
					0
			END deceasedstartinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.deceased_date) > hazardutilities.cleandate(a0.last_known_date) THEN
					1
				ELSE
					0
			END deceasedendinconsistent
		FROM
			TABLE(continuing_care.home_care.get_client) a0
		UNION ALL
		SELECT
			'ACCIS' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_date) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.admit_date) IS NULL THEN
					1
				ELSE
					0
			END startinconsistent,
			0 endinconsistent,
			0 birthdeceasedinconsistent,
			CASE
				WHEN a0.birth_date > a0.discharge_date THEN
					1
				ELSE
					0
			END birthendinconsistent,
			0 deceasedstartinconsistent,
			0 deceasedendinconsistent
		FROM
			TABLE(continuing_care.accis.get_adt) a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.startinconsistent,
	a0.endinconsistent,
	a0.birthdeceasedinconsistent,
	a0.birthendinconsistent,
	a0.deceasedstartinconsistent,
	a0.deceasedendinconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.startinconsistent,
	a0.endinconsistent,
	a0.birthdeceasedinconsistent,
	a0.birthendinconsistent,
	a0.deceasedstartinconsistent,
	a0.deceasedendinconsistent
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	6 ASC NULLS FIRST,
	7 ASC NULLS FIRST,
	8 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from continuing care
WITH
	assertiondata AS
	(
		SELECT
			'Continuing Care' sourcesystem,
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