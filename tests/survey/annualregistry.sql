-- Tabulate inconsistent records ingested from annual registry
WITH
	assertiondata AS
	(
		SELECT
			'Registry' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_dt) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.fye - 1 || '0401') IS NULL THEN
					1
				ELSE
					0
			END fiscalinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.pers_reap_end_date) < hazardutilities.cleandate(a0.birth_dt) THEN
					1
				ELSE
					0
			END birthleaveinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birth_dt) > hazardutilities.cleandate(a0.fye || '0331') THEN
					1
				ELSE
					0
			END birthendinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.pers_reap_end_date) < hazardutilities.cleandate(a0.fye - 1 || '0401') THEN
					1
				ELSE
					0
			END leavestartinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.pers_reap_end_date) > hazardutilities.cleandate(a0.fye || '0331') THEN
					1
				ELSE
					0
			END leaveendinconsistent
		FROM
			ahsdata.provincial_registry a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.fiscalinconsistent,
	a0.birthleaveinconsistent,
	a0.birthendinconsistent,
	a0.leavestartinconsistent,
	a0.leaveendinconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.fiscalinconsistent,
	a0.birthleaveinconsistent,
	a0.birthendinconsistent,
	a0.leavestartinconsistent,
	a0.leaveendinconsistent
ORDER BY
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	6 ASC NULLS FIRST,
	7 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from annual registry
WITH
	assertiondata AS
	(
		SELECT
			'Annual Registry' sourcesystem,
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
			surveyannualregistry a0
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