-- Tabulate inconsistent records ingested from inpatient care
WITH
	assertiondata AS
	(
		SELECT
			'DADS' sourcesystem,
			CASE
				WHEN hazardutilities.cleandate(a0.birthdate) IS NULL THEN
					1
				ELSE
					0
			END birthinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.admitdate) IS NULL THEN
					1
				ELSE
					0
			END admitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.disdate) IS NULL THEN
					1
				ELSE
					0
			END dischargeinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birthdate) > hazardutilities.cleandate(a0.admitdate) THEN
					1
				ELSE
					0
			END birthadmitinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.birthdate) > hazardutilities.cleandate(a0.disdate) THEN
					1
				ELSE
					0
			END birthdischargeinconsistent,
			CASE
				WHEN hazardutilities.cleandate(a0.admitdate) > hazardutilities.cleandate(a0.disdate) THEN
					1
				ELSE
					0
			END admitdischargeinconsistent
		FROM
			ahsdata.ahs_ip_doc_dx_w_lloyd a0
	)
SELECT
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.admitinconsistent,
	a0.dischargeinconsistent,
	a0.birthadmitinconsistent,
	a0.birthdischargeinconsistent,
	a0.admitdischargeinconsistent,
	COUNT(*) recordcount
FROM
	assertiondata a0
GROUP BY
	a0.sourcesystem,
	a0.birthinconsistent,
	a0.admitinconsistent,
	a0.dischargeinconsistent,
	a0.birthadmitinconsistent,
	a0.birthdischargeinconsistent,
	a0.admitdischargeinconsistent
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST,
	6 ASC NULLS FIRST,
	7 ASC NULLS FIRST;
	
-- Tabulate inconsistent records digested from inpatient care
WITH
	assertiondata AS
	(
		SELECT
			'Inpatient Care' sourcesystem,
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