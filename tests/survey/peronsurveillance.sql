-- Test for invalid combinations of data at the person surveillance level
WITH
	assertiondata AS
	(
		SELECT
			a0.cornercase,
			a0.uliabphn,
			CASE
				WHEN a0.deceaseddate < a0.birthdate THEN
					1
				ELSE
					0
			END birthdeceasederror,
			CASE
				WHEN a0.extremumend < a0.extremumstart THEN
					1
				ELSE
					0
			END startenderror,
			CASE
				WHEN a0.extremumstart < a0.birthdate THEN
					1
				ELSE
					0
			END birthstarterror,
			CASE
				WHEN a0.deceaseddate < a0.extremumend THEN
					1
				ELSE
					0
			END enddeceasederror,
			a0.surveillancebirth * a0.surveillanceimmigrate birthimmigrateerror,
			a0.surveillancedeceased * a0.surveillanceemigrate deceasedemigrateerror,
			CASE
				WHEN a0.birthdate = a0.extremumstart AND a0.surveillancebirth = 0 THEN
					1
				WHEN a0.birthdate < a0.extremumstart AND a0.surveillancebirth = 1 THEN
					1
				ELSE
					0
			END birtherror,
			CASE
				WHEN a0.deceaseddate IS NOT NULL AND a0.surveillancedeceased = 0 THEN
					1
				WHEN a0.deceaseddate IS NULL AND a0.surveillancedeceased = 1 THEN
					1
				ELSE
					0
			END deceasederror,
			CASE
				WHEN MAX(CASE a0.cornercase WHEN 'L' THEN a0.birthdate ELSE NULL END) OVER (PARTITION BY a0.uliabphn) < MIN(CASE a0.cornercase WHEN 'U' THEN a0.birthdate ELSE NULL END) OVER (PARTITION BY a0.uliabphn) THEN
					1
				ELSE
					0
			END birthlimitserror,
			CASE WHEN MAX(CASE a0.cornercase WHEN 'U' THEN a0.deceaseddate ELSE NULL END) OVER (PARTITION BY a0.uliabphn) < MIN(CASE a0.cornercase WHEN 'L' THEN a0.deceaseddate ELSE NULL END) OVER (PARTITION BY a0.uliabphn) THEN
				1
			ELSE
				0
			END deceasedlimitserror
		FROM
			personsurveillance a0
	)
SELECT
	a0.cornercase,
	a0.birthdeceasederror,
	a0.startenderror,
	a0.birthstarterror,
	a0.enddeceasederror,
	a0.birthimmigrateerror,
	a0.deceasedemigrateerror,
	a0.birtherror,
	a0.deceasederror,
	a0.birthlimitserror,
	a0.deceasedlimitserror,
	COUNT(*) personcount
FROM
	assertiondata a0
GROUP BY
	a0.cornercase,
	a0.birthdeceasederror,
	a0.startenderror,
	a0.birthstarterror,
	a0.enddeceasederror,
	a0.birthimmigrateerror,
	a0.deceasedemigrateerror,
	a0.birtherror,
	a0.deceasederror,
	a0.birthlimitserror,
	a0.deceasedlimitserror;

-- Make sure every person has two records
SELECT
	COUNT(*) OVER (PARTITION BY a0.uliabphn) intervals,
	COUNT(DISTINCT a0.cornercase) OVER (PARTITION BY a0.uliabphn) cases,
	a0.*
FROM
	personsurveillance a0
ORDER BY
	1 ASC NULLS FIRST,
	3 ASC NULLS FIRST;