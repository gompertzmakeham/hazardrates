-- Tabulate date equivocation
WITH
	persondata AS
	(
		SELECT
			CASE
				WHEN leastbirth < greatestbirth THEN
					2
				WHEN leastbirth = greatestbirth THEN
					1
				ELSE
					0
			END birthcount,
			
			CASE
				WHEN leastdeceased < greatestdeceased THEN
					2
				WHEN leastdeceased = greatestdeceased THEN
					1
				ELSE
					0
			END deceasedcount,
			a0.ageequipoise,
			a0.birthdateequipoise,
			a0.deceaseddateequipoise,
			a0.uliabphn
		FROM
			persondemographic a0
		GROUP BY
			a0.uliabphn,
			a0.ageequipoise,
			a0.birthdateequipoise,
			a0.deceaseddateequipoise
	)
SELECT
	a0.birthcount,
	a0.deceasedcount,
	a0.birthdateequipoise,
	a0.ageequipoise,
	a0.deceaseddateequipoise,
	COUNT(*) personcount
FROM
	persondata a0
GROUP BY
	a0.birthcount,
	a0.deceasedcount,
	a0.birthdateequipoise,
	a0.ageequipoise,
	a0.deceaseddateequipoise
ORDER BY
	1 ASC NULLS FIRST,
	2 ASC NULLS FIRST,
	3 ASC NULLS FIRST,
	4 ASC NULLS FIRST,
	5 ASC NULLS FIRST;