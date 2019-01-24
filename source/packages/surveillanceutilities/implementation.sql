CREATE OR REPLACE PACKAGE BODY surveillanceutilities AS

	/*
	 *  An individual person reduced from events.
	 */
	CURSOR generateinput RETURN inputinterval IS
	WITH

		-- Ingest all sources of person events
		ingestevents AS
		(
			SELECT a0.* FROM surveyambulatorycare a0 UNION ALL
			SELECT a0.* FROM surveyannualregistry a0 UNION ALL
			SELECT a0.* FROM surveycontinuingcare a0 UNION ALL
			SELECT a0.* FROM surveyinpatientcare a0 UNION ALL
			SELECT a0.* FROM surveylabratorycollection a0 UNION ALL
			SELECT a0.* FROM surveypharmacydispense a0 UNION ALL
			SELECT a0.* FROM surveyprimarycare a0 UNION ALL
			SELECT a0.* FROM surveyvitalstatistics a0
		),
	
		-- Digest to one record per person
		digestevents AS
		(
			SELECT
				a0.uliabphn,
				COALESCE(MIN(a0.sex), 'O') sex,
				COALESCE(MAX(a0.firstnations), 0) firstnations,
				MIN(a0.leastbirth) leastbirth,
				MAX(a0.greatestbirth) greatestbirth,
				MIN(a0.leastdeceased) leastdeceased,
				MAX(a0.greatestdeceased) greatestdeceased,
				least
				(
					MIN(a0.leastend),
					COALESCE(MIN(a0.servicestart), MIN(a0.leastend)),
					COALESCE(MIN(a0.serviceend), MIN(a0.leastend))
				) servicestart,
				greatest
				(
					MAX(a0.greateststart),
					COALESCE(MAX(a0.servicestart), MAX(a0.greateststart)),
					COALESCE(MAX(a0.serviceend), MAX(a0.greateststart))
				) serviceend,
				MIN(a0.surveillancestart) surveillancestart,
				least
				(
					MIN(a0.censoreddate),
					greatest
					(
						MAX(a0.surveillanceend),
						COALESCE(MAX(a0.greatestdeceased), MAX(a0.surveillanceend))
					)
				) surveillanceend,
				COALESCE(MAX(a0.surveillancebirth), 0) surveillancebirth,
				COALESCE(MAX(a0.surveillancedeceased), 0) surveillancedeceased,
				COALESCE(MAX(a0.surveillanceimmigrate), 0) surveillanceimmigrate,
				COALESCE(MAX(a0.surveillanceemigrate), 0) surveillanceemigrate,
				COALESCE(MIN(a0.albertacoverage), 1) albertacoverage,
				MIN(a0.censoreddate) censoreddate
			FROM
				ingestevents a0
			WHERE
				a0.uliabphn IS NOT NULL
			GROUP BY
				a0.uliabphn
		)

	-- Return only covered persons
	SELECT
		a0.uliabphn,
		a0.sex,
		a0.firstnations,
		a0.leastbirth,
		a0.greatestbirth,
		a0.leastdeceased,
		a0.greatestdeceased,
		a0.servicestart,
		a0.serviceend,
		a0.surveillancestart,
		a0.surveillanceend,
		a0.surveillancebirth,
		a0.surveillancedeceased,
		a0.surveillanceimmigrate,
		a0.surveillanceemigrate,
		a0.censoreddate
	FROM
		digestevents a0
	WHERE
		a0.albertacoverage = 1;

	/*
	 *  Generate a pair of surveillance extremum records for each person.
	 */
	FUNCTION generateoutput RETURN outputintervals PIPELINED AS
		returnlower outputinterval;
		returnupper outputinterval;
	BEGIN
		FOR localrow IN generateinput LOOP

			-- Lower (shortest) bound is the greatest birth date and least deceased date
			returnlower.cornercase := 'L';
			returnlower.uliabphn := localrow.uliabphn;
			returnlower.sex := localrow.sex;
			returnlower.firstnations := localrow.firstnations;
			returnlower.censoreddate := localrow.censoreddate;
			returnlower.surveillancestart := localrow.surveillancestart;
			returnlower.surveillanceend := localrow.surveillanceend;

			-- Upper (longest) bound is the least birth date and greatest deceased date
			returnupper.cornercase := 'U';
			returnupper.uliabphn := localrow.uliabphn;
			returnupper.sex := localrow.sex;
			returnupper.firstnations := localrow.firstnations;
			returnupper.censoreddate := localrow.censoreddate;
			returnupper.surveillancestart := localrow.surveillancestart;
			returnupper.surveillanceend := localrow.surveillanceend;

			-- Birth dates of lower (shortest) bound interval
			CASE
				WHEN localrow.greatestbirth IS NOT NULL THEN
					returnlower.birthdate := localrow.greatestbirth;
				WHEN localrow.surveillancebirth = 1 THEN
					returnlower.birthdate := localrow.servicestart;
				ELSE
					returnlower.birthdate := NULL;
			END CASE;

			-- Birth dates of upper (longest) bound interval
			CASE
				WHEN localrow.leastbirth IS NOT NULL THEN
					returnupper.birthdate := localrow.leastbirth;
				WHEN localrow.surveillancebirth = 1 THEN
					returnupper.birthdate := localrow.surveillancestart;
				ELSE
					returnupper.birthdate := NULL;
			END CASE;

			-- Deceased dates of lower (shortest) bound interval
			CASE
				WHEN localrow.leastdeceased IS NOT NULL THEN
					returnlower.deceaseddate := localrow.leastdeceased;
				WHEN localrow.surveillancedeceased = 1 THEN
					returnlower.deceaseddate := localrow.serviceend;
				ELSE
					returnlower.deceaseddate := NULL;
			END CASE;

			-- Deceased dates of upper (longest) bound interval
			CASE
				WHEN localrow.greatestdeceased IS NOT NULL THEN
					returnupper.deceaseddate := localrow.greatestdeceased;
				WHEN localrow.surveillancedeceased = 1 THEN
					returnupper.deceaseddate := localrow.surveillanceend;
				ELSE
					returnupper.deceaseddate := NULL;
			END CASE;

			-- Rectified extremum dates of lower (shortest) bound interval
			returnlower.extremumstart := greatest
			(
				returnlower.surveillancestart,
				returnlower.birthdate
			);
			returnlower.extremumend := least
			(
				returnlower.surveillanceend,
				COALESCE(returnlower.deceaseddate, returnlower.surveillanceend)
			);

			-- Rectified extremum dates of upper (longest) bound interval
			returnupper.extremumstart := greatest
			(
				returnupper.surveillancestart,
				returnupper.birthdate
			);
			returnupper.extremumend := least
			(
				returnupper.surveillanceend,
				COALESCE(returnupper.deceaseddate, returnupper.surveillanceend)
			);

			-- Equipoise of birth dates
			CASE
				WHEN returnupper.birthdate < returnlower.birthdate THEN
					returnlower.birthequipoise := 0;
					returnupper.birthequipoise := 0;
				ELSE
					returnlower.birthequipoise := 1;
					returnupper.birthequipoise := 1;
			END CASE;

			-- Equipoise of deceased dates
			CASE
				WHEN returnlower.deceaseddate < returnupper.deceaseddate THEN
					returnlower.deceasedequipoise := 0;
					returnupper.deceasedequipoise := 0;
				ELSE
					returnlower.deceasedequipoise := 1;
					returnupper.deceasedequipoise := 1;
			END CASE;

			-- Birth and immigration flags of lower (shortest) bound interval
			CASE
				WHEN returnlower.extremumstart <= returnlower.birthdate THEN
					returnlower.surveillancebirth := 1;
					returnlower.surveillanceimmigrate := 0;
				ELSE
					returnlower.surveillancebirth := 0;
					returnlower.surveillanceimmigrate := localrow.surveillanceimmigrate;
			END CASE;

			-- Birth and immigration flags of upper (longest) bound interval
			CASE
				WHEN returnupper.extremumstart <= returnupper.birthdate THEN
					returnupper.surveillancebirth := 1;
					returnupper.surveillanceimmigrate := 0;
				ELSE
					returnupper.surveillancebirth := 0;
					returnupper.surveillanceimmigrate := localrow.surveillanceimmigrate;
			END CASE;

			-- Deceased and emigration flags of lower (shortest) bound interval
			CASE
				WHEN returnlower.deceaseddate IS NOT NULL THEN
					returnlower.surveillancedeceased := 1;
					returnlower.surveillanceemigrate := 0;
				ELSE
					returnlower.surveillancedeceased := 0;
					returnlower.surveillanceemigrate := localrow.surveillanceemigrate;
			END CASE;

			-- Deceased and emigration flags of upper (longest) bound interval
			CASE
				WHEN returnupper.deceaseddate IS NOT NULL THEN
					returnupper.surveillancedeceased := 1;
					returnupper.surveillanceemigrate := 0;
				ELSE
					returnupper.surveillancedeceased := 0;
					returnupper.surveillanceemigrate := localrow.surveillanceemigrate;
			END CASE;

			-- Send only valid intervals where the range of birth dates is disjoint from the
			-- range of deceased dates.
			CASE
				WHEN returnupper.extremumstart <= returnlower.extremumstart
					AND returnlower.extremumstart <= returnlower.extremumend
					AND returnlower.extremumend <= returnupper.extremumend THEN
						PIPE ROW(returnlower);
						PIPE ROW(returnupper);
				ELSE
					NULL;
			END CASE;
		END LOOP;
		RETURN;
	END generateoutput;
END surveillanceutilities;