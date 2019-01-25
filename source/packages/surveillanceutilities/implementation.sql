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
			SELECT a0.* FROM surveylaboratorycollection a0 UNION ALL
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
				CASE
					WHEN MIN(a0.leastbirth) IS NOT NULL AND MAX(a0.greatestbirth) IS NOT NULL THEN
						1
					WHEN MIN(a0.leastbirth) IS NULL AND MAX(a0.greatestbirth) IS NULL THEN
						COALESCE(MAX(a0.surveillancebirth), 0)
					ELSE
						0
				END consistentbirth,
				CASE
					WHEN MIN(a0.leastdeceased) IS NOT NULL AND MAX(a0.greatestdeceased) IS NOT NULL THEN
						1
					WHEN MIN(a0.leastdeceased) IS NULL AND MAX(a0.greatestdeceased) IS NULL THEN
						1
					ELSE
						0
				END consistentdeceased,
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
		COALESCE(a0.leastbirth, a0.surveillancestart) leastbirth,
		COALESCE(a0.greatestbirth, a0.servicestart) greatestbirth,
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
		a0.albertacoverage = 1
		AND
		a0.consistentbirth = 1
		AND
		a0.consistentdeceased = 1;

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
			returnlower.birthdate := localrow.greatestbirth;

			-- Upper (longest) bound is the least birth date and greatest deceased date
			returnupper.cornercase := 'U';
			returnupper.uliabphn := localrow.uliabphn;
			returnupper.sex := localrow.sex;
			returnupper.firstnations := localrow.firstnations;
			returnupper.censoreddate := localrow.censoreddate;
			returnupper.surveillancestart := localrow.surveillancestart;
			returnupper.surveillanceend := localrow.surveillanceend;
			returnupper.birthdate := localrow.leastbirth;

			-- Rectified extremum start date of lower (shortest) bound interval
			CASE
				WHEN returnlower.surveillancestart <= returnlower.birthdate THEN
					returnlower.surveillancebirth := 1;
					returnlower.surveillanceimmigrate := 0;
					returnlower.extremumstart := returnlower.birthdate;
				WHEN localrow.surveillanceimmigrate = 1 THEN
					returnlower.surveillancebirth := 0;
					returnlower.surveillanceimmigrate := 1;
					returnlower.extremumstart := localrow.servicestart;
				ELSE
					returnlower.surveillancebirth := 0;
					returnlower.surveillanceimmigrate := 0;
					returnlower.extremumstart := returnlower.surveillancestart;
			END CASE;

			-- Deceased date and dectified extremum end date of lower (shortest) bound interval
			CASE
				WHEN localrow.leastdeceased IS NOT NULL THEN
					returnlower.surveillancedeceased := 1;
					returnlower.surveillanceemigrate := 0;
					returnlower.deceaseddate := localrow.leastdeceased;
					returnlower.extremumend := localrow.leastdeceased;
				WHEN localrow.surveillancedeceased = 1 THEN
					returnlower.surveillancedeceased := 1;
					returnlower.surveillanceemigrate := 0;
					returnlower.deceaseddate := localrow.serviceend;
					returnlower.extremumend := localrow.serviceend;
				WHEN localrow.surveillanceemigrate = 1 THEN
					returnlower.surveillancedeceased := 0;
					returnlower.surveillanceemigrate := 1;
					returnlower.deceaseddate := NULL;
					returnlower.extremumend := localrow.serviceend;
				ELSE
					returnlower.surveillancedeceased := 0;
					returnlower.surveillanceemigrate := 0;
					returnlower.deceaseddate := NULL;
					returnlower.extremumend := returnlower.surveillanceend;
			END CASE;

			-- Rectified extremum start date of upper (longest) bound interval
			CASE
				WHEN returnupper.surveillancestart <= returnupper.birthdate THEN
					returnupper.surveillancebirth := 1;
					returnupper.surveillanceimmigrate := 0;
					returnupper.extremumstart := returnupper.birthdate;
				WHEN localrow.surveillanceimmigrate = 1 THEN
					returnupper.surveillancebirth := 0;
					returnupper.surveillanceimmigrate := 1;
					returnupper.extremumstart := returnupper.surveillancestart;
				ELSE
					returnupper.surveillancebirth := 0;
					returnupper.surveillanceimmigrate := 0;
					returnupper.extremumstart := returnupper.surveillancestart;
			END CASE;

			-- Rectified extremum end date of upper (longest) bound interval
			CASE
				WHEN localrow.greatestdeceased IS NOT NULL THEN
					returnupper.surveillancedeceased := 1;
					returnupper.surveillanceemigrate := 0;
					returnupper.deceaseddate := localrow.greatestdeceased;
					returnupper.extremumend := localrow.greatestdeceased;
				WHEN localrow.surveillancedeceased = 1 THEN
					returnupper.surveillancedeceased := 1;
					returnupper.surveillanceemigrate := 0;
					returnupper.deceaseddate := localrow.surveillanceend;
					returnupper.extremumend := localrow.surveillanceend;
				WHEN localrow.surveillanceemigrate = 1 THEN
					returnupper.surveillancedeceased := 0;
					returnupper.surveillanceemigrate := 1;
					returnupper.deceaseddate := NULL;
					returnupper.extremumend := returnupper.surveillanceend;
				ELSE
					returnupper.surveillancedeceased := 0;
					returnupper.surveillanceemigrate := 0;
					returnupper.deceaseddate := NULL;
					returnupper.extremumend := returnupper.surveillanceend;
			END CASE;

			-- Equipoise of birth dates
			CASE
				WHEN returnupper.birthdate < returnlower.birthdate THEN
					returnlower.birthdateequipoise := 0;
					returnupper.birthdateequipoise := 0;
				ELSE
					returnlower.birthdateequipoise := 1;
					returnupper.birthdateequipoise := 1;
			END CASE;

			-- Equipoise of deceased dates
			CASE
				WHEN returnlower.deceaseddate < returnupper.deceaseddate THEN
					returnlower.deceaseddateequipoise := 0;
					returnupper.deceaseddateequipoise := 0;
				ELSE
					returnlower.deceaseddateequipoise := 1;
					returnupper.deceaseddateequipoise := 1;
			END CASE;

			-- Equipoise of extremum start dates
			CASE
				WHEN returnupper.extremumstart < returnlower.extremumstart THEN
					returnlower.startequipoise := 0;
					returnupper.startequipoise := 0;
				ELSE
					returnlower.startequipoise := 1;
					returnupper.startequipoise := 1;
			END CASE;

			-- Equipoise of extremum end dates
			CASE
				WHEN returnlower.extremumend < returnupper.extremumend THEN
					returnlower.endequipoise := 0;
					returnupper.endequipoise := 0;
				ELSE
					returnlower.endequipoise := 1;
					returnupper.endequipoise := 1;
			END CASE;

			-- Birth observation equipose
			CASE
				WHEN returnlower.surveillancebirth = returnupper.surveillancebirth THEN
					returnlower.birthequipoise := 1;
					returnupper.birthequipoise := 1;
				ELSE
					returnlower.birthequipoise := 0;
					returnupper.birthequipoise := 0;
			END CASE;

			-- Deceased observation equipose
			CASE
				WHEN returnlower.surveillancedeceased = returnupper.surveillancedeceased THEN
					returnlower.deceasedequipoise := 1;
					returnupper.deceasedequipoise := 1;
				ELSE
					returnlower.deceasedequipoise := 0;
					returnupper.deceasedequipoise := 0;
			END CASE;

			-- Immigration observation equipose
			CASE
				WHEN returnlower.surveillanceimmigrate = returnupper.surveillanceimmigrate THEN
					returnlower.immigrateequipoise := 1;
					returnupper.immigrateequipoise := 1;
				ELSE
					returnlower.immigrateequipoise := 0;
					returnupper.immigrateequipoise := 0;
			END CASE;

			-- Emigration observation equipose
			CASE
				WHEN returnlower.surveillanceemigrate = returnupper.surveillanceemigrate THEN
					returnlower.emigrateequipoise := 1;
					returnupper.emigrateequipoise := 1;
				ELSE
					returnlower.emigrateequipoise := 0;
					returnupper.emigrateequipoise := 0;
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