Instructions for pruning the internal Tableau workbook down to material that can be
aggregated to the age, gender, fiscal year, and corner case for publishing on Tableau
Public.

1. Create a copy of `hazardratesprivate.twb` named `hazardratespublic.twb`.
2. Delete the `Application` storyboard.
3. Delete the dashboards and worksheets, in the order specified:
    * `Deaths`
    * `Daily Deaths`
    * `Seasonal Births`
    * `Weekly Births`
    * `Un-normalized Seasonal Density of Deaths`
    * `Normalized Seasonal Density of Deaths`
    * `Annually Normalized Seasonal Births`
    * `Annually Normalized Weekly Births`
    * `Validate Interval Ordinals`
    * `Validate Life Event Equipoise`
4. Edit the worksheets and calculations to remove all references to the `[standarderrors]`
parameter:
    * `Average Daily Demographic Pyramid`
    * `Average Daily Demographic Cumulative Contours`
    * `Age Dependent Hazard Rate (Linear)`
    * `Age Dependent Hazard Rate (Logarithmic)`
    * `Settings`
    * `Settings and Filters`
    * `[statistic_hazardratemaximum]`
    * `[statistic_hazardrateminimum]`
    * `[statistic_populationfemalesmaximum]`
    * `[statistic_populationfemalesminimum]`
    * `[statistic_populationmalesmaximum]`
    * `[statistic_populationmalesminimum]`
5. Delete the parameter `[standarderrors]`.
6. Delete the calculations in order of dependency:
    * `[statistic_hazardrateSE]`
    * `[statistic_populationfemalesSE]`
    * `[statistic_populationmalesSE]`
    * `[statistic_hazardratediscount]`
    * `[statistic_populationfemalesdiscount]`
    * `[statistic_populationmalesdiscount]`
    * `[statistic_hazardratedispenalty]`
    * `[statistic_populationfemalespenalty]`
    * `[statistic_populationmalespenalty]`
    * `[statistic_percentSEsmaximum]`
    * `[statistic_percentSEsminimum]`
    * `[statistic_percentmaximumSEs]`
    * `[statistic_percentminimumSEs]`
    * `[statistic_percentlowerSEs]`
    * `[statistic_percentupperSEs]`
    * `[statistic_percentmaximum]`
    * `[statistic_percentminimum]`
    * `[statistic_percentupper]`
    * `[statistic_percentlower]`
    * `[index_orderfirst]`
    * `[index_orderlast]`
    * `[index_deceaseddayfemale]`
    * `[index_deceaseddaymale]`
    * `[index_deceasedyearfemale]`
    * `[index_deceasedyearmale]`
    * `[index_deceasedagefemale]`
    * `[index_deceasedagemale]`
    * `[index_birthyearpercent]`
7. Edit the parameter `[hazardrate]` to remove references to the following values:
    * `[AMBULATORYPERCENTUTILIZATION]`
    * `[AMBULATORYPRIVATEPERCENTUTILIZATION]`
    * `[AMBULATORYWORKPERCENTUTILIZATION]`
    * `[INPATIENTPERCENTUTILIZATION]`
    * `[INPATIENTPRIVATEPERCENTUTILIZATION]`
    * `[INPATIENTWORKPERCENTUTILIZATION]`
    * `[CAREMANAGERPERCENTUTILIZATION]`
    * `[HOMECAREPERCENTUTILIZATION]`
    * `[HOMECAREPROFESSIONALPERCENTUTILIZATION]`
    * `[HOMECARETRANSITIONPERCENTUTILIZATION]`
    * `[LABORATORYPERCENTUTILIZATION]`
    * `[LONGTERMCAREPERCENTUTILIZATION]`
    * `[ALLDOSESPERDISPENSEDPERSON]`
    * `[PHARMACYPERCENTDISPENSED]`
    * `[STANDARDDOSESPERDISPENSEDPERSON]`
    * `[PHARMACYPERCENTDISPENSEDSTANDARD]`
    * `[CONTROLLEDDOSESPERDISPENSEDPERSON]`
    * `[PHARMACYPERCENTDISPENSEDCONTROLLED]`
    * `[PRIMARYCAREPERCENTUTILIZATION]`
    * `[ANESTHESIOLOGYPERCENTUTILIZATION]`
    * `[CONSULTPERCENTUTILIZATION]`
    * `[GENERALPRACTICEPERCENTUTILIZATION]`
    * `[GERIATRICPERCENTUTILIZATION]`
    * `[OBSTETRICPERCENTUTILIZATION]`
    * `[PATHOLOGYPERCENTUTILIZATION]`
    * `[PEDIATRICPERCENTUTILIZATION]`
    * `[PEDIATRICSURGERYPERCENTUTILIZATION]`
    * `[PSYCHIATRYPERCENTUTILIZATION]`
    * `[RADIOLOGYPERCENTUTILIZATION]`
    * `[SPECIALTYPERCENTUTILIZATION]`
    * `[SURGERYPERCENTUTILIZATION]`
    * `[SUPPORTIVELIVINGPERCENTUTILIZATION]`
8. Create fields:
    * `[statistic_hazardratedenominator]`
    * `[statistic_hazardratenumerator]`
9. Edit the calculations in the fields:
    * `[statistic_hazardrate]`
    * `[statistic_hazardratelower]`
    * `[statistic_hazardrateupper]`
    * `[variable_hazardraterecommended]`
10. Create a new live connection to the `hazardrates` table named
`hazardratespublic`, ensuring that the `Initial SQL...` opens a parallel session.

```SQL
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8
```

11. Reset the names and ensure `integer` types are not coerced into `float` types.
12. Edit the aliases in the dimensions:
    * `[CORNERCASE]`
        - `Longest Lifespan`
        - `Shortest Lifespan`
    * `[SEX]`
        - `Female`
        - `Male` 
13. Replace the `hazardratesprivate` data source with the `hazardratespublic` data source,
and close `hazardratesprivate`.
14. Change the following measures to dimensions:
    * `[AGECOINCIDECENSUS]`
    * `[AGECOINCIDEINTERVAL]`
    * `[AGEEQUIPOISE]`
    * `[BIRTHDATEEQUIPOISE]`
    * `[BIRTHOBSERVATIONEQUIPOISE]`
    * `[DECEASEDDATEEQUIPOISE]`
    * `[DECEASEDOBSERVATIONEQUIPOISE]`
    * `[EMIGRATEDATEEQUIPOISE]`
    * `[EMIGRATEOBSERVATIONEQUIPOISE]`
    * `[FIRSTNATIONS]`
    * `[IMMIGRATEDATEEQUIPOISE]`
    * `[IMMIGRATEOBSERVATIONEQUIPOISE]`
    * `[INTERVALAGE]`
    * `[INTERVALCOUNT]`
    * `[INTERVALFIRST]`
    * `[INTERVALLAST]`
    * `[INTERVALORDER]`
    * `[SURVEILLANCEENDEQUIPOISE]`
    * `[SURVEILLANCESTARTEQUIPOISE]`
    * `[ULIABPHN]`
15. Convert the following dimensions to continuous:
    * `[AGEEND]`
    * `[AGESTART]`
    * `[BIRTHDATE]`
    * `[CENSOREDDATE]`
    * `[CENSUSEND]`
    * `[CENSUSSTART]`
    * `[DECEASEDDATE]`
    * `[DURATIONEND]`
    * `[DURATIONSTART]`
    * `[EMIGRATEDATE]`
    * `[EXTREMUMEND]`
    * `[EXTREMUMSTART]`
    * `[IMMIGRATEDATE]`
    * `[INTERVALAGE]`
    * `[INTERVALCOUNT]`
    * `[INTERVALEND]`
    * `[INTERVALORDER]`
    * `[INTERVALSTART]`
    * `[SURVEILLANCEEND]`
    * `[SURVEILLANCESTART]`
16. Ensure the following fields remain measures and have a default aggregation of sum:
    * `[INTERVALBIRTH]`
    * `[INTERVALDECEASED]`
    * `[INTERVALEMIGRATE]`
    * `[INTERVALIMMIGRATE]`
17. Edit the aliases of the `[Measure Names]` in the `Sample Sizes` worksheet:
    * `Community Laboratory Assays`
    * `Community Pharmacy Dispensed`
    * `Primary Care Procedures`
    * `Census Intervals`
    * `Emergency Ambulatory Care Hours`
    * `Person Years`
    * `Long Term Care Days`
    * `Care Managed Weeks`
    * `Emergency Inpatient Care Days`
    * `Designated Supportive Living Days`
    * `Home Care Activities`
    * `Provincial Immigrants`
    * `Births`
    * `Provincial Emigrants`
    * `Deaths`
18. Hide unused dimensions.
19. Double check that all sensitive fields are suppressed so that the aggregates are
provincial; not including the calculated dimensions, only the following dimensions should be
shown:
    * `[CENSUSSTART]`
    * `[CENSUSEND]`
    * `[CORNERCASE]`
    * `[INTERVALAGE]`
    * `[SEX]`
    * `[CENSOREDDATE]`
20. Create extract `hazardratespublic.hyper`, aggregating to the visible dimensions.
21. Triple check that all sensitive fields are suppressed so that the aggregates are
provincial.
23. Hide all the worksheets.
24. Publish on Tableau public.

*Hopefully after this it is just refreshes until pushing back the aggregates.*