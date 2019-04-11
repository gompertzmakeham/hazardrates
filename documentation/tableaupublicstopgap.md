Instructions for pruning the internal Tableau workbook down to material that can be
aggregated to the age, gender, fiscal year, and corner case for publishing on Tableau
Public.

1. Create a copy of `hazardratesprivate.twb` name `hazardratespublic.twb`
2. Replace data source with live connection
3. Prune dashboards, sheets
4. Replace, add, and remove calculations
5. Prune parameters
6. Hide unused dimensions
7. Create extract `hazardratespublic.hyper`, aggregating to visible dimensions
8. Publish on Tableau public

*To do: flush this out the next time through the whole process.*
*Hopefully after this it is just refreshes until pushing back the aggregates.*