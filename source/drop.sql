-- Reverse dependency order structure releasing
DROP MATERIALIZED VIEW personsurveillance;
DROP MATERIALIZED VIEW surveyambulatorycare;
DROP MATERIALIZED VIEW surveyannualregistry;
DROP MATERIALIZED VIEW surveycontinuingcare;
DROP MATERIALIZED VIEW surveyinpatientcare;
DROP MATERIALIZED VIEW surveylabratorycollection;
DROP MATERIALIZED VIEW surveypharmacydispense;
DROP MATERIALIZED VIEW surveyprimarycare;
DROP MATERIALIZED VIEW surveyvitalstatistics;

-- Base support
DROP PACKAGE maintenanceutilities;
DROP PACKAGE surveillanceutilities;
DROP PACKAGE hazardutilities;