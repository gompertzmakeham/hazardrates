/*
 *  Reverse dependency order releasing.
 */

-- Census structures
DROP MATERIALIZED VIEW personcensus;
DROP MATERIALIZED VIEW censusambulatorycare;
DROP MATERIALIZED VIEW censusinpatientcare;
DROP MATERIALIZED VIEW censuslongtermcare;
DROP MATERIALIZED VIEW censuslabratorycollection;
DROP MATERIALIZED VIEW censuspharmacydispense;
DROP MATERIALIZED VIEW censusprimarycare;
DROP MATERIALIZED VIEW censussupportiveliving;

-- Surveillance support
DROP PACKAGE surveillanceutilities;

-- Surveillance structures
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
DROP PACKAGE hazardutilities;