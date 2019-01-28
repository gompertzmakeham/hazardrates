/*
 *  Reverse dependency order releasing.
 */

-- Census support
DROP MATERIALIZED VIEW personcensus;

-- Census structures
DROP MATERIALIZED VIEW censusambulatorycare;
DROP MATERIALIZED VIEW censusinpatientcare;
DROP MATERIALIZED VIEW censuslongtermcare;
DROP MATERIALIZED VIEW censuslaboratorycollection;
DROP MATERIALIZED VIEW censuspharmacydispense;
DROP MATERIALIZED VIEW censusprimarycare;
DROP MATERIALIZED VIEW censussupportiveliving;

-- Surveillance support
DROP MATERIALIZED VIEW personsurveillance;
DROP PACKAGE surveillanceutilities;

-- Surveillance structures
DROP MATERIALIZED VIEW surveyambulatorycare;
DROP MATERIALIZED VIEW surveyannualregistry;
DROP MATERIALIZED VIEW surveycontinuingcare;
DROP MATERIALIZED VIEW surveyinpatientcare;
DROP MATERIALIZED VIEW surveylaboratorycollection;
DROP MATERIALIZED VIEW surveypharmacydispense;
DROP MATERIALIZED VIEW surveyprimarycare;
DROP MATERIALIZED VIEW surveyvitalstatistics;

-- Base support
DROP PACKAGE maintenanceutilities;
DROP PACKAGE hazardutilities;