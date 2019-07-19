CREATE MATERIALIZED VIEW personutilization NOLOGGING COMPRESS NOCACHE PARALLEL 8 BUILD DEFERRED REFRESH COMPLETE ON DEMAND AS
WITH

	-- Link ambulatory to inpatient
	ambulatoryinpatient AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.visitminutes, 0) ambulatoryminutes,
			COALESCE(a0.visitcount, 0) ambulatoryvisits,
			COALESCE(a0.visitsitedays, 0) ambulatorysitedays,
			COALESCE(a0.visitdays, 0) ambulatorydays,
			COALESCE(a0.privatevisitminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.privatevisitcount, 0) ambulatoryprivatevisits,
			COALESCE(a0.privatevisitsitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.privatevisitdays, 0) ambulatoryprivatedays,
			COALESCE(a0.workvisitminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.workvisitcount, 0) ambulatoryworkvisits,
			COALESCE(a0.workvisitsitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.workvisitdays, 0) ambulatoryworkdays,
			COALESCE(a1.staydays, 0) inpatientdays,
			COALESCE(a1.admissioncount, 0) inpatientadmissions,
			COALESCE(a1.dischargecount, 0) inpatientdischarges,
			COALESCE(a1.intersectingstays, 0) inpatientstays,
			COALESCE(a1.privatestaydays, 0) inpatientprivatedays,
			COALESCE(a1.privateadmissioncount, 0) inpatientprivateadmissions,
			COALESCE(a1.privatedischargecount, 0) inpatientprivatedischarges,
			COALESCE(a1.privateintersectingstays, 0) inpatientprivatestays,
			COALESCE(a1.workstaydays, 0) inpatientworkdays,
			COALESCE(a1.workadmissioncount, 0) inpatientworkadmissions,
			COALESCE(a1.workdischargecount, 0) inpatientworkdischarges,
			COALESCE(a1.workintersectingstays, 0) inpatientworkstays
		FROM
			censusambulatorycare a0
			FULL JOIN
			censusinpatientcare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in care management
	addcaremanager AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a1.manageddays, 0) caremanagerdays,
			COALESCE(a1.allocationcount, 0) caremanagerallocations,
			COALESCE(a1.releasecount, 0) caremanagerreleases,
			COALESCE(a1.intersectingmanagement, 0) caremanagers
		FROM
			ambulatoryinpatient a0
			FULL JOIN
			censuscaremanagement a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in home care activity
	addhomecare AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a1.professionalactivities, 0) homecareprofessionalservices,
			COALESCE(a1.transitionactivities, 0) homecaretransitionservices,
			COALESCE(a1.allactivities, 0) homecareservices,
			COALESCE(a1.professionalproviderdays, 0) homecareprofessionalvisits,
			COALESCE(a1.transitionproviderdays, 0) homecaretransitionvisits,
			COALESCE(a1.allproviderdays, 0) homecarevisits,
			COALESCE(a1.professionaldays, 0) homecareprofessionaldays,
			COALESCE(a1.transitiondays, 0) homecaretransitiondays,
			COALESCE(a1.alldays, 0) homecaredays
		FROM
			addcaremanager a0
			FULL JOIN
			censushomecare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in laboratory
	addlaboratory AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a0.homecareprofessionalservices, 0) homecareprofessionalservices,
			COALESCE(a0.homecaretransitionservices, 0) homecaretransitionservices,
			COALESCE(a0.homecareservices, 0) homecareservices,
			COALESCE(a0.homecareprofessionalvisits, 0) homecareprofessionalvisits,
			COALESCE(a0.homecaretransitionvisits, 0) homecaretransitionvisits,
			COALESCE(a0.homecarevisits, 0) homecarevisits,
			COALESCE(a0.homecareprofessionaldays, 0) homecareprofessionaldays,
			COALESCE(a0.homecaretransitiondays, 0) homecaretransitiondays,
			COALESCE(a0.homecaredays, 0) homecaredays,
			COALESCE(a1.assaycount, 0) laboratoryassays,
			COALESCE(a1.collectsitedays, 0) laboratorysitedays,
			COALESCE(a1.collectdays, 0) laboratorydays
		FROM
			addhomecare a0
			FULL JOIN
			censuslaboratorycollection a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in long term care
	addlongtermcare AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a0.homecareprofessionalservices, 0) homecareprofessionalservices,
			COALESCE(a0.homecaretransitionservices, 0) homecaretransitionservices,
			COALESCE(a0.homecareservices, 0) homecareservices,
			COALESCE(a0.homecareprofessionalvisits, 0) homecareprofessionalvisits,
			COALESCE(a0.homecaretransitionvisits, 0) homecaretransitionvisits,
			COALESCE(a0.homecarevisits, 0) homecarevisits,
			COALESCE(a0.homecareprofessionaldays, 0) homecareprofessionaldays,
			COALESCE(a0.homecaretransitiondays, 0) homecaretransitiondays,
			COALESCE(a0.homecaredays, 0) homecaredays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a1.staydays, 0) longtermcaredays,
			COALESCE(a1.admissioncount, 0) longtermcareadmissions,
			COALESCE(a1.dischargecount, 0) longtermcaredischarges,
			COALESCE(a1.intersectingstays, 0) longtermcarestays
		FROM
			addlaboratory a0
			FULL JOIN
			censuslongtermcare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in mother-newborn
	addmothernewborn AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a1.livenewborns, 0) livenewborns,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a0.homecareprofessionalservices, 0) homecareprofessionalservices,
			COALESCE(a0.homecaretransitionservices, 0) homecaretransitionservices,
			COALESCE(a0.homecareservices, 0) homecareservices,
			COALESCE(a0.homecareprofessionalvisits, 0) homecareprofessionalvisits,
			COALESCE(a0.homecaretransitionvisits, 0) homecaretransitionvisits,
			COALESCE(a0.homecarevisits, 0) homecarevisits,
			COALESCE(a0.homecareprofessionaldays, 0) homecareprofessionaldays,
			COALESCE(a0.homecaretransitiondays, 0) homecaretransitiondays,
			COALESCE(a0.homecaredays, 0) homecaredays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcaredays, 0) longtermcaredays,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays
		FROM
			addlongtermcare a0
			FULL JOIN
			censusmothernewborn a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in pharmacy
	addpharmacy AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.livenewborns, 0) livenewborns,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a0.homecareprofessionalservices, 0) homecareprofessionalservices,
			COALESCE(a0.homecaretransitionservices, 0) homecaretransitionservices,
			COALESCE(a0.homecareservices, 0) homecareservices,
			COALESCE(a0.homecareprofessionalvisits, 0) homecareprofessionalvisits,
			COALESCE(a0.homecaretransitionvisits, 0) homecaretransitionvisits,
			COALESCE(a0.homecarevisits, 0) homecarevisits,
			COALESCE(a0.homecareprofessionaldays, 0) homecareprofessionaldays,
			COALESCE(a0.homecaretransitiondays, 0) homecaretransitiondays,
			COALESCE(a0.homecaredays, 0) homecaredays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcaredays, 0) longtermcaredays,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
			COALESCE(a1.standarddailydoses, 0) pharmacystandarddailydoses,
			COALESCE(a1.controlleddailydoses, 0) pharmacycontrolleddailydoses,
			COALESCE(a1.alldailydoses, 0) pharmacydailydoses,
			COALESCE(a1.standardtherapeutics, 0) pharmacystandardtherapeutics,
			COALESCE(a1.controlledtherapeutics, 0) pharmacycontrolledtherapeutics,
			COALESCE(a1.alltherapeutics, 0) pharmacytherapeutics,
			COALESCE(a1.standardsitedays, 0) pharmacystandardsitedays,
			COALESCE(a1.controlledsitedays, 0) pharmacycontrolledsitedays,
			COALESCE(a1.allsitedays, 0) pharmacysitedays,
			COALESCE(a1.standarddays, 0) pharmacystandarddays,
			COALESCE(a1.controlleddays, 0) pharmacycontrolleddays,
			COALESCE(a1.alldays, 0) pharmacydays
		FROM
			addmothernewborn a0
			FULL JOIN
			censuspharmacydispense a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	),

	-- Mix in primary care
	addprimarycare AS
	(
		SELECT
			COALESCE(a0.uliabphn, a1.uliabphn) uliabphn,
			COALESCE(a0.cornercase, a1.cornercase) cornercase,
			COALESCE(a0.intervalstart, a1.intervalstart) intervalstart,
			COALESCE(a0.intervalend, a1.intervalend) intervalend,
			COALESCE(a0.livenewborns, 0) livenewborns,
			COALESCE(a0.ambulatoryminutes, 0) ambulatoryminutes,
			COALESCE(a0.ambulatoryvisits, 0) ambulatoryvisits,
			COALESCE(a0.ambulatorysitedays, 0) ambulatorysitedays,
			COALESCE(a0.ambulatorydays, 0) ambulatorydays,
			COALESCE(a0.ambulatoryprivateminutes, 0) ambulatoryprivateminutes,
			COALESCE(a0.ambulatoryprivatevisits, 0) ambulatoryprivatevisits,
			COALESCE(a0.ambulatoryprivatesitedays, 0) ambulatoryprivatesitedays,
			COALESCE(a0.ambulatoryprivatedays, 0) ambulatoryprivatedays,
			COALESCE(a0.ambulatoryworkminutes, 0) ambulatoryworkminutes,
			COALESCE(a0.ambulatoryworkvisits, 0) ambulatoryworkvisits,
			COALESCE(a0.ambulatoryworksitedays, 0) ambulatoryworksitedays,
			COALESCE(a0.ambulatoryworkdays, 0) ambulatoryworkdays,
			COALESCE(a0.inpatientdays, 0) inpatientdays,
			COALESCE(a0.inpatientadmissions, 0) inpatientadmissions,
			COALESCE(a0.inpatientdischarges, 0) inpatientdischarges,
			COALESCE(a0.inpatientstays, 0) inpatientstays,
			COALESCE(a0.inpatientprivatedays, 0) inpatientprivatedays,
			COALESCE(a0.inpatientprivateadmissions, 0) inpatientprivateadmissions,
			COALESCE(a0.inpatientprivatedischarges, 0) inpatientprivatedischarges,
			COALESCE(a0.inpatientprivatestays, 0) inpatientprivatestays,
			COALESCE(a0.inpatientworkdays, 0) inpatientworkdays,
			COALESCE(a0.inpatientworkadmissions, 0) inpatientworkadmissions,
			COALESCE(a0.inpatientworkdischarges, 0) inpatientworkdischarges,
			COALESCE(a0.inpatientworkstays, 0) inpatientworkstays,
			COALESCE(a0.caremanagerdays, 0) caremanagerdays,
			COALESCE(a0.caremanagerallocations, 0) caremanagerallocations,
			COALESCE(a0.caremanagerreleases, 0) caremanagerreleases,
			COALESCE(a0.caremanagers, 0) caremanagers,
			COALESCE(a0.homecareprofessionalservices, 0) homecareprofessionalservices,
			COALESCE(a0.homecaretransitionservices, 0) homecaretransitionservices,
			COALESCE(a0.homecareservices, 0) homecareservices,
			COALESCE(a0.homecareprofessionalvisits, 0) homecareprofessionalvisits,
			COALESCE(a0.homecaretransitionvisits, 0) homecaretransitionvisits,
			COALESCE(a0.homecarevisits, 0) homecarevisits,
			COALESCE(a0.homecareprofessionaldays, 0) homecareprofessionaldays,
			COALESCE(a0.homecaretransitiondays, 0) homecaretransitiondays,
			COALESCE(a0.homecaredays, 0) homecaredays,
			COALESCE(a0.laboratoryassays, 0) laboratoryassays,
			COALESCE(a0.laboratorysitedays, 0) laboratorysitedays,
			COALESCE(a0.laboratorydays, 0) laboratorydays,
			COALESCE(a0.longtermcaredays, 0) longtermcaredays,
			COALESCE(a0.longtermcareadmissions, 0) longtermcareadmissions,
			COALESCE(a0.longtermcaredischarges, 0) longtermcaredischarges,
			COALESCE(a0.longtermcarestays, 0) longtermcarestays,
			COALESCE(a0.pharmacystandarddailydoses, 0) pharmacystandarddailydoses,
			COALESCE(a0.pharmacycontrolleddailydoses, 0) pharmacycontrolleddailydoses,
			COALESCE(a0.pharmacydailydoses, 0) pharmacydailydoses,
			COALESCE(a0.pharmacystandardtherapeutics, 0) pharmacystandardtherapeutics,
			COALESCE(a0.pharmacycontrolledtherapeutics, 0) pharmacycontrolledtherapeutics,
			COALESCE(a0.pharmacytherapeutics, 0) pharmacytherapeutics,
			COALESCE(a0.pharmacystandardsitedays, 0) pharmacystandardsitedays,
			COALESCE(a0.pharmacycontrolledsitedays, 0) pharmacycontrolledsitedays,
			COALESCE(a0.pharmacysitedays, 0) pharmacysitedays,
			COALESCE(a0.pharmacystandarddays, 0) pharmacystandarddays,
			COALESCE(a0.pharmacycontrolleddays, 0) pharmacycontrolleddays,
			COALESCE(a0.pharmacydays, 0) pharmacydays,
			COALESCE(a1.anesthesiologyprocedures, 0) anesthesiologyprocedures,
			COALESCE(a1.consultprocedures, 0) consultprocedures,
			COALESCE(a1.generalpracticeprocedures, 0) generalpracticeprocedures,
			COALESCE(a1.geriatricprocedures, 0) geriatricprocedures,
			COALESCE(a1.obstetricprocedures, 0) obstetricprocedures,
			COALESCE(a1.pathologyprocedures, 0) pathologyprocedures,
			COALESCE(a1.pediatricprocedures, 0) pediatricprocedures,
			COALESCE(a1.pediatricsurgicalprocedures, 0) pediatricsurgicalprocedures,
			COALESCE(a1.psychiatryprocedures, 0) psychiatryprocedures,
			COALESCE(a1.radiologyprocedures, 0) radiologyprocedures,
			COALESCE(a1.specialtyprocedures, 0) specialtyprocedures,
			COALESCE(a1.surgicalprocedures, 0) surgicalprocedures,
			COALESCE(a1.allprocedures, 0) primarycareprocedures,
			COALESCE(a1.anesthesiologistsdays, 0) anesthesiologistsdays,
			COALESCE(a1.consultprovidersdays, 0) consultprovidersdays,
			COALESCE(a1.generalpractitionersdays, 0) generalpractitionersdays,
			COALESCE(a1.geriatriciansdays, 0) geriatriciansdays,
			COALESCE(a1.obstetriciansdays, 0) obstetriciansdays,
			COALESCE(a1.pathologistsdays, 0) pathologistsdays,
			COALESCE(a1.pediatriciansdays, 0) pediatriciansdays,
			COALESCE(a1.pediatricsurgeonsdays, 0) pediatricsurgeonsdays,
			COALESCE(a1.psychiatristsdays, 0) psychiatristsdays,
			COALESCE(a1.radiologistsdays, 0) radiologistsdays,
			COALESCE(a1.specialistsdays, 0) specialistsdays,
			COALESCE(a1.surgeonsdays, 0) surgeonsdays,
			COALESCE(a1.allproviderdays, 0) primarycareproviderdays,
			COALESCE(a1.anesthesiologydays, 0) anesthesiologydays,
			COALESCE(a1.consultdays, 0) consultdays,
			COALESCE(a1.generalpracticedays, 0) generalpracticedays,
			COALESCE(a1.geriatricdays, 0) geriatricdays,
			COALESCE(a1.obstetricdays, 0) obstetricdays,
			COALESCE(a1.pathologydays, 0) pathologydays,
			COALESCE(a1.pediatricdays, 0) pediatricdays,
			COALESCE(a1.pediatricsurgerydays, 0) pediatricsurgerydays,
			COALESCE(a1.psychiatrydays, 0) psychiatrydays,
			COALESCE(a1.radiologydays, 0) radiologydays,
			COALESCE(a1.specialtydays, 0) specialtydays,
			COALESCE(a1.surgerydays, 0) surgerydays,
			COALESCE(a1.alldays, 0) primarycaredays
		FROM
			addpharmacy a0
			FULL JOIN
			censusprimarycare a1
			ON
				a0.uliabphn = a1.uliabphn
				AND
				a0.cornercase = a1.cornercase
				AND
				a0.intervalstart = a1.intervalstart
				AND
				a0.intervalend = a1.intervalend
	)

-- Final mix supportive living
SELECT
	CAST(COALESCE(a0.uliabphn, a1.uliabphn) AS INTEGER) uliabphn,
	CAST(COALESCE(a0.cornercase, a1.cornercase) AS VARCHAR2(1)) cornercase,
	CAST(COALESCE(a0.intervalstart, a1.intervalstart) AS DATE) intervalstart,
	CAST(COALESCE(a0.intervalend, a1.intervalend) AS DATE) intervalend,
	CAST(COALESCE(a0.livenewborns, 0) AS INTEGER) livenewborns,
	CAST(COALESCE(a0.ambulatoryminutes, 0) AS INTEGER) ambulatoryminutes,
	CAST(COALESCE(a0.ambulatoryvisits, 0) AS INTEGER) ambulatoryvisits,
	CAST(COALESCE(a0.ambulatorysitedays, 0) AS INTEGER) ambulatorysitedays,
	CAST(COALESCE(a0.ambulatorydays, 0) AS INTEGER) ambulatorydays,
	CAST(COALESCE(a0.ambulatoryprivateminutes, 0) AS INTEGER) ambulatoryprivateminutes,
	CAST(COALESCE(a0.ambulatoryprivatevisits, 0) AS INTEGER) ambulatoryprivatevisits,
	CAST(COALESCE(a0.ambulatoryprivatesitedays, 0) AS INTEGER) ambulatoryprivatesitedays,
	CAST(COALESCE(a0.ambulatoryprivatedays, 0) AS INTEGER) ambulatoryprivatedays,
	CAST(COALESCE(a0.ambulatoryworkminutes, 0) AS INTEGER) ambulatoryworkminutes,
	CAST(COALESCE(a0.ambulatoryworkvisits, 0) AS INTEGER) ambulatoryworkvisits,
	CAST(COALESCE(a0.ambulatoryworksitedays, 0) AS INTEGER) ambulatoryworksitedays,
	CAST(COALESCE(a0.ambulatoryworkdays, 0) AS INTEGER) ambulatoryworkdays,
	CAST(COALESCE(a0.inpatientdays, 0) AS INTEGER) inpatientdays,
	CAST(COALESCE(a0.inpatientadmissions, 0) AS INTEGER) inpatientadmissions,
	CAST(COALESCE(a0.inpatientdischarges, 0) AS INTEGER) inpatientdischarges,
	CAST(COALESCE(a0.inpatientstays, 0) AS INTEGER) inpatientstays,
	CAST(COALESCE(a0.inpatientprivatedays, 0) AS INTEGER) inpatientprivatedays,
	CAST(COALESCE(a0.inpatientprivateadmissions, 0) AS INTEGER) inpatientprivateadmissions,
	CAST(COALESCE(a0.inpatientprivatedischarges, 0) AS INTEGER) inpatientprivatedischarges,
	CAST(COALESCE(a0.inpatientprivatestays, 0) AS INTEGER) inpatientprivatestays,
	CAST(COALESCE(a0.inpatientworkdays, 0) AS INTEGER) inpatientworkdays,
	CAST(COALESCE(a0.inpatientworkadmissions, 0) AS INTEGER) inpatientworkadmissions,
	CAST(COALESCE(a0.inpatientworkdischarges, 0) AS INTEGER) inpatientworkdischarges,
	CAST(COALESCE(a0.inpatientworkstays, 0) AS INTEGER) inpatientworkstays,
	CAST(COALESCE(a0.caremanagerdays, 0) AS INTEGER) caremanagerdays,
	CAST(COALESCE(a0.caremanagerallocations, 0) AS INTEGER) caremanagerallocations,
	CAST(COALESCE(a0.caremanagerreleases, 0) AS INTEGER) caremanagerreleases,
	CAST(COALESCE(a0.caremanagers, 0) AS INTEGER) caremanagers,
	CAST(COALESCE(a0.homecareprofessionalservices, 0) AS INTEGER) homecareprofessionalservices,
	CAST(COALESCE(a0.homecaretransitionservices, 0) AS INTEGER) homecaretransitionservices,
	CAST(COALESCE(a0.homecareservices, 0) AS INTEGER) homecareservices,
	CAST(COALESCE(a0.homecareprofessionalvisits, 0) AS INTEGER) homecareprofessionalvisits,
	CAST(COALESCE(a0.homecaretransitionvisits, 0) AS INTEGER) homecaretransitionvisits,
	CAST(COALESCE(a0.homecarevisits, 0) AS INTEGER) homecarevisits,
	CAST(COALESCE(a0.homecareprofessionaldays, 0) AS INTEGER) homecareprofessionaldays,
	CAST(COALESCE(a0.homecaretransitiondays, 0) AS INTEGER) homecaretransitiondays,
	CAST(COALESCE(a0.homecaredays, 0) AS INTEGER) homecaredays,
	CAST(COALESCE(a0.laboratoryassays, 0) AS INTEGER) laboratoryassays,
	CAST(COALESCE(a0.laboratorysitedays, 0) AS INTEGER) laboratorysitedays,
	CAST(COALESCE(a0.laboratorydays, 0) AS INTEGER) laboratorydays,
	CAST(COALESCE(a0.longtermcaredays, 0) AS INTEGER) longtermcaredays,
	CAST(COALESCE(a0.longtermcareadmissions, 0) AS INTEGER) longtermcareadmissions,
	CAST(COALESCE(a0.longtermcaredischarges, 0) AS INTEGER) longtermcaredischarges,
	CAST(COALESCE(a0.longtermcarestays, 0) AS INTEGER) longtermcarestays,
	CAST(COALESCE(a0.pharmacystandarddailydoses, 0) AS INTEGER) pharmacystandarddailydoses,
	CAST(COALESCE(a0.pharmacycontrolleddailydoses, 0) AS INTEGER) pharmacycontrolleddailydoses,
	CAST(COALESCE(a0.pharmacydailydoses, 0) AS INTEGER) pharmacydailydoses,
	CAST(COALESCE(a0.pharmacystandardtherapeutics, 0) AS INTEGER) pharmacystandardtherapeutics,
	CAST(COALESCE(a0.pharmacycontrolledtherapeutics, 0) AS INTEGER) pharmacycontrolledtherapeutics,
	CAST(COALESCE(a0.pharmacytherapeutics, 0) AS INTEGER) pharmacytherapeutics,
	CAST(COALESCE(a0.pharmacystandardsitedays, 0) AS INTEGER) pharmacystandardsitedays,
	CAST(COALESCE(a0.pharmacycontrolledsitedays, 0) AS INTEGER) pharmacycontrolledsitedays,
	CAST(COALESCE(a0.pharmacysitedays, 0) AS INTEGER) pharmacysitedays,
	CAST(COALESCE(a0.pharmacystandarddays, 0) AS INTEGER) pharmacystandarddays,
	CAST(COALESCE(a0.pharmacycontrolleddays, 0) AS INTEGER) pharmacycontrolleddays,
	CAST(COALESCE(a0.pharmacydays, 0) AS INTEGER) pharmacydays,
	CAST(COALESCE(a0.anesthesiologyprocedures, 0) AS INTEGER) anesthesiologyprocedures,
	CAST(COALESCE(a0.consultprocedures, 0) AS INTEGER) consultprocedures,
	CAST(COALESCE(a0.generalpracticeprocedures, 0) AS INTEGER) generalpracticeprocedures,
	CAST(COALESCE(a0.geriatricprocedures, 0) AS INTEGER) geriatricprocedures,
	CAST(COALESCE(a0.obstetricprocedures, 0) AS INTEGER) obstetricprocedures,
	CAST(COALESCE(a0.pathologyprocedures, 0) AS INTEGER) pathologyprocedures,
	CAST(COALESCE(a0.pediatricprocedures, 0) AS INTEGER) pediatricprocedures,
	CAST(COALESCE(a0.pediatricsurgicalprocedures, 0) AS INTEGER) pediatricsurgicalprocedures,
	CAST(COALESCE(a0.psychiatryprocedures, 0) AS INTEGER) psychiatryprocedures,
	CAST(COALESCE(a0.radiologyprocedures, 0) AS INTEGER) radiologyprocedures,
	CAST(COALESCE(a0.specialtyprocedures, 0) AS INTEGER) specialtyprocedures,
	CAST(COALESCE(a0.surgicalprocedures, 0) AS INTEGER) surgicalprocedures,
	CAST(COALESCE(a0.primarycareprocedures, 0) AS INTEGER) primarycareprocedures,
	CAST(COALESCE(a0.anesthesiologistsdays, 0) AS INTEGER) anesthesiologistsdays,
	CAST(COALESCE(a0.consultprovidersdays, 0) AS INTEGER) consultprovidersdays,
	CAST(COALESCE(a0.generalpractitionersdays, 0) AS INTEGER) generalpractitionersdays,
	CAST(COALESCE(a0.geriatriciansdays, 0) AS INTEGER) geriatriciansdays,
	CAST(COALESCE(a0.obstetriciansdays, 0) AS INTEGER) obstetriciansdays,
	CAST(COALESCE(a0.pathologistsdays, 0) AS INTEGER) pathologistsdays,
	CAST(COALESCE(a0.pediatriciansdays, 0) AS INTEGER) pediatriciansdays,
	CAST(COALESCE(a0.pediatricsurgeonsdays, 0) AS INTEGER) pediatricsurgeonsdays,
	CAST(COALESCE(a0.psychiatristsdays, 0) AS INTEGER) psychiatristsdays,
	CAST(COALESCE(a0.radiologistsdays, 0) AS INTEGER) radiologistsdays,
	CAST(COALESCE(a0.specialistsdays, 0) AS INTEGER) specialistsdays,
	CAST(COALESCE(a0.surgeonsdays, 0) AS INTEGER) surgeonsdays,
	CAST(COALESCE(a0.primarycareproviderdays, 0) AS INTEGER) primarycareproviderdays,
	CAST(COALESCE(a0.anesthesiologydays, 0) AS INTEGER) anesthesiologydays,
	CAST(COALESCE(a0.consultdays, 0) AS INTEGER) consultdays,
	CAST(COALESCE(a0.generalpracticedays, 0) AS INTEGER) generalpracticedays,
	CAST(COALESCE(a0.geriatricdays, 0) AS INTEGER) geriatricdays,
	CAST(COALESCE(a0.obstetricdays, 0) AS INTEGER) obstetricdays,
	CAST(COALESCE(a0.pathologydays, 0) AS INTEGER) pathologydays,
	CAST(COALESCE(a0.pediatricdays, 0) AS INTEGER) pediatricdays,
	CAST(COALESCE(a0.pediatricsurgerydays, 0) AS INTEGER) pediatricsurgerydays,
	CAST(COALESCE(a0.psychiatrydays, 0) AS INTEGER) psychiatrydays,
	CAST(COALESCE(a0.radiologydays, 0) AS INTEGER) radiologydays,
	CAST(COALESCE(a0.specialtydays, 0) AS INTEGER) specialtydays,
	CAST(COALESCE(a0.surgerydays, 0) AS INTEGER) surgerydays,
	CAST(COALESCE(a0.primarycaredays, 0) AS INTEGER) primarycaredays,
	CAST(COALESCE(a1.designateddays, 0) AS INTEGER) designateddays,
	CAST(COALESCE(a1.designatedadmissions, 0) AS INTEGER) designatedadmissions,
	CAST(COALESCE(a1.designateddischarges, 0) AS INTEGER) designateddischarges,
	CAST(COALESCE(a1.designatedstays, 0) AS INTEGER) designatedstays,
	CAST(COALESCE(a1.nondesignateddays, 0) AS INTEGER) nondesignateddays,
	CAST(COALESCE(a1.nondesignatedadmissions, 0) AS INTEGER) nondesignatedadmissions,
	CAST(COALESCE(a1.nondesignateddischarges, 0) AS INTEGER) nondesignateddischarges,
	CAST(COALESCE(a1.nondesignatedstays, 0) AS INTEGER) nondesignatedstays,
	CAST(COALESCE(a1.staydays, 0) AS INTEGER) supportivelivingdays,
	CAST(COALESCE(a1.admissioncount, 0) AS INTEGER) supportivelivingadmissions,
	CAST(COALESCE(a1.dischargecount, 0) AS INTEGER) supportivelivingdischarges,
	CAST(COALESCE(a1.intersectingstays, 0) AS INTEGER) supportivelivingstays
FROM
	addprimarycare a0
	FULL JOIN
	censussupportiveliving a1
	ON
		a0.uliabphn = a1.uliabphn
		AND
		a0.cornercase = a1.cornercase
		AND
		a0.intervalstart = a1.intervalstart
		AND
		a0.intervalend = a1.intervalend;

ALTER TABLE personutilization ADD CONSTRAINT primaryutilization PRIMARY KEY (uliabphn, cornercase, intervalstart, intervalend);

COMMENT ON MATERIALIZED VIEW personutilization IS 'For every person that at any time was covered by Alberta Healthcare Insurance partition the surviellance interval by the intersections of fiscal years and age years, rectified by the start and end of the surveillance interval.';
COMMENT ON COLUMN personutilization.uliabphn IS 'Unique lifetime identifier of the person, Alberta provincial healthcare number.';
COMMENT ON COLUMN personutilization.cornercase IS 'Extremum of the observations of the birth and death dates: L greatest birth date and least deceased date, U least birth date and greatest deceased date.';
COMMENT ON COLUMN personutilization.intervalstart IS 'Closed start of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personutilization.intervalend IS 'Closed end of the intersection of the fiscal year and the age interval.';
COMMENT ON COLUMN personutilization.livenewborns IS 'Naive count of live newborns delivered by the mother in the census interval, minimal plausibility checks.';
COMMENT ON COLUMN personutilization.ambulatoryminutes IS 'Naive sum of emergency ambulatory care minutes that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN personutilization.ambulatoryvisits IS 'Emergency ambulatory care visits in the census interval.';
COMMENT ON COLUMN personutilization.ambulatorysitedays IS 'Unique combinations of days and ambulatory care sites visited for an emergency in the census interval.';
COMMENT ON COLUMN personutilization.ambulatorydays IS 'Unique days of ambulatory care visits for an emergency in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryprivateminutes IS 'Naive sum of emergency ambulatory care minutes, for private casualties, that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN personutilization.ambulatoryprivatevisits IS 'Emergency ambulatory care visits, for private casualties, in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryprivatesitedays IS 'Unique combinations of days and ambulatory care sites visited for a private casualty emergency in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryprivatedays IS 'Unique days of ambulatory care visits for a private casualty emergency in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryworkminutes IS 'Naive sum of emergency ambulatory care minutes, for workplace casualties, that intersected with the census interval, including overlapping visits.';
COMMENT ON COLUMN personutilization.ambulatoryworkvisits IS 'Emergency ambulatory care visits, for workplace casualties, in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryworksitedays IS 'Unique combinations of days and ambulatory care sites visited for a workplace casualty emergency in the census interval.';
COMMENT ON COLUMN personutilization.ambulatoryworkdays IS 'Unique days of ambulatory care visits for a workplace casualty emergency in the census interval.';
COMMENT ON COLUMN personutilization.inpatientdays IS 'Naive sum of emergency inpatient care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.inpatientadmissions IS 'Emergency inpatient care admissions in the census interval.';
COMMENT ON COLUMN personutilization.inpatientdischarges IS 'Emergency inpatient care discharges in the census interval.';
COMMENT ON COLUMN personutilization.inpatientstays IS 'Emergency inpatient care stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.inpatientprivatedays IS 'Naive sum of emergency inpatient care days, for private casualties, that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.inpatientprivateadmissions IS 'Emergency inpatient care admissions, for private casualties, in the census interval.';
COMMENT ON COLUMN personutilization.inpatientprivatedischarges IS 'Emergency inpatient care discharges, for private casualties, in the census interval.';
COMMENT ON COLUMN personutilization.inpatientprivatestays IS 'Emergency inpatient care stays, for private casualties, intersecting with the census interval.';
COMMENT ON COLUMN personutilization.inpatientworkdays IS 'Naive sum of emergency inpatient care days, for workplace casualties, that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.inpatientworkadmissions IS 'Emergency inpatient care admissions, for workplace casualties, in the census interval.';
COMMENT ON COLUMN personutilization.inpatientworkdischarges IS 'Emergency inpatient care discharges, for workplace casualties, in the census interval.';
COMMENT ON COLUMN personutilization.inpatientworkstays IS 'Emergency inpatient care stays, for workplace casualties, intersecting with the census interval.';
COMMENT ON COLUMN personutilization.caremanagerdays IS 'Naive sum of days of professionals allocated to provide care, case, transition, or placement managment or coordination, that intersected with the census interval, including overlapping allocations.';
COMMENT ON COLUMN personutilization.caremanagerallocations IS 'Allocations of professionals to provide care, case, transition, or placement managment or coordination.';
COMMENT ON COLUMN personutilization.caremanagerreleases IS 'Release of professionals from providing care, case, transition, or placement managment or coordination.';
COMMENT ON COLUMN personutilization.caremanagers IS 'Allocations of professionals providing care, case, transition, or placement managment or coordination that intersected with the census interval.';
COMMENT ON COLUMN personutilization.homecareprofessionalservices IS 'Number of of home care activities provided by a registered, regulated, or licensed professional in the census interval.';
COMMENT ON COLUMN personutilization.homecaretransitionservices IS 'Number of of transition, or placement activities provided by a registered, regulated, or licensed professional in the census interval.';
COMMENT ON COLUMN personutilization.homecareservices IS 'Number of of home care, transition, or placement activities provided by a registered, regulated, or licensed professional in the census interval.';
COMMENT ON COLUMN personutilization.homecareprofessionalvisits IS 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one home care service to the person in the census interval.';
COMMENT ON COLUMN personutilization.homecaretransitionvisits IS 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one transition, or placement service to the person in the census interval.';
COMMENT ON COLUMN personutilization.homecarevisits IS 'Number of unique combinations of days and registered, regulated, or licensed professionals when the professional provided at least one home care, transition, or placement service to the person in the census interval.';
COMMENT ON COLUMN personutilization.homecareprofessionaldays IS 'Number of unique days in the census interval when the person was provided home care services by a registered or regulated professional.';
COMMENT ON COLUMN personutilization.homecaretransitiondays IS 'Number of unique days in the census interval when the person was provided transition, or placement services by a registered or regulated professional.';
COMMENT ON COLUMN personutilization.homecaredays IS 'Number of unique days in the census interval when the person was provided home care, transition, or placement services by a registered or regulated professional.';
COMMENT ON COLUMN personutilization.laboratoryassays IS 'Number assays done of community laboratory samples collected in the census interval.';
COMMENT ON COLUMN personutilization.laboratorysitedays IS 'Number unique combinations of community laboratory collection sites and days in the census interval where the person had a collection taken.';
COMMENT ON COLUMN personutilization.laboratorydays IS 'Number of unique days in the census interval when the person had a community laboratory collection taken.';
COMMENT ON COLUMN personutilization.longtermcaredays IS 'Naive sum of long term care days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.longtermcareadmissions IS 'Long term care admissions in the census interval.';
COMMENT ON COLUMN personutilization.longtermcaredischarges IS 'Long term care discharges in the census interval.';
COMMENT ON COLUMN personutilization.longtermcarestays IS 'Long term care stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.pharmacystandarddailydoses IS 'Naive sum of days supply dispensed from a community pharmacy of standard prescription therapeutics not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolleddailydoses IS 'Naive sum of days supply dispensed from a community pharmacy of triple pad prescription therapeutics subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacydailydoses IS 'Naive sum of days supply dispensed from a community pharmacy of all prescription therapeutics.';
COMMENT ON COLUMN personutilization.pharmacystandardtherapeutics IS 'Number of distinct standard prescription therapeutics dispensed from a community pharmacy not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolledtherapeutics IS 'Number of distinct triple pad prescription therapeutics dispensed from a community pharmacy subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacytherapeutics IS 'Number of distinct prescription therapeutics dispensed from a community pharmacy.';
COMMENT ON COLUMN personutilization.pharmacystandardsitedays IS 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolledsitedays IS 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacysitedays IS 'Number of unique combinations of community pharmacies and days in the census interval when the person was dispensed any prescription therapeutic.';
COMMENT ON COLUMN personutilization.pharmacystandarddays IS 'Number of unique days in the census interval when the person was dispensed from a community pharmacy a standard prescription of a therapeutic not subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacycontrolleddays IS 'Number of unique days in the census interval when the person was dispensed from a community pharmacy a triple pad prescription of a therapeutic subject to controlled substances regulations.';
COMMENT ON COLUMN personutilization.pharmacydays IS 'Number of unique days in the census interval when the person was dispensed from a community pharmacy any prescription therapeutic.';
COMMENT ON COLUMN personutilization.anesthesiologyprocedures IS 'Number of primary care procedures in the census interval delivered by an anesthiologist in the role of most responsible procedure provider and specifically delivering care in their specialty.';
COMMENT ON COLUMN personutilization.consultprocedures IS 'Number of primary care procedures in the census interval delivered by a provider when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpracticeprocedures IS 'Number of primary care procedures in the census interval delivered by a general practitioner in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.geriatricprocedures IS 'Number of primary care procedures in the census interval delivered by a geriatrician in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.obstetricprocedures IS 'Number of primary care procedures in the census interval delivered by a obstetrician-gynecologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologyprocedures IS 'Number of primary care procedures in the census interval delivered by a pathologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatricprocedures IS 'Number of primary care procedures in the census interval delivered by a pediatrician in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatricsurgicalprocedures IS 'Number of primary care procedures in the census interval delivered by a pediatric surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.psychiatryprocedures IS 'Number of primary care procedures in the census interval delivered by a psychiatrist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologyprocedures IS 'Number of primary care procedures in the census interval delivered by a radiologist in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialtyprocedures IS 'Number of primary care procedures in the census interval delivered by a specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.surgicalprocedures IS 'Number of primary care procedures in the census interval delivered by a surgeon in the role of most responsible procedure provider and specifically delivering procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycareprocedures IS 'Number of primary care procedures in the census interval.';
COMMENT ON COLUMN personutilization.anesthesiologistsdays IS 'Number of unique combinations of primary care anesthesiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.consultprovidersdays IS 'Number of unique combinations of primary care providers and days in the census interval when either their role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpractitionersdays IS 'Number of unique combinations of primary care general practitioners and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.geriatriciansdays IS 'Number of unique combinations of primary care geriatricians and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.obstetriciansdays IS 'Number of unique combinations of primary care obstetrician-gynecologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologistsdays IS 'Number of unique combinations of primary care pathologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatriciansdays IS 'Number of unique combinations of primary care pediatricians and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatricsurgeonsdays IS 'Number of unique combinations of primary care pediatric surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.psychiatristsdays IS 'Number of unique combinations of primary care psychiatrists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologistsdays IS 'Number of unique combinations of primary care radiologists and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialistsdays IS 'Number of unique combinations of primary care specialists other than an anesthesiologists, general practitioners, pathologists, radiologists, or surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.surgeonsdays IS 'Number of unique combinations of primary care surgeons and days in the census interval when the provider was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycareproviderdays IS 'Number of unique combinations of primary care providers and unique days in the census interval when the person utilized primary care.';
COMMENT ON COLUMN personutilization.anesthesiologydays IS 'Number of unique days in the census interval when a primary care anesthesiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.consultdays IS 'Number of unique days in the census interval when either the primary care provider role was consult, assistant, or second, or the procedure was outside of their specialty.';
COMMENT ON COLUMN personutilization.generalpracticedays IS 'Number of unique days in the census interval when a primary care general practitioner was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.geriatricdays IS 'Number of unique days in the census interval when a primary care geriatrician was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.obstetricdays IS 'Number of unique days in the census interval when a primary care obstetrician-gynecologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pathologydays IS 'Number of unique days in the census interval when a primary care pathologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatricdays IS 'Number of unique days in the census interval when a primary care pediatrician was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.pediatricsurgerydays IS 'Number of unique days in the census interval when a primary care pediatric surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.psychiatrydays IS 'Number of unique days in the census interval when a primary care psychiatrist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.radiologydays IS 'Number of unique days in the census interval when a primary care radiologist was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.specialtydays IS 'Number of unique days in the census interval when a primary care specialist other than an anesthesiologist, general practitioner, pathologist, radiologist, or surgeon was in the role of most responsible procedure provider and specifically delivered care in their specialty.';
COMMENT ON COLUMN personutilization.surgerydays IS 'Number of unique days in the census interval when a primary care surgeon was in the role of most responsible procedure provider and specifically delivered procedures in their specialty.';
COMMENT ON COLUMN personutilization.primarycaredays IS 'Number of unique days in the census interval when the person visited primary care in the community.';
COMMENT ON COLUMN personutilization.designateddays IS 'Naive sum of designated supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.designatedadmissions IS 'Designated supportive living admissions in the census interval.';
COMMENT ON COLUMN personutilization.designateddischarges IS 'Designated supportive living discharges in the census interval.';
COMMENT ON COLUMN personutilization.designatedstays IS 'Designated supportive living stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.nondesignateddays IS 'Naive sum of non-designated supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.nondesignatedadmissions IS 'Non-designated supportive living admissions in the census interval.';
COMMENT ON COLUMN personutilization.nondesignateddischarges IS 'Non-designated supportive living discharges in the census interval.';
COMMENT ON COLUMN personutilization.nondesignatedstays IS 'Non-designated supportive living stays intersecting with the census interval.';
COMMENT ON COLUMN personutilization.supportivelivingdays IS 'Naive sum of supportive living days that intersected with the census interval, including overlapping stays.';
COMMENT ON COLUMN personutilization.supportivelivingadmissions IS 'Supportive living admissions in the census interval.';
COMMENT ON COLUMN personutilization.supportivelivingdischarges IS 'Supportive living discharges in the census interval.';
COMMENT ON COLUMN personutilization.supportivelivingstays IS 'Supportive living stays intersecting with the census interval.';