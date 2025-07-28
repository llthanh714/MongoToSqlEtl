namespace MongoToSqlEtl.Models;

public partial class StgPatientorderitem
{
    public string Id { get; set; } = null!;

    public string? Noofcopies { get; set; }

    public string? Orderitemuid { get; set; }

    public bool? Iscontinuousorder { get; set; }

    public bool? Broughtfromoutside { get; set; }

    public bool? Excludeacknowledgement { get; set; }

    public string? Ordercattype { get; set; }

    public bool? Isacknowledgementmandatory { get; set; }

    public bool? Isdoctorshareitem { get; set; }

    public int? Uniqrefno { get; set; }

    public string? Ordercatuid { get; set; }

    public string? Ordersubcatuid { get; set; }

    public string? Billingtype { get; set; }

    public string? Orderitemname { get; set; }

    public DateTime? Startdate { get; set; }

    public DateTime? Enddate { get; set; }

    public string? Statusuid { get; set; }

    public bool? Isactive { get; set; }

    public int? Maxqty { get; set; }

    public int? Quantity { get; set; }

    public int? Duration { get; set; }

    public string? Durationuom { get; set; }

    public bool? Isexecutable { get; set; }

    public bool? Isapprovalrequired { get; set; }

    public bool? Pharmacyapprovalrequired { get; set; }

    public bool? Antibioticapprovalrequired { get; set; }

    public bool? Iscosignrequired { get; set; }

    public bool? Isnursecosignrequired { get; set; }

    public bool? Ishourlycharge { get; set; }

    public string? Infusionrateuom { get; set; }

    public string? Infusiondurationuom { get; set; }

    public string? Priorityuid { get; set; }

    public bool? Isoutlab { get; set; }

    public bool? Departmentnotification { get; set; }

    public bool? Isstockitem { get; set; }

    public string? Associateddiagnosisdetailuid { get; set; }

    public bool? Isexcludeprebilling { get; set; }

    public bool? Istaperingdose { get; set; }

    public bool? Istitration { get; set; }

    public int? Payordiscount { get; set; }

    public int? Copayamount { get; set; }

    public bool? Isotcsale { get; set; }

    public string? Patientvisitpayoruid { get; set; }

    public bool? Ispriceoverwritten { get; set; }

    public int? Unitpayordiscount { get; set; }

    public int? Specialdiscount { get; set; }

    public string? Billinggroupuid { get; set; }

    public string? Billingsubgroupuid { get; set; }

    public int? Taxamount { get; set; }

    public bool? Iscopayexcluded { get; set; }

    public string? Chargecode { get; set; }

    public string? Tariffuid { get; set; }

    public string? Tarifftypeuid { get; set; }

    public string? QuantityUom { get; set; }

    public int? Mfccopay { get; set; }

    public bool? Irradiation { get; set; }

    public bool? Washedrbc { get; set; }

    public bool? Thaw { get; set; }

    public bool? Isleukodepleted { get; set; }

    public bool? Isadditivereduced { get; set; }

    public bool? Isintrauterinefetaltransfusion { get; set; }

    public bool? Cmvtested { get; set; }

    public bool? Iscrossmatchrequired { get; set; }

    public bool? Withoutsample { get; set; }

    public bool? Linkedorderbackground { get; set; }

    public bool? Ismtp { get; set; }

    public bool? Isfasting { get; set; }

    public bool? Formuladiet { get; set; }

    public string? Sameorderlinknumber { get; set; }

    public bool? Taxondoctorgst { get; set; }

    public bool? Isneedadditionalapprove { get; set; }

    public string? Orgorderitemuid { get; set; }

    public string? Assignquantity { get; set; }

    public string? Batchid { get; set; }

    public DateTime? Expirydate { get; set; }

    public int? Internalbatchid { get; set; }

    public DateTime? UpdatedAt { get; set; }

    public bool? Isbillingapprovalrequired { get; set; }

    public string? Ordersetuid { get; set; }

    public bool? Nodiscount { get; set; }

    public string? Tariffordersetuid { get; set; }

    public string? Frequencyuid { get; set; }

    public string? Routeuid { get; set; }

    public string? DosageUom { get; set; }

    public string? Clinicalinformation { get; set; }

    public string? Drugmasteruid { get; set; }

    public bool? Isipfillallowed { get; set; }

    public bool? Isdiluent { get; set; }

    public string? Instructionuid { get; set; }

    public string? Dosageoveruom { get; set; }

    public string? Associatedproblemuid { get; set; }

    public string? Dosageoverwithuom { get; set; }

    public string? Administrationinstruction { get; set; }

    public string? Localadministrationinstruction { get; set; }

    public int? Quantityperdose { get; set; }

    public bool? Ispreauthrequired { get; set; }

    public DateTime? Waitingtime { get; set; }

    public string? Flexipackageuid { get; set; }

    public string? Patientpackageitemuid { get; set; }

    public string? Patientpackageuid { get; set; }

    public int? Balanceduration { get; set; }

    public string? Balancedurationuom { get; set; }

    public bool? Isirradition { get; set; }

    public bool? Isthaw { get; set; }

    public bool? Iswashed { get; set; }

    public string? Patientfutureorderuid { get; set; }

    public bool? Usepreviousspecimen { get; set; }

    public string? Specimenuid { get; set; }

    public DateTime? Specimencollectedat { get; set; }

    public string? Specimennumber { get; set; }

    public string? Comments { get; set; }

    public DateTime? Executedat { get; set; }

    public string? Externalstatusuid { get; set; }

    public DateTime? Specimenacceptedat { get; set; }

    public DateTime? Resultauthorizedat { get; set; }

    public bool? Isnokdiet { get; set; }

    public int? Unitsbasedprice { get; set; }

    public bool? Feederrequired { get; set; }

    public string? Copiedfromorderuid { get; set; }

    public string? Parentorderuid { get; set; }

    public string? Parentorderitemuid { get; set; }

    public bool? Isdaywisepackage { get; set; }

    public bool? Ismodifier { get; set; }

    public DateTime? CreatedAt { get; set; }

    public DateTime? Canceldatetime { get; set; }

    public string? Cancelledby { get; set; }

    public DateTime? Discontinueddatetime { get; set; }

    public int? Noofexposures { get; set; }

    public string? Accessionnumber { get; set; }

    public string? Executedby { get; set; }

    public string? Executedbyuseruid { get; set; }

    public DateTime? Resultenteredat { get; set; }

    public int? Totalquantity { get; set; }

    public string? Parentordernumber { get; set; }

    public bool? Itemwisecharging { get; set; }

    public bool? Hasinterventions { get; set; }

    public bool? Isprnorder { get; set; }

    public string Patientordersuid { get; set; } = null!;
}
