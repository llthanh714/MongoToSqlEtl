namespace MongoToSqlEtl.Models;

public partial class StgPatientdiagnosisuid
{
    public string Id { get; set; } = null!;

    public string? Diagnosisuid { get; set; }

    public string Patientordersuid { get; set; } = null!;
}
