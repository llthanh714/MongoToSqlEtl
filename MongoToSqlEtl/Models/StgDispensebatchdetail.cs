namespace MongoToSqlEtl.Models;

public partial class StgDispensebatchdetail
{
    public string Id { get; set; } = null!;

    public string? Batchid { get; set; }

    public int? Internalbatchid { get; set; }

    public DateTime? Expirydate { get; set; }

    public int? Assignquantity { get; set; }

    public string Patientorderitemsuid { get; set; } = null!;
}
