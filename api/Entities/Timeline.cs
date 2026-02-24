namespace Api.Entities;

public class Timeline
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public bool IsActive { get; set; } = true;

    // Navigation
    public ICollection<ClassOffering> ClassOfferings { get; set; } = new List<ClassOffering>();
}
