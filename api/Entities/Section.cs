namespace Api.Entities;

public class Section
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Capacity { get; set; }
    public bool IsActive { get; set; } = true;

    // Navigation
    public ICollection<ClassOffering> ClassOfferings { get; set; } = new List<ClassOffering>();
}
