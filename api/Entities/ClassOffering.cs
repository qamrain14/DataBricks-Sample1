namespace Api.Entities;

public class ClassOffering
{
    public int Id { get; set; }
    public int ClassId { get; set; }
    public int SubjectId { get; set; }
    public int SectionId { get; set; }
    public int TimelineId { get; set; }
    public string? InstructorId { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    // Navigation
    public Class? Class { get; set; }
    public Subject? Subject { get; set; }
    public Section? Section { get; set; }
    public Timeline? Timeline { get; set; }
    public AppUser? Instructor { get; set; }
}
