namespace Api.Entities;

public class UserProfile
{
    public int Id { get; set; }
    public string AppUserId { get; set; } = string.Empty;
    public string? Phone { get; set; }
    
    // Location fields
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    public string? Country { get; set; }
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }

    // Navigation
    public AppUser? AppUser { get; set; }
}
