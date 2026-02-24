using Microsoft.AspNetCore.Identity;

namespace Api.Entities;

public class AppUser : IdentityUser
{
    public string FullName { get; set; } = string.Empty;
    public string? RoleDisplay { get; set; }
    public bool IsDeleted { get; set; } = false;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }

    // Navigation
    public UserProfile? Profile { get; set; }
}
