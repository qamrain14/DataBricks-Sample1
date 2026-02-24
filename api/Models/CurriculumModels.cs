using System.ComponentModel.DataAnnotations;

namespace Api.Models;

public class ClassOfferingDto
{
    public int Id { get; set; }
    public int ClassId { get; set; }
    public string? ClassName { get; set; }
    public int SubjectId { get; set; }
    public string? SubjectName { get; set; }
    public int SectionId { get; set; }
    public string? SectionName { get; set; }
    public int TimelineId { get; set; }
    public string? TimelineName { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public string? InstructorId { get; set; }
    public string? InstructorName { get; set; }
}

public class CreateClassOfferingRequest
{
    [Required]
    public int ClassId { get; set; }

    [Required]
    public int SubjectId { get; set; }

    [Required]
    public int SectionId { get; set; }

    [Required]
    public int TimelineId { get; set; }

    public string? InstructorId { get; set; }
}

public class UpdateClassOfferingRequest
{
    public int? ClassId { get; set; }
    public int? SubjectId { get; set; }
    public int? SectionId { get; set; }
    public int? TimelineId { get; set; }
    public string? InstructorId { get; set; }
}

public class ClassDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public bool IsActive { get; set; }
}

public class SubjectDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public bool IsActive { get; set; }
}

public class SectionDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Capacity { get; set; }
    public bool IsActive { get; set; }
}

public class TimelineDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public bool IsActive { get; set; }
}

public class PagedResult<T>
{
    public List<T> Items { get; set; } = new();
    public int Total { get; set; }
    public int Page { get; set; }
    public int PageSize { get; set; }
    public int TotalPages => (int)Math.Ceiling(Total / (double)PageSize);
}

public class OfferingFilterRequest
{
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 10;
    public string? Sort { get; set; }
    public string? Dir { get; set; } = "asc";
    public int? ClassId { get; set; }
    public int? SubjectId { get; set; }
    public int? SectionId { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
}
