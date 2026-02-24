using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Api.Data;
using Api.Entities;
using Api.Models;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class CurriculumController : ControllerBase
{
    private readonly AppDbContext _context;
    private readonly ILogger<CurriculumController> _logger;

    public CurriculumController(AppDbContext context, ILogger<CurriculumController> logger)
    {
        _context = context;
        _logger = logger;
    }

    /// <summary>
    /// Get class offerings with filtering and pagination
    /// </summary>
    [HttpGet("offerings")]
    public async Task<ActionResult<PagedResult<ClassOfferingDto>>> GetOfferings([FromQuery] OfferingFilterRequest filter)
    {
        var query = _context.ClassOfferings
            .Include(o => o.Class)
            .Include(o => o.Subject)
            .Include(o => o.Section)
            .Include(o => o.Timeline)
            .Include(o => o.Instructor)
            .AsQueryable();

        if (filter.ClassId.HasValue)
            query = query.Where(o => o.ClassId == filter.ClassId.Value);

        if (filter.SubjectId.HasValue)
            query = query.Where(o => o.SubjectId == filter.SubjectId.Value);

        if (filter.SectionId.HasValue)
            query = query.Where(o => o.SectionId == filter.SectionId.Value);

        if (filter.StartDate.HasValue)
            query = query.Where(o => o.Timeline!.StartDate >= filter.StartDate.Value);

        if (filter.EndDate.HasValue)
            query = query.Where(o => o.Timeline!.EndDate <= filter.EndDate.Value);

        var totalCount = await query.CountAsync();

        var offerings = await query
            .OrderBy(o => o.Class!.Name)
            .ThenBy(o => o.Subject!.Name)
            .Skip((filter.Page - 1) * filter.PageSize)
            .Take(filter.PageSize)
            .Select(o => new ClassOfferingDto
            {
                Id = o.Id,
                ClassId = o.ClassId,
                ClassName = o.Class!.Name,
                SubjectId = o.SubjectId,
                SubjectName = o.Subject!.Name,
                SectionId = o.SectionId,
                SectionName = o.Section!.Name,
                TimelineId = o.TimelineId,
                TimelineName = o.Timeline!.Name,
                StartDate = o.Timeline.StartDate,
                EndDate = o.Timeline.EndDate,
                InstructorId = o.InstructorId,
                InstructorName = o.Instructor != null ? o.Instructor.FullName : null
            })
            .ToListAsync();

        return Ok(new PagedResult<ClassOfferingDto>
        {
            Items = offerings,
            Total = totalCount,
            Page = filter.Page,
            PageSize = filter.PageSize
        });
    }

    /// <summary>
    /// Get all classes
    /// </summary>
    [HttpGet("classes")]
    public async Task<ActionResult<List<ClassDto>>> GetClasses()
    {
        var classes = await _context.Classes
            .OrderBy(c => c.Name)
            .Select(c => new ClassDto { Id = c.Id, Name = c.Name, Description = c.Description, IsActive = c.IsActive })
            .ToListAsync();

        return Ok(classes);
    }

    /// <summary>
    /// Get all subjects
    /// </summary>
    [HttpGet("subjects")]
    public async Task<ActionResult<List<SubjectDto>>> GetSubjects()
    {
        var subjects = await _context.Subjects
            .OrderBy(s => s.Name)
            .Select(s => new SubjectDto { Id = s.Id, Name = s.Name, Description = s.Description, IsActive = s.IsActive })
            .ToListAsync();

        return Ok(subjects);
    }

    /// <summary>
    /// Get all sections
    /// </summary>
    [HttpGet("sections")]
    public async Task<ActionResult<List<SectionDto>>> GetSections()
    {
        var sections = await _context.Sections
            .OrderBy(s => s.Name)
            .Select(s => new SectionDto { Id = s.Id, Name = s.Name, Capacity = s.Capacity, IsActive = s.IsActive })
            .ToListAsync();

        return Ok(sections);
    }

    /// <summary>
    /// Get all timelines
    /// </summary>
    [HttpGet("timelines")]
    public async Task<ActionResult<List<TimelineDto>>> GetTimelines()
    {
        var timelines = await _context.Timelines
            .OrderByDescending(t => t.StartDate)
            .Select(t => new TimelineDto { Id = t.Id, Name = t.Name, StartDate = t.StartDate, EndDate = t.EndDate, IsActive = t.IsActive })
            .ToListAsync();

        return Ok(timelines);
    }

    /// <summary>
    /// Create a new class offering (Supervisor only)
    /// </summary>
    [HttpPost("offerings")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<ClassOfferingDto>> CreateOffering([FromBody] CreateOfferingRequest request)
    {
        var offering = new ClassOffering
        {
            ClassId = request.ClassId,
            SubjectId = request.SubjectId,
            SectionId = request.SectionId,
            TimelineId = request.TimelineId,
            InstructorId = request.InstructorId
        };

        _context.ClassOfferings.Add(offering);
        await _context.SaveChangesAsync();

        var created = await _context.ClassOfferings
            .Include(o => o.Class)
            .Include(o => o.Subject)
            .Include(o => o.Section)
            .Include(o => o.Timeline)
            .Include(o => o.Instructor)
            .FirstAsync(o => o.Id == offering.Id);

        return CreatedAtAction(nameof(GetOfferings), new { id = offering.Id }, new ClassOfferingDto
        {
            Id = created.Id,
            ClassId = created.ClassId,
            ClassName = created.Class!.Name,
            SubjectId = created.SubjectId,
            SubjectName = created.Subject!.Name,
            SectionId = created.SectionId,
            SectionName = created.Section!.Name,
            TimelineId = created.TimelineId,
            TimelineName = created.Timeline!.Name,
            StartDate = created.Timeline.StartDate,
            EndDate = created.Timeline.EndDate,
            InstructorId = created.InstructorId,
            InstructorName = created.Instructor?.FullName
        });
    }

    /// <summary>
    /// Delete a class offering (Supervisor only)
    /// </summary>
    [HttpDelete("offerings/{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteOffering(int id)
    {
        var offering = await _context.ClassOfferings.FindAsync(id);
        if (offering == null)
            return NotFound();

        _context.ClassOfferings.Remove(offering);
        await _context.SaveChangesAsync();

        return NoContent();
    }
}

public class CreateOfferingRequest
{
    public int ClassId { get; set; }
    public int SubjectId { get; set; }
    public int SectionId { get; set; }
    public int TimelineId { get; set; }
    public string? InstructorId { get; set; }
}
