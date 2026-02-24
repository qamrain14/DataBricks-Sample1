using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Api.Data;
using Api.Entities;
using Api.Models;
using System.Security.Claims;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class UsersController : ControllerBase
{
    private readonly UserManager<AppUser> _userManager;
    private readonly AppDbContext _context;
    private readonly ILogger<UsersController> _logger;

    public UsersController(UserManager<AppUser> userManager, AppDbContext context, ILogger<UsersController> logger)
    {
        _userManager = userManager;
        _context = context;
        _logger = logger;
    }

    /// <summary>
    /// Get current user's profile
    /// </summary>
    [HttpGet("me")]
    public async Task<ActionResult<UserDto>> GetCurrentUser()
    {
        var userId = User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (string.IsNullOrEmpty(userId))
            return Unauthorized();

        var user = await _context.Users
            .Include(u => u.Profile)
            .FirstOrDefaultAsync(u => u.Id == userId);

        if (user == null)
            return NotFound();

        var roles = await _userManager.GetRolesAsync(user);

        return Ok(new UserDto
        {
            Id = user.Id,
            Email = user.Email!,
            FullName = user.FullName,
            Role = roles.FirstOrDefault() ?? "User",
            Profile = user.Profile != null ? new UserProfileDto
            {
                Phone = user.Profile.Phone,
                Address1 = user.Profile.Address1,
                Address2 = user.Profile.Address2,
                City = user.Profile.City,
                State = user.Profile.State,
                Zip = user.Profile.Zip,
                Country = user.Profile.Country,
                Latitude = user.Profile.Latitude,
                Longitude = user.Profile.Longitude
            } : null
        });
    }

    /// <summary>
    /// Update current user's profile
    /// </summary>
    [HttpPut("me")]
    public async Task<ActionResult<UserDto>> UpdateProfile([FromBody] UpdateProfileRequest request)
    {
        var userId = User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (string.IsNullOrEmpty(userId))
            return Unauthorized();

        var user = await _context.Users
            .Include(u => u.Profile)
            .FirstOrDefaultAsync(u => u.Id == userId);

        if (user == null)
            return NotFound();

        user.FullName = request.FullName ?? user.FullName;

        if (user.Profile == null)
        {
            user.Profile = new UserProfile { AppUserId = user.Id };
        }

        user.Profile.Phone = request.Phone ?? user.Profile.Phone;
        user.Profile.Address1 = request.Address1 ?? user.Profile.Address1;
        user.Profile.Address2 = request.Address2 ?? user.Profile.Address2;
        user.Profile.City = request.City ?? user.Profile.City;
        user.Profile.State = request.State ?? user.Profile.State;
        user.Profile.Zip = request.Zip ?? user.Profile.Zip;
        user.Profile.Country = request.Country ?? user.Profile.Country;
        user.Profile.Latitude = request.Latitude ?? user.Profile.Latitude;
        user.Profile.Longitude = request.Longitude ?? user.Profile.Longitude;

        await _context.SaveChangesAsync();

        var roles = await _userManager.GetRolesAsync(user);

        return Ok(new UserDto
        {
            Id = user.Id,
            Email = user.Email!,
            FullName = user.FullName,
            Role = roles.FirstOrDefault() ?? "User",
            Profile = new UserProfileDto
            {
                Phone = user.Profile.Phone,
                Address1 = user.Profile.Address1,
                Address2 = user.Profile.Address2,
                City = user.Profile.City,
                State = user.Profile.State,
                Zip = user.Profile.Zip,
                Country = user.Profile.Country,
                Latitude = user.Profile.Latitude,
                Longitude = user.Profile.Longitude
            }
        });
    }

    /// <summary>
    /// Get all users (Supervisor only)
    /// </summary>
    [HttpGet]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<PagedResult<UserDto>>> GetAllUsers([FromQuery] int page = 1, [FromQuery] int pageSize = 20)
    {
        var query = _context.Users
            .Include(u => u.Profile)
            .Where(u => !u.IsDeleted)
            .OrderBy(u => u.FullName);

        var totalCount = await query.CountAsync();
        var users = await query
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        var userDtos = new List<UserDto>();
        foreach (var user in users)
        {
            var roles = await _userManager.GetRolesAsync(user);
            userDtos.Add(new UserDto
            {
                Id = user.Id,
                Email = user.Email!,
                FullName = user.FullName,
                Role = roles.FirstOrDefault() ?? "User",
                Profile = user.Profile != null ? new UserProfileDto
                {
                    Phone = user.Profile.Phone,
                    Address1 = user.Profile.Address1,
                    Address2 = user.Profile.Address2,
                    City = user.Profile.City,
                    State = user.Profile.State,
                    Zip = user.Profile.Zip,
                    Country = user.Profile.Country,
                    Latitude = user.Profile.Latitude,
                    Longitude = user.Profile.Longitude
                } : null
            });
        }

        return Ok(new PagedResult<UserDto>
        {
            Items = userDtos,
            Total = totalCount,
            Page = page,
            PageSize = pageSize
        });
    }

    /// <summary>
    /// Soft delete a user (Supervisor only)
    /// </summary>
    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteUser(string id)
    {
        var user = await _context.Users.FindAsync(id);
        if (user == null)
            return NotFound();

        user.IsDeleted = true;
        await _context.SaveChangesAsync();

        _logger.LogInformation("User {UserId} soft-deleted by {AdminId}", id, User.FindFirstValue(ClaimTypes.NameIdentifier));

        return NoContent();
    }
}
