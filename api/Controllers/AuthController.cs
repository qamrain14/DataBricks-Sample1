using Microsoft.AspNetCore.Mvc;
using Api.Models;
using Api.Services;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly IAuthService _authService;
    private readonly ILogger<AuthController> _logger;

    public AuthController(IAuthService authService, ILogger<AuthController> logger)
    {
        _authService = authService;
        _logger = logger;
    }

    /// <summary>
    /// Authenticate user and receive JWT token
    /// </summary>
    [HttpPost("login")]
    public async Task<ActionResult<AuthResponse>> Login([FromBody] LoginRequest request)
    {
        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        var result = await _authService.LoginAsync(request);
        if (result == null)
        {
            _logger.LogWarning("Failed login attempt for {Email}", request.Email);
            return Unauthorized(new { message = "Invalid email or password" });
        }

        return Ok(result);
    }

    /// <summary>
    /// Register a new user account
    /// </summary>
    [HttpPost("register")]
    public async Task<ActionResult<AuthResponse>> Register([FromBody] RegisterRequest request)
    {
        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        var result = await _authService.RegisterAsync(request);
        if (result == null)
        {
            _logger.LogWarning("Failed registration for {Email}", request.Email);
            return BadRequest(new { message = "Registration failed. Email may already be in use." });
        }

        _logger.LogInformation("New user registered: {Email}", request.Email);
        return Ok(result);
    }

    /// <summary>
    /// Refresh access token using refresh token
    /// </summary>
    [HttpPost("refresh")]
    public async Task<ActionResult<AuthResponse>> RefreshToken([FromBody] RefreshTokenRequest request)
    {
        if (string.IsNullOrEmpty(request.RefreshToken))
            return BadRequest(new { message = "Refresh token is required" });

        var userId = User.FindFirst(System.Security.Claims.ClaimTypes.NameIdentifier)?.Value ?? string.Empty;
        var result = await _authService.RefreshTokenAsync(request.RefreshToken, userId);
        if (result == null)
            return Unauthorized(new { message = "Invalid refresh token" });

        return Ok(result);
    }
}
