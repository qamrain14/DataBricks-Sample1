using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Identity;
using Microsoft.IdentityModel.Tokens;
using Api.Entities;
using Api.Models;

namespace Api.Services;

public interface IAuthService
{
    Task<AuthResponse?> LoginAsync(LoginRequest request);
    Task<AuthResponse?> RegisterAsync(RegisterRequest request);
    Task<AuthResponse?> RefreshTokenAsync(string refreshToken, string userId);
    string GenerateRefreshToken();
}

public class AuthService : IAuthService
{
    private readonly UserManager<AppUser> _userManager;
    private readonly IConfiguration _configuration;
    private readonly SignInManager<AppUser> _signInManager;

    public AuthService(
        UserManager<AppUser> userManager,
        IConfiguration configuration,
        SignInManager<AppUser> signInManager)
    {
        _userManager = userManager;
        _configuration = configuration;
        _signInManager = signInManager;
    }

    public async Task<AuthResponse?> LoginAsync(LoginRequest request)
    {
        var user = await _userManager.FindByEmailAsync(request.Email);
        if (user == null || user.IsDeleted)
            return null;

        var result = await _signInManager.CheckPasswordSignInAsync(user, request.Password, false);
        if (!result.Succeeded)
            return null;

        return await GenerateAuthResponse(user);
    }

    public async Task<AuthResponse?> RegisterAsync(RegisterRequest request)
    {
        var existingUser = await _userManager.FindByEmailAsync(request.Email);
        if (existingUser != null)
            return null;

        var user = new AppUser
        {
            UserName = request.Email,
            Email = request.Email,
            FullName = request.FullName,
            RoleDisplay = "User",
            CreatedAt = DateTime.UtcNow
        };

        var result = await _userManager.CreateAsync(user, request.Password);
        if (!result.Succeeded)
            return null;

        await _userManager.AddToRoleAsync(user, "User");
        return await GenerateAuthResponse(user);
    }

    public async Task<AuthResponse?> RefreshTokenAsync(string refreshToken, string userId)
    {
        var user = await _userManager.FindByIdAsync(userId);
        if (user == null || user.IsDeleted)
            return null;

        // In production, validate refresh token from database
        return await GenerateAuthResponse(user);
    }

    public string GenerateRefreshToken()
    {
        var randomNumber = new byte[64];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(randomNumber);
        return Convert.ToBase64String(randomNumber);
    }

    private async Task<AuthResponse> GenerateAuthResponse(AppUser user)
    {
        var roles = await _userManager.GetRolesAsync(user);
        var role = roles.FirstOrDefault() ?? "User";

        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, user.Id),
            new(ClaimTypes.Email, user.Email ?? ""),
            new(ClaimTypes.Name, user.FullName),
            new(ClaimTypes.Role, role)
        };

        var secret = _configuration["JwtSettings:Secret"] ?? "default-dev-key-min-32-characters-long!!";
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secret));
        var credentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
        var expiration = DateTime.UtcNow.AddHours(24);

        var token = new JwtSecurityToken(
            issuer: _configuration["JwtSettings:Issuer"],
            audience: _configuration["JwtSettings:Audience"],
            claims: claims,
            expires: expiration,
            signingCredentials: credentials
        );

        return new AuthResponse
        {
            Token = new JwtSecurityTokenHandler().WriteToken(token),
            RefreshToken = GenerateRefreshToken(),
            Expiration = expiration,
            User = new UserDto
            {
                Id = user.Id,
                Email = user.Email ?? "",
                FullName = user.FullName,
                Role = role
            }
        };
    }
}
