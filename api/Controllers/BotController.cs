using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Api.Models;
using Api.Services;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize(Policy = "SupervisorOnly")]
public class BotController : ControllerBase
{
    private readonly ISupervisorBotService _botService;
    private readonly ILogger<BotController> _logger;

    public BotController(ISupervisorBotService botService, ILogger<BotController> logger)
    {
        _botService = botService;
        _logger = logger;
    }

    /// <summary>
    /// Send a query to the supervisor bot
    /// </summary>
    [HttpPost("query")]
    public async Task<ActionResult<BotReply>> Query([FromBody] BotQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Query))
            return BadRequest(new { message = "Query cannot be empty" });

        _logger.LogInformation("Bot query received: {Query}", request.Query);

        var reply = await _botService.ProcessQueryAsync(request.Query);

        return Ok(reply);
    }
}
