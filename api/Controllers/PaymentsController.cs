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
public class PaymentsController : ControllerBase
{
    private readonly AppDbContext _context;

    public PaymentsController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<PaymentDto>>> GetPayments([FromQuery] int page = 1, [FromQuery] int pageSize = 20, [FromQuery] PaymentDirection? direction = null, [FromQuery] DateTime? from = null, [FromQuery] DateTime? to = null)
    {
        var query = _context.Payments.AsQueryable();

        if (direction.HasValue)
            query = query.Where(p => p.Direction == direction.Value);
        if (from.HasValue)
            query = query.Where(p => p.Date >= from.Value);
        if (to.HasValue)
            query = query.Where(p => p.Date <= to.Value);

        var totalCount = await query.CountAsync();
        var payments = await query
            .OrderByDescending(p => p.Date)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(p => new PaymentDto
            {
                Id = p.Id,
                Date = p.Date,
                Amount = p.Amount,
                Direction = p.Direction,
                ReferenceId = p.ReferenceId,
                ReferenceType = p.ReferenceType,
                Notes = p.Notes
            })
            .ToListAsync();

        return Ok(new PagedResult<PaymentDto> { Items = payments, Total = totalCount, Page = page, PageSize = pageSize });
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<PaymentDto>> GetPayment(int id)
    {
        var payment = await _context.Payments.FindAsync(id);
        if (payment == null) return NotFound();

        return Ok(new PaymentDto
        {
            Id = payment.Id,
            Date = payment.Date,
            Amount = payment.Amount,
            Direction = payment.Direction,
            ReferenceId = payment.ReferenceId,
            ReferenceType = payment.ReferenceType,
            Notes = payment.Notes
        });
    }

    [HttpPost]
    public async Task<ActionResult<PaymentDto>> CreatePayment([FromBody] CreatePaymentRequest request)
    {
        var payment = new Payment
        {
            Date = request.Date,
            Amount = request.Amount,
            Direction = request.Direction,
            ReferenceId = request.ReferenceId,
            ReferenceType = request.ReferenceType,
            Notes = request.Notes
        };

        _context.Payments.Add(payment);
        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetPayment), new { id = payment.Id }, new PaymentDto
        {
            Id = payment.Id,
            Date = payment.Date,
            Amount = payment.Amount,
            Direction = payment.Direction,
            ReferenceId = payment.ReferenceId,
            ReferenceType = payment.ReferenceType,
            Notes = payment.Notes
        });
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeletePayment(int id)
    {
        var payment = await _context.Payments.FindAsync(id);
        if (payment == null) return NotFound();

        _context.Payments.Remove(payment);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
