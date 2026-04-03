using Api.Data;
using Api.Entities;
using Api.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize]
public class CustomersController : ControllerBase
{
    private readonly AppDbContext _context;

    public CustomersController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<CustomerDto>>> GetCustomers(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 20,
        [FromQuery] string? search = null
    )
    {
        var query = _context.Customers.AsQueryable();

        if (!string.IsNullOrEmpty(search))
            query = query.Where(c =>
                c.Name.Contains(search) || (c.Email != null && c.Email.Contains(search))
            );

        var totalCount = await query.CountAsync();
        var customers = await query
            .OrderBy(c => c.Name)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(c => new CustomerDto
            {
                Id = c.Id,
                Name = c.Name,
                Email = c.Email,
                Phone = c.Phone,
            })
            .ToListAsync();

        return Ok(
            new PagedResult<CustomerDto>
            {
                Items = customers,
                Total = totalCount,
                Page = page,
                PageSize = pageSize,
            }
        );
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<CustomerDto>> GetCustomer(int id)
    {
        var customer = await _context.Customers.FindAsync(id);
        if (customer == null)
            return NotFound();

        return Ok(
            new CustomerDto
            {
                Id = customer.Id,
                Name = customer.Name,
                Email = customer.Email,
                Phone = customer.Phone,
            }
        );
    }

    [HttpPost]
    public async Task<ActionResult<CustomerDto>> CreateCustomer([FromBody] CustomerDto dto)
    {
        var customer = new Customer
        {
            Name = dto.Name,
            Email = dto.Email,
            Phone = dto.Phone,
        };
        _context.Customers.Add(customer);
        await _context.SaveChangesAsync();

        dto.Id = customer.Id;
        return CreatedAtAction(nameof(GetCustomer), new { id = customer.Id }, dto);
    }

    [HttpPut("{id}")]
    public async Task<ActionResult<CustomerDto>> UpdateCustomer(int id, [FromBody] CustomerDto dto)
    {
        var customer = await _context.Customers.FindAsync(id);
        if (customer == null)
            return NotFound();

        customer.Name = dto.Name;
        customer.Email = dto.Email;
        customer.Phone = dto.Phone;

        await _context.SaveChangesAsync();
        dto.Id = customer.Id;
        return Ok(dto);
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteCustomer(int id)
    {
        var customer = await _context.Customers.FindAsync(id);
        if (customer == null)
            return NotFound();

        _context.Customers.Remove(customer);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
