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
public class SuppliersController : ControllerBase
{
    private readonly AppDbContext _context;

    public SuppliersController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<SupplierDto>>> GetSuppliers([FromQuery] int page = 1, [FromQuery] int pageSize = 20, [FromQuery] string? search = null)
    {
        var query = _context.Suppliers.AsQueryable();

        if (!string.IsNullOrEmpty(search))
            query = query.Where(s => s.Name.Contains(search));

        var totalCount = await query.CountAsync();
        var suppliers = await query
            .OrderBy(s => s.Name)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(s => new SupplierDto { Id = s.Id, Name = s.Name, Email = s.Email, Phone = s.Phone, Address = s.Address, IsActive = s.IsActive })
            .ToListAsync();

        return Ok(new PagedResult<SupplierDto> { Items = suppliers, Total = totalCount, Page = page, PageSize = pageSize });
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<SupplierDto>> GetSupplier(int id)
    {
        var supplier = await _context.Suppliers.FindAsync(id);
        if (supplier == null) return NotFound();

        return Ok(new SupplierDto { Id = supplier.Id, Name = supplier.Name, Email = supplier.Email, Phone = supplier.Phone, Address = supplier.Address, IsActive = supplier.IsActive });
    }

    [HttpPost]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<SupplierDto>> CreateSupplier([FromBody] SupplierDto dto)
    {
        var supplier = new Supplier { Name = dto.Name, Email = dto.Email, Phone = dto.Phone, Address = dto.Address, IsActive = dto.IsActive };
        _context.Suppliers.Add(supplier);
        await _context.SaveChangesAsync();

        dto.Id = supplier.Id;
        return CreatedAtAction(nameof(GetSupplier), new { id = supplier.Id }, dto);
    }

    [HttpPut("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<SupplierDto>> UpdateSupplier(int id, [FromBody] SupplierDto dto)
    {
        var supplier = await _context.Suppliers.FindAsync(id);
        if (supplier == null) return NotFound();

        supplier.Name = dto.Name;
        supplier.Email = dto.Email;
        supplier.Phone = dto.Phone;
        supplier.Address = dto.Address;
        supplier.IsActive = dto.IsActive;

        await _context.SaveChangesAsync();
        dto.Id = supplier.Id;
        return Ok(dto);
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteSupplier(int id)
    {
        var supplier = await _context.Suppliers.FindAsync(id);
        if (supplier == null) return NotFound();

        _context.Suppliers.Remove(supplier);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
