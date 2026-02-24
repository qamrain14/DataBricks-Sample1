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
public class ProductsController : ControllerBase
{
    private readonly AppDbContext _context;

    public ProductsController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<ProductDto>>> GetProducts([FromQuery] int page = 1, [FromQuery] int pageSize = 20, [FromQuery] string? search = null)
    {
        var query = _context.Products.AsQueryable();

        if (!string.IsNullOrEmpty(search))
            query = query.Where(p => p.Name.Contains(search) || p.Sku.Contains(search));

        var totalCount = await query.CountAsync();
        var products = await query
            .OrderBy(p => p.Name)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(p => new ProductDto { Id = p.Id, Name = p.Name, Sku = p.Sku, Description = p.Description, UnitPrice = p.UnitPrice, StockQuantity = p.StockQuantity, IsActive = p.IsActive })
            .ToListAsync();

        return Ok(new PagedResult<ProductDto>
        {
            Items = products,
            Total = totalCount,
            Page = page,
            PageSize = pageSize
        });
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<ProductDto>> GetProduct(int id)
    {
        var product = await _context.Products.FindAsync(id);
        if (product == null) return NotFound();

        return Ok(new ProductDto { Id = product.Id, Name = product.Name, Sku = product.Sku, Description = product.Description, UnitPrice = product.UnitPrice, StockQuantity = product.StockQuantity, IsActive = product.IsActive });
    }

    [HttpPost]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<ProductDto>> CreateProduct([FromBody] ProductDto dto)
    {
        var product = new Product { Name = dto.Name, Sku = dto.Sku, Description = dto.Description, UnitPrice = dto.UnitPrice, StockQuantity = dto.StockQuantity, IsActive = dto.IsActive };
        _context.Products.Add(product);
        await _context.SaveChangesAsync();

        dto.Id = product.Id;
        return CreatedAtAction(nameof(GetProduct), new { id = product.Id }, dto);
    }

    [HttpPut("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<ProductDto>> UpdateProduct(int id, [FromBody] ProductDto dto)
    {
        var product = await _context.Products.FindAsync(id);
        if (product == null) return NotFound();

        product.Name = dto.Name;
        product.Sku = dto.Sku;
        product.Description = dto.Description;
        product.UnitPrice = dto.UnitPrice;
        product.StockQuantity = dto.StockQuantity;
        product.IsActive = dto.IsActive;

        await _context.SaveChangesAsync();
        dto.Id = product.Id;
        return Ok(dto);
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteProduct(int id)
    {
        var product = await _context.Products.FindAsync(id);
        if (product == null) return NotFound();

        _context.Products.Remove(product);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
