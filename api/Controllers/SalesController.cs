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
public class SalesController : ControllerBase
{
    private readonly AppDbContext _context;

    public SalesController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<SaleDto>>> GetSales([FromQuery] int page = 1, [FromQuery] int pageSize = 20, [FromQuery] DateTime? from = null, [FromQuery] DateTime? to = null)
    {
        var query = _context.Sales.Include(s => s.Customer).Include(s => s.Items).ThenInclude(i => i.Product).AsQueryable();

        if (from.HasValue)
            query = query.Where(s => s.Date >= from.Value);
        if (to.HasValue)
            query = query.Where(s => s.Date <= to.Value);

        var totalCount = await query.CountAsync();
        var sales = await query
            .OrderByDescending(s => s.Date)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(s => new SaleDto
            {
                Id = s.Id,
                Date = s.Date,
                CustomerId = s.CustomerId,
                CustomerName = s.Customer!.Name,
                Items = s.Items.Select(i => new SaleItemDto { Id = i.Id, ProductId = i.ProductId, ProductName = i.Product!.Name, Quantity = i.Quantity, UnitPrice = i.UnitPrice, LineTotal = i.Quantity * i.UnitPrice }).ToList(),
                Total = s.Total,
                Status = s.Status,
                Notes = s.Notes
            })
            .ToListAsync();

        return Ok(new PagedResult<SaleDto> { Items = sales, Total = totalCount, Page = page, PageSize = pageSize });
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<SaleDto>> GetSale(int id)
    {
        var sale = await _context.Sales
            .Include(s => s.Customer)
            .Include(s => s.Items).ThenInclude(i => i.Product)
            .FirstOrDefaultAsync(s => s.Id == id);

        if (sale == null) return NotFound();

        return Ok(new SaleDto
        {
            Id = sale.Id,
            Date = sale.Date,
            CustomerId = sale.CustomerId,
            CustomerName = sale.Customer!.Name,
            Items = sale.Items.Select(i => new SaleItemDto { Id = i.Id, ProductId = i.ProductId, ProductName = i.Product!.Name, Quantity = i.Quantity, UnitPrice = i.UnitPrice, LineTotal = i.Quantity * i.UnitPrice }).ToList(),
            Total = sale.Total,
            Status = sale.Status,
            Notes = sale.Notes
        });
    }

    [HttpPost]
    public async Task<ActionResult<SaleDto>> CreateSale([FromBody] CreateSaleRequest request)
    {
        var sale = new Sale
        {
            Date = request.Date,
            CustomerId = request.CustomerId,
            Notes = request.Notes,
            Items = request.Items.Select(i => new SaleItem { ProductId = i.ProductId, Quantity = i.Quantity, UnitPrice = i.UnitPrice }).ToList()
        };
        sale.Total = sale.Items.Sum(i => i.Quantity * i.UnitPrice);

        _context.Sales.Add(sale);

        // Update stock
        foreach (var item in request.Items)
        {
            var product = await _context.Products.FindAsync(item.ProductId);
            if (product != null)
                product.StockQuantity -= item.Quantity;
        }

        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetSale), new { id = sale.Id }, new SaleDto { Id = sale.Id, Date = sale.Date, Total = sale.Total });
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeleteSale(int id)
    {
        var sale = await _context.Sales.Include(s => s.Items).FirstOrDefaultAsync(s => s.Id == id);
        if (sale == null) return NotFound();

        // Restore stock
        foreach (var item in sale.Items)
        {
            var product = await _context.Products.FindAsync(item.ProductId);
            if (product != null)
                product.StockQuantity += item.Quantity;
        }

        _context.Sales.Remove(sale);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
