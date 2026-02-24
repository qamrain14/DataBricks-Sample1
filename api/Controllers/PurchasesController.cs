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
public class PurchasesController : ControllerBase
{
    private readonly AppDbContext _context;

    public PurchasesController(AppDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<PagedResult<PurchaseDto>>> GetPurchases([FromQuery] int page = 1, [FromQuery] int pageSize = 20, [FromQuery] DateTime? from = null, [FromQuery] DateTime? to = null)
    {
        var query = _context.Purchases.Include(p => p.Supplier).Include(p => p.Items).ThenInclude(i => i.Product).AsQueryable();

        if (from.HasValue)
            query = query.Where(p => p.Date >= from.Value);
        if (to.HasValue)
            query = query.Where(p => p.Date <= to.Value);

        var totalCount = await query.CountAsync();
        var purchases = await query
            .OrderByDescending(p => p.Date)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(p => new PurchaseDto
            {
                Id = p.Id,
                Date = p.Date,
                SupplierId = p.SupplierId,
                SupplierName = p.Supplier!.Name,
                Items = p.Items.Select(i => new PurchaseItemDto { Id = i.Id, ProductId = i.ProductId, ProductName = i.Product!.Name, Quantity = i.Quantity, UnitPrice = i.UnitPrice, LineTotal = i.Quantity * i.UnitPrice }).ToList(),
                Total = p.Total,
                Status = p.Status,
                Notes = p.Notes
            })
            .ToListAsync();

        return Ok(new PagedResult<PurchaseDto> { Items = purchases, Total = totalCount, Page = page, PageSize = pageSize });
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<PurchaseDto>> GetPurchase(int id)
    {
        var purchase = await _context.Purchases
            .Include(p => p.Supplier)
            .Include(p => p.Items).ThenInclude(i => i.Product)
            .FirstOrDefaultAsync(p => p.Id == id);

        if (purchase == null) return NotFound();

        return Ok(new PurchaseDto
        {
            Id = purchase.Id,
            Date = purchase.Date,
            SupplierId = purchase.SupplierId,
            SupplierName = purchase.Supplier!.Name,
            Items = purchase.Items.Select(i => new PurchaseItemDto { Id = i.Id, ProductId = i.ProductId, ProductName = i.Product!.Name, Quantity = i.Quantity, UnitPrice = i.UnitPrice, LineTotal = i.Quantity * i.UnitPrice }).ToList(),
            Total = purchase.Total,
            Status = purchase.Status,
            Notes = purchase.Notes
        });
    }

    [HttpPost]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult<PurchaseDto>> CreatePurchase([FromBody] CreatePurchaseRequest request)
    {
        var purchase = new Purchase
        {
            Date = request.Date,
            SupplierId = request.SupplierId,
            Notes = request.Notes,
            Items = request.Items.Select(i => new PurchaseItem { ProductId = i.ProductId, Quantity = i.Quantity, UnitPrice = i.UnitPrice }).ToList()
        };
        purchase.Total = purchase.Items.Sum(i => i.Quantity * i.UnitPrice);

        _context.Purchases.Add(purchase);

        // Update stock
        foreach (var item in request.Items)
        {
            var product = await _context.Products.FindAsync(item.ProductId);
            if (product != null)
                product.StockQuantity += item.Quantity;
        }

        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetPurchase), new { id = purchase.Id }, new PurchaseDto { Id = purchase.Id, Date = purchase.Date, Total = purchase.Total });
    }

    [HttpDelete("{id}")]
    [Authorize(Policy = "SupervisorOnly")]
    public async Task<ActionResult> DeletePurchase(int id)
    {
        var purchase = await _context.Purchases.Include(p => p.Items).FirstOrDefaultAsync(p => p.Id == id);
        if (purchase == null) return NotFound();

        // Restore stock
        foreach (var item in purchase.Items)
        {
            var product = await _context.Products.FindAsync(item.ProductId);
            if (product != null)
                product.StockQuantity -= item.Quantity;
        }

        _context.Purchases.Remove(purchase);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}
