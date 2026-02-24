using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Api.Data;
using Api.Entities;
using Api.Models;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
[Authorize(Policy = "SupervisorOnly")]
public class SummaryController : ControllerBase
{
    private readonly AppDbContext _context;

    public SummaryController(AppDbContext context)
    {
        _context = context;
    }

    /// <summary>
    /// Get commerce summary for supervisors
    /// </summary>
    [HttpGet]
    public async Task<ActionResult<CommerceSummaryDto>> GetSummary([FromQuery] DateTime? from = null, [FromQuery] DateTime? to = null)
    {
        var startDate = from ?? DateTime.UtcNow.AddDays(-30);
        var endDate = to ?? DateTime.UtcNow;

        // Sales
        var salesQuery = _context.Sales.Where(s => s.Date >= startDate && s.Date <= endDate);
        var totalSales = await salesQuery.SumAsync(s => s.Total);
        var salesCount = await salesQuery.CountAsync();

        // Purchases
        var purchasesQuery = _context.Purchases.Where(p => p.Date >= startDate && p.Date <= endDate);
        var totalPurchases = await purchasesQuery.SumAsync(p => p.Total);
        var purchasesCount = await purchasesQuery.CountAsync();

        // Payments
        var paymentsQuery = _context.Payments.Where(p => p.Date >= startDate && p.Date <= endDate);
        var incomingPayments = await paymentsQuery.Where(p => p.Direction == PaymentDirection.Incoming).SumAsync(p => p.Amount);
        var outgoingPayments = await paymentsQuery.Where(p => p.Direction == PaymentDirection.Outgoing).SumAsync(p => p.Amount);

        // Top selling products
        var topProducts = await _context.SaleItems
            .Include(si => si.Sale)
            .Include(si => si.Product)
            .Where(si => si.Sale!.Date >= startDate && si.Sale.Date <= endDate)
            .GroupBy(si => new { si.ProductId, si.Product!.Name })
            .Select(g => new TopItemDto { Name = g.Key.Name, Total = g.Sum(si => si.Quantity * si.UnitPrice), Count = g.Sum(si => si.Quantity) })
            .OrderByDescending(x => x.Total)
            .Take(5)
            .ToListAsync();

        // Top customers
        var topCustomers = await _context.Sales
            .Include(s => s.Customer)
            .Where(s => s.Date >= startDate && s.Date <= endDate)
            .GroupBy(s => new { s.CustomerId, s.Customer!.Name })
            .Select(g => new TopItemDto { Name = g.Key.Name, Total = g.Sum(s => s.Total), Count = g.Count() })
            .OrderByDescending(x => x.Total)
            .Take(5)
            .ToListAsync();

        // Daily summary
        var dailySales = await _context.Sales
            .Where(s => s.Date >= startDate && s.Date <= endDate)
            .GroupBy(s => s.Date.Date)
            .Select(g => new { Date = g.Key, Sales = g.Sum(s => s.Total) })
            .ToListAsync();

        var dailyPurchases = await _context.Purchases
            .Where(p => p.Date >= startDate && p.Date <= endDate)
            .GroupBy(p => p.Date.Date)
            .Select(g => new { Date = g.Key, Purchases = g.Sum(p => p.Total) })
            .ToListAsync();

        var allDates = dailySales.Select(d => d.Date).Union(dailyPurchases.Select(d => d.Date)).Distinct().OrderBy(d => d);
        var dailySummary = allDates.Select(date => new DailySummaryDto
        {
            Date = date,
            Sales = dailySales.FirstOrDefault(d => d.Date == date)?.Sales ?? 0,
            Purchases = dailyPurchases.FirstOrDefault(d => d.Date == date)?.Purchases ?? 0
        }).ToList();

        return Ok(new CommerceSummaryDto
        {
            TotalSales = totalSales,
            SalesCount = salesCount,
            TotalPurchases = totalPurchases,
            PurchasesCount = purchasesCount,
            TotalIncomingPayments = incomingPayments,
            TotalOutgoingPayments = outgoingPayments,
            NetCashFlow = incomingPayments - outgoingPayments,
            TopProducts = topProducts,
            TopCustomers = topCustomers,
            DailyBreakdown = dailySummary
        });
    }
}
