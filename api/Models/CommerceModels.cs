using System.ComponentModel.DataAnnotations;
using Api.Entities;

namespace Api.Models;

// Product
public class ProductDto
{
    public int Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public decimal UnitPrice { get; set; }
    public int StockQuantity { get; set; }
    public bool IsActive { get; set; }
}

public class CreateProductRequest
{
    [Required]
    public string Sku { get; set; } = string.Empty;

    [Required]
    public string Name { get; set; } = string.Empty;

    public string? Description { get; set; }

    [Range(0.01, double.MaxValue)]
    public decimal UnitPrice { get; set; }

    [Range(0, int.MaxValue)]
    public int StockQuantity { get; set; }
}

// Customer
public class CustomerDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
    public bool IsActive { get; set; }
}

public class CreateCustomerRequest
{
    [Required]
    public string Name { get; set; } = string.Empty;

    [EmailAddress]
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
}

// Supplier
public class SupplierDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
    public bool IsActive { get; set; }
}

public class CreateSupplierRequest
{
    [Required]
    public string Name { get; set; } = string.Empty;

    [EmailAddress]
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
}

// Sale
public class SaleDto
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public int CustomerId { get; set; }
    public string? CustomerName { get; set; }
    public decimal Total { get; set; }
    public SaleStatus Status { get; set; }
    public string? Notes { get; set; }
    public List<SaleItemDto> Items { get; set; } = new();
}

public class SaleItemDto
{
    public int Id { get; set; }
    public int ProductId { get; set; }
    public string? ProductName { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal { get; set; }
}

public class CreateSaleRequest
{
    [Required]
    public DateTime Date { get; set; }

    [Required]
    public int CustomerId { get; set; }

    public string? Notes { get; set; }

    [Required]
    [MinLength(1)]
    public List<CreateSaleItemRequest> Items { get; set; } = new();
}

public class CreateSaleItemRequest
{
    [Required]
    public int ProductId { get; set; }

    [Range(1, int.MaxValue)]
    public int Quantity { get; set; }

    [Range(0.01, double.MaxValue)]
    public decimal UnitPrice { get; set; }
}

// Purchase
public class PurchaseDto
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public int SupplierId { get; set; }
    public string? SupplierName { get; set; }
    public decimal Total { get; set; }
    public PurchaseStatus Status { get; set; }
    public string? Notes { get; set; }
    public List<PurchaseItemDto> Items { get; set; } = new();
}

public class PurchaseItemDto
{
    public int Id { get; set; }
    public int ProductId { get; set; }
    public string? ProductName { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal { get; set; }
}

public class CreatePurchaseRequest
{
    [Required]
    public DateTime Date { get; set; }

    [Required]
    public int SupplierId { get; set; }

    public string? Notes { get; set; }

    [Required]
    [MinLength(1)]
    public List<CreatePurchaseItemRequest> Items { get; set; } = new();
}

public class CreatePurchaseItemRequest
{
    [Required]
    public int ProductId { get; set; }

    [Range(1, int.MaxValue)]
    public int Quantity { get; set; }

    [Range(0.01, double.MaxValue)]
    public decimal UnitPrice { get; set; }
}

// Payment
public class PaymentDto
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public decimal Amount { get; set; }
    public PaymentDirection Direction { get; set; }
    public int? ReferenceId { get; set; }
    public string? ReferenceType { get; set; }
    public string? Notes { get; set; }
}

public class CreatePaymentRequest
{
    [Required]
    public DateTime Date { get; set; }

    [Range(0.01, double.MaxValue)]
    public decimal Amount { get; set; }

    [Required]
    public PaymentDirection Direction { get; set; }

    public int? ReferenceId { get; set; }
    public string? ReferenceType { get; set; }
    public string? Notes { get; set; }
}

// Summary
public class CommerceSummaryDto
{
    public decimal TotalSales { get; set; }
    public decimal TotalPurchases { get; set; }
    public decimal TotalIncomingPayments { get; set; }
    public decimal TotalOutgoingPayments { get; set; }
    public decimal NetCashFlow { get; set; }
    public int SalesCount { get; set; }
    public int PurchasesCount { get; set; }
    public List<DailySummaryDto> DailyBreakdown { get; set; } = new();
    public List<TopItemDto> TopProducts { get; set; } = new();
    public List<TopItemDto> TopCustomers { get; set; } = new();
}

public class DailySummaryDto
{
    public DateTime Date { get; set; }
    public decimal Sales { get; set; }
    public decimal Purchases { get; set; }
    public decimal Payments { get; set; }
}

public class TopItemDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public int Count { get; set; }
}
