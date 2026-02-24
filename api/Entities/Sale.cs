namespace Api.Entities;

public enum SaleStatus
{
    Pending,
    Completed,
    Cancelled
}

public class Sale
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public int CustomerId { get; set; }
    public decimal Total { get; set; }
    public SaleStatus Status { get; set; } = SaleStatus.Pending;
    public string? Notes { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    // Navigation
    public Customer? Customer { get; set; }
    public ICollection<SaleItem> Items { get; set; } = new List<SaleItem>();
}
