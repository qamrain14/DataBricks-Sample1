namespace Api.Entities;

public enum PurchaseStatus
{
    Pending,
    Received,
    Cancelled
}

public class Purchase
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public int SupplierId { get; set; }
    public decimal Total { get; set; }
    public PurchaseStatus Status { get; set; } = PurchaseStatus.Pending;
    public string? Notes { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    // Navigation
    public Supplier? Supplier { get; set; }
    public ICollection<PurchaseItem> Items { get; set; } = new List<PurchaseItem>();
}
