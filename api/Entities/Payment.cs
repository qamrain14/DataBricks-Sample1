namespace Api.Entities;

public enum PaymentDirection
{
    Incoming,
    Outgoing
}

public class Payment
{
    public int Id { get; set; }
    public DateTime Date { get; set; }
    public decimal Amount { get; set; }
    public PaymentDirection Direction { get; set; }
    public int? ReferenceId { get; set; }
    public string? ReferenceType { get; set; } // "Sale" or "Purchase"
    public string? Notes { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
