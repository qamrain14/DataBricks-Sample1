namespace Api.Entities;

public class PurchaseItem
{
    public int Id { get; set; }
    public int PurchaseId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal => Quantity * UnitPrice;

    // Navigation
    public Purchase? Purchase { get; set; }
    public Product? Product { get; set; }
}
