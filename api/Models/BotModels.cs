using System.ComponentModel.DataAnnotations;

namespace Api.Models;

public enum BotIntent
{
    SalesSummary,
    TopProducts,
    PurchasesSummary,
    PaymentsSummary,
    UserProfile,
    RemoveUser,
    Help,
    Unknown
}

public class BotQueryRequest
{
    [Required]
    public string Query { get; set; } = string.Empty;
}

public class BotReply
{
    public string Text { get; set; } = string.Empty;
    public BotIntent Intent { get; set; }
    public float Confidence { get; set; }
    public List<BotCard>? Cards { get; set; }
    public List<BotAction>? Actions { get; set; }
}

public class BotCard
{
    public string Title { get; set; } = string.Empty;
    public string? Subtitle { get; set; }
    public Dictionary<string, string>? Data { get; set; }
}

public class BotAction
{
    public string ActionId { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string? TargetId { get; set; }
    public string? ConfirmMessage { get; set; }
}

// Training data model for ML.NET
public class IntentData
{
    public string Text { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
}

public class IntentPrediction
{
    public string PredictedLabel { get; set; } = string.Empty;
    public float[] Score { get; set; } = Array.Empty<float>();
}
