using Microsoft.ML;
using Microsoft.EntityFrameworkCore;
using Api.Data;
using Api.Models;
using Api.Entities;

namespace Api.Services;

public interface ISupervisorBotService
{
    Task<BotReply> ProcessQueryAsync(string query);
}

public class SupervisorBotService : ISupervisorBotService
{
    private readonly MLContext _mlContext;
    private readonly ITransformer? _model;
    private readonly PredictionEngine<IntentData, IntentPrediction>? _predictionEngine;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<SupervisorBotService> _logger;
    private static readonly string[] IntentLabels = { "SalesSummary", "TopProducts", "PurchasesSummary", "PaymentsSummary", "UserProfile", "RemoveUser", "Help" };

    public SupervisorBotService(IServiceProvider serviceProvider, ILogger<SupervisorBotService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _mlContext = new MLContext(seed: 0);

        try
        {
            var trainingData = GetTrainingData();
            var dataView = _mlContext.Data.LoadFromEnumerable(trainingData);

            var pipeline = _mlContext.Transforms.Conversion.MapValueToKey("Label")
                .Append(_mlContext.Transforms.Text.FeaturizeText("Features", nameof(IntentData.Text)))
                .Append(_mlContext.MulticlassClassification.Trainers.SdcaMaximumEntropy("Label", "Features"))
                .Append(_mlContext.Transforms.Conversion.MapKeyToValue("PredictedLabel"));

            _model = pipeline.Fit(dataView);
            _predictionEngine = _mlContext.Model.CreatePredictionEngine<IntentData, IntentPrediction>(_model);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize ML model");
        }
    }

    private static List<IntentData> GetTrainingData()
    {
        return new List<IntentData>
        {
            // SalesSummary
            new() { Text = "show me sales", Label = "SalesSummary" },
            new() { Text = "what are total sales", Label = "SalesSummary" },
            new() { Text = "sales summary", Label = "SalesSummary" },
            new() { Text = "how much did we sell", Label = "SalesSummary" },
            new() { Text = "sales report", Label = "SalesSummary" },
            new() { Text = "revenue summary", Label = "SalesSummary" },
            new() { Text = "total revenue", Label = "SalesSummary" },
            new() { Text = "sales this month", Label = "SalesSummary" },

            // TopProducts
            new() { Text = "top selling products", Label = "TopProducts" },
            new() { Text = "best sellers", Label = "TopProducts" },
            new() { Text = "most popular items", Label = "TopProducts" },
            new() { Text = "what sells the most", Label = "TopProducts" },
            new() { Text = "top products", Label = "TopProducts" },
            new() { Text = "highest selling items", Label = "TopProducts" },

            // PurchasesSummary
            new() { Text = "show me purchases", Label = "PurchasesSummary" },
            new() { Text = "purchase summary", Label = "PurchasesSummary" },
            new() { Text = "what did we buy", Label = "PurchasesSummary" },
            new() { Text = "total purchases", Label = "PurchasesSummary" },
            new() { Text = "spending report", Label = "PurchasesSummary" },
            new() { Text = "how much did we spend", Label = "PurchasesSummary" },

            // PaymentsSummary
            new() { Text = "show payments", Label = "PaymentsSummary" },
            new() { Text = "payment summary", Label = "PaymentsSummary" },
            new() { Text = "cash flow", Label = "PaymentsSummary" },
            new() { Text = "incoming payments", Label = "PaymentsSummary" },
            new() { Text = "outgoing payments", Label = "PaymentsSummary" },
            new() { Text = "money in and out", Label = "PaymentsSummary" },

            // UserProfile
            new() { Text = "show user profile", Label = "UserProfile" },
            new() { Text = "user information", Label = "UserProfile" },
            new() { Text = "find user", Label = "UserProfile" },
            new() { Text = "who is this user", Label = "UserProfile" },
            new() { Text = "user details", Label = "UserProfile" },
            new() { Text = "lookup user", Label = "UserProfile" },

            // RemoveUser
            new() { Text = "remove user", Label = "RemoveUser" },
            new() { Text = "delete user", Label = "RemoveUser" },
            new() { Text = "deactivate user", Label = "RemoveUser" },
            new() { Text = "ban user", Label = "RemoveUser" },
            new() { Text = "disable account", Label = "RemoveUser" },

            // Help
            new() { Text = "help", Label = "Help" },
            new() { Text = "what can you do", Label = "Help" },
            new() { Text = "commands", Label = "Help" },
            new() { Text = "how to use", Label = "Help" },
            new() { Text = "show options", Label = "Help" },
            new() { Text = "menu", Label = "Help" }
        };
    }

    public async Task<BotReply> ProcessQueryAsync(string query)
    {
        if (_predictionEngine == null)
        {
            return new BotReply
            {
                Text = "Bot service is not available. Please try again later.",
                Intent = BotIntent.Unknown,
                Confidence = 0
            };
        }

        var prediction = _predictionEngine.Predict(new IntentData { Text = query.ToLower() });
        var intent = Enum.TryParse<BotIntent>(prediction.PredictedLabel, out var parsedIntent) 
            ? parsedIntent 
            : BotIntent.Unknown;

        var maxScore = prediction.Score?.Max() ?? 0;

        return intent switch
        {
            BotIntent.SalesSummary => await HandleSalesSummary(intent, maxScore),
            BotIntent.TopProducts => await HandleTopProducts(intent, maxScore),
            BotIntent.PurchasesSummary => await HandlePurchasesSummary(intent, maxScore),
            BotIntent.PaymentsSummary => await HandlePaymentsSummary(intent, maxScore),
            BotIntent.UserProfile => HandleUserProfile(intent, maxScore, query),
            BotIntent.RemoveUser => HandleRemoveUser(intent, maxScore, query),
            BotIntent.Help => HandleHelp(intent, maxScore),
            _ => new BotReply
            {
                Text = "I'm not sure what you're asking. Type 'help' to see available commands.",
                Intent = BotIntent.Unknown,
                Confidence = maxScore
            }
        };
    }

    private async Task<BotReply> HandleSalesSummary(BotIntent intent, float confidence)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var thirtyDaysAgo = DateTime.UtcNow.AddDays(-30);
        var sales = await context.Sales
            .Where(s => s.Date >= thirtyDaysAgo)
            .ToListAsync();

        var total = sales.Sum(s => s.Total);
        var count = sales.Count;

        return new BotReply
        {
            Text = $"Sales Summary (Last 30 Days):\n• Total Sales: ${total:N2}\n• Number of Transactions: {count}\n• Average Sale: ${(count > 0 ? total / count : 0):N2}",
            Intent = intent,
            Confidence = confidence,
            Cards = new List<BotCard>
            {
                new() { Title = "Total Sales", Data = new Dictionary<string, string> { { "Amount", $"${total:N2}" }, { "Count", count.ToString() } } }
            }
        };
    }

    private async Task<BotReply> HandleTopProducts(BotIntent intent, float confidence)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var topProducts = await context.SaleItems
            .Include(si => si.Product)
            .GroupBy(si => new { si.ProductId, si.Product!.Name })
            .Select(g => new { g.Key.Name, Total = g.Sum(si => si.Quantity * si.UnitPrice), Count = g.Sum(si => si.Quantity) })
            .OrderByDescending(x => x.Total)
            .Take(5)
            .ToListAsync();

        var productList = string.Join("\n", topProducts.Select((p, i) => $"{i + 1}. {p.Name}: ${p.Total:N2} ({p.Count} units)"));
        
        return new BotReply
        {
            Text = $"Top Selling Products:\n{(string.IsNullOrEmpty(productList) ? "No sales data available." : productList)}",
            Intent = intent,
            Confidence = confidence
        };
    }

    private async Task<BotReply> HandlePurchasesSummary(BotIntent intent, float confidence)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var thirtyDaysAgo = DateTime.UtcNow.AddDays(-30);
        var purchases = await context.Purchases
            .Where(p => p.Date >= thirtyDaysAgo)
            .ToListAsync();

        var total = purchases.Sum(p => p.Total);
        var count = purchases.Count;

        return new BotReply
        {
            Text = $"Purchases Summary (Last 30 Days):\n• Total Purchases: ${total:N2}\n• Number of Orders: {count}\n• Average Purchase: ${(count > 0 ? total / count : 0):N2}",
            Intent = intent,
            Confidence = confidence
        };
    }

    private async Task<BotReply> HandlePaymentsSummary(BotIntent intent, float confidence)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var thirtyDaysAgo = DateTime.UtcNow.AddDays(-30);
        var payments = await context.Payments
            .Where(p => p.Date >= thirtyDaysAgo)
            .ToListAsync();

        var incoming = payments.Where(p => p.Direction == PaymentDirection.Incoming).Sum(p => p.Amount);
        var outgoing = payments.Where(p => p.Direction == PaymentDirection.Outgoing).Sum(p => p.Amount);

        return new BotReply
        {
            Text = $"Payments Summary (Last 30 Days):\n• Incoming: ${incoming:N2}\n• Outgoing: ${outgoing:N2}\n• Net Cash Flow: ${incoming - outgoing:N2}",
            Intent = intent,
            Confidence = confidence
        };
    }

    private BotReply HandleUserProfile(BotIntent intent, float confidence, string query)
    {
        return new BotReply
        {
            Text = "To look up a user profile, please provide the user's email address. Example: 'find user john@school.local'",
            Intent = intent,
            Confidence = confidence
        };
    }

    private BotReply HandleRemoveUser(BotIntent intent, float confidence, string query)
    {
        return new BotReply
        {
            Text = "To remove a user, please specify the user's email. This action requires confirmation.",
            Intent = intent,
            Confidence = confidence,
            Actions = new List<BotAction>
            {
                new() { ActionId = "confirm_remove", Label = "Confirm Removal", ConfirmMessage = "Are you sure you want to remove this user?" }
            }
        };
    }

    private BotReply HandleHelp(BotIntent intent, float confidence)
    {
        return new BotReply
        {
            Text = "Available Commands:\n• 'sales summary' - View sales totals\n• 'top products' - See best sellers\n• 'purchases summary' - View purchase totals\n• 'payments summary' - View cash flow\n• 'find user [email]' - Look up user profile\n• 'remove user [email]' - Deactivate a user account",
            Intent = intent,
            Confidence = confidence
        };
    }
}
