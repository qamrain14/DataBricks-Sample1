using Microsoft.AspNetCore.Identity;
using Api.Entities;

namespace Api.Data;

public static class DbSeeder
{
    public static async Task SeedAsync(IServiceProvider serviceProvider)
    {
        var roleManager = serviceProvider.GetRequiredService<RoleManager<IdentityRole>>();
        var userManager = serviceProvider.GetRequiredService<UserManager<AppUser>>();

        // Create roles
        string[] roles = { "User", "Supervisor" };
        foreach (var role in roles)
        {
            if (!await roleManager.RoleExistsAsync(role))
            {
                await roleManager.CreateAsync(new IdentityRole(role));
            }
        }

        // Create admin supervisor
        var adminEmail = "admin@school.local";
        var adminUser = await userManager.FindByEmailAsync(adminEmail);
        
        if (adminUser == null)
        {
            adminUser = new AppUser
            {
                UserName = adminEmail,
                Email = adminEmail,
                EmailConfirmed = true,
                FullName = "System Administrator",
                RoleDisplay = "Supervisor",
                CreatedAt = DateTime.UtcNow
            };

            var result = await userManager.CreateAsync(adminUser, "Admin@123!");
            if (result.Succeeded)
            {
                await userManager.AddToRoleAsync(adminUser, "Supervisor");
            }
        }

        // Seed sample data for curriculum
        using var scope = serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        if (!context.Classes.Any())
        {
            context.Classes.AddRange(
                new Class { Name = "Grade 1", Description = "First grade" },
                new Class { Name = "Grade 2", Description = "Second grade" },
                new Class { Name = "Grade 3", Description = "Third grade" }
            );
        }

        if (!context.Subjects.Any())
        {
            context.Subjects.AddRange(
                new Subject { Name = "Mathematics", Description = "Math subject" },
                new Subject { Name = "English", Description = "English language" },
                new Subject { Name = "Science", Description = "Science subject" }
            );
        }

        if (!context.Sections.Any())
        {
            context.Sections.AddRange(
                new Section { Name = "Section A", Capacity = 30 },
                new Section { Name = "Section B", Capacity = 30 },
                new Section { Name = "Section C", Capacity = 25 }
            );
        }

        if (!context.Timelines.Any())
        {
            context.Timelines.AddRange(
                new Timeline { Name = "Fall 2026", StartDate = new DateTime(2026, 9, 1), EndDate = new DateTime(2026, 12, 15) },
                new Timeline { Name = "Spring 2027", StartDate = new DateTime(2027, 1, 15), EndDate = new DateTime(2027, 5, 30) }
            );
        }

        if (!context.Products.Any())
        {
            context.Products.AddRange(
                new Product { Sku = "BOOK-001", Name = "Math Textbook", UnitPrice = 29.99m, StockQuantity = 100 },
                new Product { Sku = "BOOK-002", Name = "English Workbook", UnitPrice = 19.99m, StockQuantity = 150 },
                new Product { Sku = "SUP-001", Name = "Notebook Pack", UnitPrice = 9.99m, StockQuantity = 200 }
            );
        }

        if (!context.Customers.Any())
        {
            context.Customers.AddRange(
                new Customer { Name = "John Doe", Email = "john@example.com", Phone = "555-0101" },
                new Customer { Name = "Jane Smith", Email = "jane@example.com", Phone = "555-0102" }
            );
        }

        if (!context.Suppliers.Any())
        {
            context.Suppliers.AddRange(
                new Supplier { Name = "Book Distributors Inc", Email = "books@supplier.com", Phone = "555-1001" },
                new Supplier { Name = "School Supplies Co", Email = "supplies@supplier.com", Phone = "555-1002" }
            );
        }

        await context.SaveChangesAsync();
    }
}
