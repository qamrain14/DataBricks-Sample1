using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Api.Entities;

namespace Api.Data;

public class AppDbContext : IdentityDbContext<AppUser, IdentityRole, string>
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<UserProfile> UserProfiles => Set<UserProfile>();
    public DbSet<Class> Classes => Set<Class>();
    public DbSet<Subject> Subjects => Set<Subject>();
    public DbSet<Section> Sections => Set<Section>();
    public DbSet<Timeline> Timelines => Set<Timeline>();
    public DbSet<ClassOffering> ClassOfferings => Set<ClassOffering>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Supplier> Suppliers => Set<Supplier>();
    public DbSet<Sale> Sales => Set<Sale>();
    public DbSet<SaleItem> SaleItems => Set<SaleItem>();
    public DbSet<Purchase> Purchases => Set<Purchase>();
    public DbSet<PurchaseItem> PurchaseItems => Set<PurchaseItem>();
    public DbSet<Payment> Payments => Set<Payment>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);

        // AppUser
        builder.Entity<AppUser>(e =>
        {
            e.HasOne(u => u.Profile)
             .WithOne(p => p.AppUser)
             .HasForeignKey<UserProfile>(p => p.AppUserId)
             .OnDelete(DeleteBehavior.Cascade);
        });

        // UserProfile
        builder.Entity<UserProfile>(e =>
        {
            e.HasIndex(p => p.AppUserId).IsUnique();
        });

        // Product
        builder.Entity<Product>(e =>
        {
            e.HasIndex(p => p.Sku).IsUnique();
            e.Property(p => p.UnitPrice).HasPrecision(18, 2);
        });

        // Customer
        builder.Entity<Customer>(e =>
        {
            e.HasIndex(c => c.Email);
        });

        // Supplier
        builder.Entity<Supplier>(e =>
        {
            e.HasIndex(s => s.Email);
        });

        // ClassOffering
        builder.Entity<ClassOffering>(e =>
        {
            e.HasOne(co => co.Class)
             .WithMany(c => c.ClassOfferings)
             .HasForeignKey(co => co.ClassId)
             .OnDelete(DeleteBehavior.Restrict);

            e.HasOne(co => co.Subject)
             .WithMany(s => s.ClassOfferings)
             .HasForeignKey(co => co.SubjectId)
             .OnDelete(DeleteBehavior.Restrict);

            e.HasOne(co => co.Section)
             .WithMany(s => s.ClassOfferings)
             .HasForeignKey(co => co.SectionId)
             .OnDelete(DeleteBehavior.Restrict);

            e.HasOne(co => co.Timeline)
             .WithMany(t => t.ClassOfferings)
             .HasForeignKey(co => co.TimelineId)
             .OnDelete(DeleteBehavior.Restrict);

            e.HasOne(co => co.Instructor)
             .WithMany()
             .HasForeignKey(co => co.InstructorId)
             .OnDelete(DeleteBehavior.SetNull);

            e.HasIndex(co => new { co.ClassId, co.SubjectId, co.SectionId, co.TimelineId });
        });

        // Sale
        builder.Entity<Sale>(e =>
        {
            e.Property(s => s.Total).HasPrecision(18, 2);
            e.HasIndex(s => s.Date);
            e.HasOne(s => s.Customer)
             .WithMany(c => c.Sales)
             .HasForeignKey(s => s.CustomerId)
             .OnDelete(DeleteBehavior.Restrict);
        });

        // SaleItem
        builder.Entity<SaleItem>(e =>
        {
            e.Property(si => si.UnitPrice).HasPrecision(18, 2);
            e.HasOne(si => si.Sale)
             .WithMany(s => s.Items)
             .HasForeignKey(si => si.SaleId)
             .OnDelete(DeleteBehavior.Cascade);

            e.HasOne(si => si.Product)
             .WithMany(p => p.SaleItems)
             .HasForeignKey(si => si.ProductId)
             .OnDelete(DeleteBehavior.Restrict);
        });

        // Purchase
        builder.Entity<Purchase>(e =>
        {
            e.Property(p => p.Total).HasPrecision(18, 2);
            e.HasIndex(p => p.Date);
            e.HasOne(p => p.Supplier)
             .WithMany(s => s.Purchases)
             .HasForeignKey(p => p.SupplierId)
             .OnDelete(DeleteBehavior.Restrict);
        });

        // PurchaseItem
        builder.Entity<PurchaseItem>(e =>
        {
            e.Property(pi => pi.UnitPrice).HasPrecision(18, 2);
            e.HasOne(pi => pi.Purchase)
             .WithMany(p => p.Items)
             .HasForeignKey(pi => pi.PurchaseId)
             .OnDelete(DeleteBehavior.Cascade);

            e.HasOne(pi => pi.Product)
             .WithMany(p => p.PurchaseItems)
             .HasForeignKey(pi => pi.ProductId)
             .OnDelete(DeleteBehavior.Restrict);
        });

        // Payment
        builder.Entity<Payment>(e =>
        {
            e.Property(p => p.Amount).HasPrecision(18, 2);
            e.HasIndex(p => p.Date);
        });
    }
}
