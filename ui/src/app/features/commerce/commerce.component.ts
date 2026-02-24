import { Component, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { CommerceService } from '../../core/services/commerce.service';
import { ProductDto, CustomerDto, SupplierDto, SaleDto } from '../../core/models';

@Component({
  selector: 'app-commerce',
  standalone: true,
  imports: [CommonModule, RouterLink],
  template: `
    <div class="page">
      <header><a routerLink="/" class="back">← Dashboard</a><h1>Commerce Management</h1></header>
      <div class="tabs">
        <button [class.active]="tab() === 'products'" (click)="tab.set('products')">Products</button>
        <button [class.active]="tab() === 'customers'" (click)="tab.set('customers')">Customers</button>
        <button [class.active]="tab() === 'suppliers'" (click)="tab.set('suppliers')">Suppliers</button>
        <button [class.active]="tab() === 'sales'" (click)="tab.set('sales')">Sales</button>
      </div>
      @switch (tab()) {
        @case ('products') {
          <table><thead><tr><th>SKU</th><th>Name</th><th>Price</th><th>Stock</th><th>Status</th></tr></thead><tbody>
            @for (p of products(); track p.id) { <tr><td>{{p.sku}}</td><td>{{p.name}}</td><td>{{p.unitPrice|currency}}</td><td>{{p.stockQuantity}}</td><td><span [class]="p.isActive?'on':'off'">{{p.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="5" class="empty">No products</td></tr> }
          </tbody></table>
        }
        @case ('customers') {
          <table><thead><tr><th>Name</th><th>Email</th><th>Phone</th><th>Status</th></tr></thead><tbody>
            @for (c of customers(); track c.id) { <tr><td>{{c.name}}</td><td>{{c.email||'-'}}</td><td>{{c.phone||'-'}}</td><td><span [class]="c.isActive?'on':'off'">{{c.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="4" class="empty">No customers</td></tr> }
          </tbody></table>
        }
        @case ('suppliers') {
          <table><thead><tr><th>Name</th><th>Email</th><th>Phone</th><th>Status</th></tr></thead><tbody>
            @for (s of suppliers(); track s.id) { <tr><td>{{s.name}}</td><td>{{s.email||'-'}}</td><td>{{s.phone||'-'}}</td><td><span [class]="s.isActive?'on':'off'">{{s.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="4" class="empty">No suppliers</td></tr> }
          </tbody></table>
        }
        @case ('sales') {
          <table><thead><tr><th>Date</th><th>Customer</th><th>Total</th><th>Status</th></tr></thead><tbody>
            @for (s of sales(); track s.id) { <tr><td>{{s.date|date:'mediumDate'}}</td><td>{{s.customerName||'-'}}</td><td>{{s.total|currency}}</td><td>{{s.status}}</td></tr> }
            @empty { <tr><td colspan="4" class="empty">No sales</td></tr> }
          </tbody></table>
        }
      }
    </div>
  `,
  styles: [`
    .page{padding:2rem;max-width:1200px;margin:0 auto}header{margin-bottom:1.5rem}.back{color:#667eea;text-decoration:none;display:inline-block;margin-bottom:.5rem}h1{margin:0}
    .tabs{display:flex;gap:.5rem;margin-bottom:1.5rem;border-bottom:2px solid #eee;padding-bottom:.5rem}
    .tabs button{padding:.75rem 1.5rem;border:none;background:none;cursor:pointer;font-size:1rem;color:#666;border-radius:4px 4px 0 0}
    .tabs button.active{background:#667eea;color:#fff}
    table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);overflow:hidden}
    th,td{padding:1rem;text-align:left;border-bottom:1px solid #eee}th{background:#f8f9fa;font-weight:600}
    .empty{text-align:center;color:#999}.on{background:#d4edda;color:#155724;padding:.25rem .75rem;border-radius:12px;font-size:.85rem}
    .off{background:#f8d7da;color:#721c24;padding:.25rem .75rem;border-radius:12px;font-size:.85rem}
  `]
})
export class CommerceComponent implements OnInit {
  tab = signal<'products'|'customers'|'suppliers'|'sales'>('products');
  products = signal<ProductDto[]>([]); customers = signal<CustomerDto[]>([]); suppliers = signal<SupplierDto[]>([]); sales = signal<SaleDto[]>([]);
  constructor(private svc: CommerceService) {}
  ngOnInit() {
    this.svc.getProducts(1,100).subscribe(r => this.products.set(r.items));
    this.svc.getCustomers(1,100).subscribe(r => this.customers.set(r.items));
    this.svc.getSuppliers(1,100).subscribe(r => this.suppliers.set(r.items));
    this.svc.getSales(1,100).subscribe(r => this.sales.set(r.items));
  }
}
