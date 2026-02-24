import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiService } from './api.service';
import { PagedResult, ProductDto, CustomerDto, SupplierDto, SaleDto, PurchaseDto, PaymentDto } from '../models';

@Injectable({ providedIn: 'root' })
export class CommerceService {
  constructor(private api: ApiService) {}

  // Products
  getProducts(page = 1, pageSize = 10): Observable<PagedResult<ProductDto>> {
    return this.api.getPaged<ProductDto>('products', page, pageSize);
  }

  getProduct(id: number): Observable<ProductDto> {
    return this.api.getById<ProductDto>('products', id);
  }

  createProduct(data: Partial<ProductDto>): Observable<ProductDto> {
    return this.api.post<ProductDto>('products', data);
  }

  updateProduct(id: number, data: Partial<ProductDto>): Observable<ProductDto> {
    return this.api.put<ProductDto>('products', id, data);
  }

  deleteProduct(id: number): Observable<void> {
    return this.api.delete('products', id);
  }

  // Customers
  getCustomers(page = 1, pageSize = 10): Observable<PagedResult<CustomerDto>> {
    return this.api.getPaged<CustomerDto>('customers', page, pageSize);
  }

  getCustomer(id: number): Observable<CustomerDto> {
    return this.api.getById<CustomerDto>('customers', id);
  }

  createCustomer(data: Partial<CustomerDto>): Observable<CustomerDto> {
    return this.api.post<CustomerDto>('customers', data);
  }

  updateCustomer(id: number, data: Partial<CustomerDto>): Observable<CustomerDto> {
    return this.api.put<CustomerDto>('customers', id, data);
  }

  deleteCustomer(id: number): Observable<void> {
    return this.api.delete('customers', id);
  }

  // Suppliers
  getSuppliers(page = 1, pageSize = 10): Observable<PagedResult<SupplierDto>> {
    return this.api.getPaged<SupplierDto>('suppliers', page, pageSize);
  }

  getSupplier(id: number): Observable<SupplierDto> {
    return this.api.getById<SupplierDto>('suppliers', id);
  }

  createSupplier(data: Partial<SupplierDto>): Observable<SupplierDto> {
    return this.api.post<SupplierDto>('suppliers', data);
  }

  updateSupplier(id: number, data: Partial<SupplierDto>): Observable<SupplierDto> {
    return this.api.put<SupplierDto>('suppliers', id, data);
  }

  deleteSupplier(id: number): Observable<void> {
    return this.api.delete('suppliers', id);
  }

  // Sales
  getSales(page = 1, pageSize = 10): Observable<PagedResult<SaleDto>> {
    return this.api.getPaged<SaleDto>('sales', page, pageSize);
  }

  getSale(id: number): Observable<SaleDto> {
    return this.api.getById<SaleDto>('sales', id);
  }

  createSale(data: Partial<SaleDto>): Observable<SaleDto> {
    return this.api.post<SaleDto>('sales', data);
  }

  // Purchases
  getPurchases(page = 1, pageSize = 10): Observable<PagedResult<PurchaseDto>> {
    return this.api.getPaged<PurchaseDto>('purchases', page, pageSize);
  }

  getPurchase(id: number): Observable<PurchaseDto> {
    return this.api.getById<PurchaseDto>('purchases', id);
  }

  createPurchase(data: Partial<PurchaseDto>): Observable<PurchaseDto> {
    return this.api.post<PurchaseDto>('purchases', data);
  }

  // Payments
  getPayments(page = 1, pageSize = 10): Observable<PagedResult<PaymentDto>> {
    return this.api.getPaged<PaymentDto>('payments', page, pageSize);
  }

  getPayment(id: number): Observable<PaymentDto> {
    return this.api.getById<PaymentDto>('payments', id);
  }

  createPayment(data: Partial<PaymentDto>): Observable<PaymentDto> {
    return this.api.post<PaymentDto>('payments', data);
  }
}
