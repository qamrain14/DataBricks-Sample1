import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { PagedResult } from '../models';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly BASE_URL = 'http://localhost:5154/api';

  constructor(private http: HttpClient) {}

  get<T>(endpoint: string, params?: Record<string, any>): Observable<T> {
    let httpParams = new HttpParams();
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          httpParams = httpParams.set(key, String(value));
        }
      });
    }
    return this.http.get<T>(`${this.BASE_URL}/${endpoint}`, { params: httpParams });
  }

  getById<T>(endpoint: string, id: number | string): Observable<T> {
    return this.http.get<T>(`${this.BASE_URL}/${endpoint}/${id}`);
  }

  getPaged<T>(endpoint: string, page = 1, pageSize = 10, filters?: Record<string, any>): Observable<PagedResult<T>> {
    const params = { page, pageSize, ...filters };
    return this.get<PagedResult<T>>(endpoint, params);
  }

  post<T>(endpoint: string, body: any): Observable<T> {
    return this.http.post<T>(`${this.BASE_URL}/${endpoint}`, body);
  }

  put<T>(endpoint: string, id: number | string, body: any): Observable<T> {
    return this.http.put<T>(`${this.BASE_URL}/${endpoint}/${id}`, body);
  }

  patch<T>(endpoint: string, id: number | string, body: any): Observable<T> {
    return this.http.patch<T>(`${this.BASE_URL}/${endpoint}/${id}`, body);
  }

  delete(endpoint: string, id: number | string): Observable<void> {
    return this.http.delete<void>(`${this.BASE_URL}/${endpoint}/${id}`);
  }
}
