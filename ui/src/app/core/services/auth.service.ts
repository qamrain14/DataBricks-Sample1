import { Injectable, signal, computed } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { Observable, tap, catchError, throwError } from 'rxjs';
import { LoginRequest, RegisterRequest, AuthResponse, UserDto, UpdateProfileRequest } from '../models';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly API_URL = 'http://localhost:5154/api/auth';
  private readonly TOKEN_KEY = 'school_suite_token';
  private readonly USER_KEY = 'school_suite_user';

  private currentUserSignal = signal<UserDto | null>(null);
  readonly currentUser = this.currentUserSignal.asReadonly();
  readonly isAuthenticated = computed(() => !!this.currentUserSignal());
  readonly userRole = computed(() => this.currentUserSignal()?.role ?? '');

  constructor(private http: HttpClient, private router: Router) {
    this.loadStoredUser();
  }

  private loadStoredUser(): void {
    const token = localStorage.getItem(this.TOKEN_KEY);
    const userJson = localStorage.getItem(this.USER_KEY);
    if (token && userJson) {
      try {
        this.currentUserSignal.set(JSON.parse(userJson));
      } catch {
        this.clearStorage();
      }
    }
  }

  login(request: LoginRequest): Observable<AuthResponse> {
    return this.http.post<AuthResponse>(`${this.API_URL}/login`, request).pipe(
      tap(response => this.handleAuthSuccess(response)),
      catchError(err => throwError(() => err))
    );
  }

  register(request: RegisterRequest): Observable<AuthResponse> {
    return this.http.post<AuthResponse>(`${this.API_URL}/register`, request).pipe(
      tap(response => this.handleAuthSuccess(response)),
      catchError(err => throwError(() => err))
    );
  }

  logout(): void {
    this.clearStorage();
    this.currentUserSignal.set(null);
    this.router.navigate(['/login']);
  }

  refreshToken(): Observable<AuthResponse> {
    const token = this.getToken();
    const refreshToken = localStorage.getItem('school_suite_refresh');
    return this.http.post<AuthResponse>(`${this.API_URL}/refresh`, { token, refreshToken }).pipe(
      tap(response => this.handleAuthSuccess(response)),
      catchError(err => {
        this.logout();
        return throwError(() => err);
      })
    );
  }

  getProfile(): Observable<UserDto> {
    return this.http.get<UserDto>(`${this.API_URL}/profile`);
  }

  updateProfile(request: UpdateProfileRequest): Observable<UserDto> {
    return this.http.put<UserDto>(`${this.API_URL}/profile`, request).pipe(
      tap(user => {
        this.currentUserSignal.set(user);
        localStorage.setItem(this.USER_KEY, JSON.stringify(user));
      })
    );
  }

  getToken(): string | null {
    return localStorage.getItem(this.TOKEN_KEY);
  }

  private handleAuthSuccess(response: AuthResponse): void {
    localStorage.setItem(this.TOKEN_KEY, response.token);
    localStorage.setItem('school_suite_refresh', response.refreshToken);
    localStorage.setItem(this.USER_KEY, JSON.stringify(response.user));
    this.currentUserSignal.set(response.user);
  }

  private clearStorage(): void {
    localStorage.removeItem(this.TOKEN_KEY);
    localStorage.removeItem('school_suite_refresh');
    localStorage.removeItem(this.USER_KEY);
  }
}
